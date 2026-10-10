// SPDX-FileCopyrightText: 2026 Benjamin Grewell
// SPDX-License-Identifier: Apache-2.0

package configapi

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/omec-project/openapi/v2"
	"github.com/omec-project/openapi/v2/models"
	"github.com/omec-project/webconsole/configmodels"
	"github.com/omec-project/webconsole/dbadapter"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	testSyncMcc = "208"
	testSyncMnc = "93"
	opsKey      = "ops"
)

// credentialsMockDBClient stands in for AuthDBClient: it answers a $in read of
// the authentication subscriptions with the IMSIs it was given credentials for.
type credentialsMockDBClient struct {
	dbadapter.DBInterface
	imsis []string
	reads []bson.M
	err   error
}

func (db *credentialsMockDBClient) RestfulAPIGetMany(coll string, filter bson.M) ([]map[string]any, error) {
	db.reads = append(db.reads, filter)
	if db.err != nil {
		return nil, db.err
	}
	if coll != authSubsDataColl {
		return nil, nil
	}
	var results []map[string]any
	for _, imsi := range db.imsis {
		if slices.Contains(requestedUeIds(filter), "imsi-"+imsi) {
			results = append(results, map[string]any{ueIdKey: "imsi-" + imsi})
		}
	}
	return results, nil
}

// subscriberSyncMockDBClient stands in for CommonDBClient: it answers a $in
// read of a collection from stored and records the one-at-a-time writes a
// client without bulk writes receives.
type subscriberSyncMockDBClient struct {
	dbadapter.DBInterface
	stored  map[string][]map[string]any
	reads   []map[string]any
	putData []map[string]any
	readErr error
}

func (db *subscriberSyncMockDBClient) RestfulAPIGetMany(coll string, filter bson.M) ([]map[string]any, error) {
	db.reads = append(db.reads, map[string]any{collKey: coll, filterKey: filter})
	if db.readErr != nil {
		return nil, db.readErr
	}
	var results []map[string]any
	for _, doc := range db.stored[coll] {
		if ueId, ok := doc[ueIdKey].(string); ok && slices.Contains(requestedUeIds(filter), ueId) {
			results = append(results, doc)
		}
	}
	return results, nil
}

func (db *subscriberSyncMockDBClient) RestfulAPIPutOne(coll string, filter bson.M, data map[string]any) (bool, error) {
	db.putData = append(db.putData, map[string]any{collKey: coll, filterKey: filter, dataKey: data})
	return true, nil
}

// bulkSubscriberSyncMockDBClient is subscriberSyncMockDBClient with bulk
// writes, recorded per call.
type bulkSubscriberSyncMockDBClient struct {
	subscriberSyncMockDBClient
	bulks []map[string]any
}

func (db *bulkSubscriberSyncMockDBClient) BulkUpsert(_ context.Context, coll string, ops []dbadapter.UpsertOperation) error {
	db.bulks = append(db.bulks, map[string]any{collKey: coll, opsKey: ops})
	return nil
}

func requestedUeIds(filter bson.M) []string {
	in, ok := filter[ueIdKey].(bson.M)
	if !ok {
		return nil
	}
	ueIds, _ := in["$in"].([]string)
	return ueIds
}

func testSnssai() *models.Snssai {
	return &models.Snssai{Sst: 1, Sd: openapi.PtrString("010203")}
}

func testDnnMap() map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos {
	return map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos{
		dnnInternet: {
			{
				DnnMbrUplink:   2000000,
				DnnMbrDownlink: 5000000,
				TrafficClass:   &configmodels.TrafficClassInfo{Qci: 9},
			},
		},
	}
}

func testAggregatedQoS() configmodels.DeviceGroupsIpDomainExpandedUeDnnQos {
	return configmodels.DeviceGroupsIpDomainExpandedUeDnnQos{DnnMbrUplink: 2000000, DnnMbrDownlink: 5000000}
}

func useSyncClients(t *testing.T, common dbadapter.DBInterface, auth dbadapter.DBInterface) {
	t.Helper()
	originalCommon, originalAuth := dbadapter.CommonDBClient, dbadapter.AuthDBClient
	t.Cleanup(func() { dbadapter.CommonDBClient, dbadapter.AuthDBClient = originalCommon, originalAuth })
	dbadapter.CommonDBClient, dbadapter.AuthDBClient = common, auth
}

func testImsis(n int) []string {
	imsis := make([]string, n)
	for i := range imsis {
		imsis[i] = fmt.Sprintf("%s%s0%09d", testSyncMcc, testSyncMnc, i+1)
	}
	return imsis
}

// storedAfter returns what the collections hold after the writes in bulks,
// decoded the way a read returns them.
func storedAfter(t *testing.T, bulks []map[string]any) map[string][]map[string]any {
	t.Helper()
	stored := map[string][]map[string]any{}
	for _, bulk := range bulks {
		coll, _ := bulk[collKey].(string)
		ops, _ := bulk[opsKey].([]dbadapter.UpsertOperation)
		for _, op := range ops {
			doc, err := bsonRoundTrip(op.Data)
			if err != nil {
				t.Fatalf("failed to decode a written document: %v", err)
			}
			stored[coll] = append(stored[coll], doc)
		}
	}
	return stored
}

func opsOn(t *testing.T, bulks []map[string]any, coll string) []dbadapter.UpsertOperation {
	t.Helper()
	var ops []dbadapter.UpsertOperation
	for _, bulk := range bulks {
		if bulk[collKey] == coll {
			ops = append(ops, bulk[opsKey].([]dbadapter.UpsertOperation)...)
		}
	}
	return ops
}

func TestSyncProvisionedSubscribersWritesEveryDocumentOfANewSubscriber(t *testing.T) {
	imsis := testImsis(3)
	common := &bulkSubscriberSyncMockDBClient{}
	useSyncClients(t, common, &credentialsMockDBClient{imsis: imsis[:2]})

	report, err := syncProvisionedSubscribers(imsis, []string{"1001"}, testSnssai(), testDnnMap(), testSyncMcc, testSyncMnc, testAggregatedQoS())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := subscriberSyncReport{imsis: 3, provisioned: 2, compared: 10, written: 10, elapsed: report.elapsed}
	if report != want {
		t.Fatalf("expected report %+v, got %+v", want, report)
	}
	if len(common.bulks) != len(subscriberSyncCollections) {
		t.Fatalf("expected one bulk write per collection, got %d", len(common.bulks))
	}
	for i, coll := range subscriberSyncCollections {
		if common.bulks[i][collKey] != coll {
			t.Errorf("expected bulk write %d on %s, got %s", i, coll, common.bulks[i][collKey])
		}
		ops := opsOn(t, common.bulks, coll)
		if len(ops) != 2 {
			t.Errorf("expected 2 documents written to %s, got %d", coll, len(ops))
		}
	}
	if len(common.putData) != 0 {
		t.Errorf("expected no one-at-a-time writes, got %d", len(common.putData))
	}

	amData := opsOn(t, common.bulks, amDataColl)
	wantAmFilter := bson.M{
		ueIdKey: "imsi-" + imsis[0],
		"$or":   []bson.M{{servingPlmnIdKey: testSyncMcc + testSyncMnc}, {servingPlmnIdKey: bson.M{"$exists": false}}},
	}
	if !reflect.DeepEqual(amData[0].Filter, wantAmFilter) {
		t.Errorf("expected amData filter %v, got %v", wantAmFilter, amData[0].Filter)
	}
	if gpsis, ok := amData[0].Data["gpsis"].([]any); !ok || len(gpsis) != 1 || gpsis[0] != "1001" {
		t.Errorf("expected the first IMSI's AM data to carry its MSISDN, got %v", amData[0].Data["gpsis"])
	}
	if _, ok := amData[1].Data["gpsis"]; ok {
		t.Errorf("expected the second IMSI, which has no MSISDN, to carry no gpsis, got %v", amData[1].Data["gpsis"])
	}
	smData := opsOn(t, common.bulks, smDataColl)
	wantSmFilter := bson.M{ueIdKey: "imsi-" + imsis[1], servingPlmnIdKey: testSyncMcc + testSyncMnc}
	if !reflect.DeepEqual(smData[1].Filter, wantSmFilter) {
		t.Errorf("expected smData filter %v, got %v", wantSmFilter, smData[1].Filter)
	}
	for _, key := range []string{"singlenssai", "dnnconfigurations"} {
		if _, ok := smData[1].Data[key]; !ok {
			t.Errorf("expected %s in the smData document", key)
		}
	}
	for _, coll := range []string{amPolicyDataColl, smPolicyDataColl} {
		if filter := opsOn(t, common.bulks, coll)[0].Filter; !reflect.DeepEqual(filter, bson.M{ueIdKey: "imsi-" + imsis[0]}) {
			t.Errorf("expected %s keyed by ueId alone, got %v", coll, filter)
		}
	}
}

// A second sync with nothing changed is what SimApp sends on every restart; it
// must cost reads only. Fields the sync does not own stay and do not count.
func TestSyncProvisionedSubscribersLeavesStoredDocumentsAlone(t *testing.T) {
	imsis := testImsis(2)
	first := &bulkSubscriberSyncMockDBClient{}
	useSyncClients(t, first, &credentialsMockDBClient{imsis: imsis})
	if _, err := syncProvisionedSubscribers(imsis, nil, testSnssai(), testDnnMap(), testSyncMcc, testSyncMnc, testAggregatedQoS()); err != nil {
		t.Fatalf("unexpected error on the first sync: %v", err)
	}
	stored := storedAfter(t, first.bulks)
	for _, docs := range stored {
		for _, doc := range docs {
			doc["_lastSeen"] = int64(1700000000)
		}
	}

	second := &bulkSubscriberSyncMockDBClient{subscriberSyncMockDBClient: subscriberSyncMockDBClient{stored: stored}}
	useSyncClients(t, second, &credentialsMockDBClient{imsis: imsis})
	report, err := syncProvisionedSubscribers(imsis, nil, testSnssai(), testDnnMap(), testSyncMcc, testSyncMnc, testAggregatedQoS())
	if err != nil {
		t.Fatalf("unexpected error on the second sync: %v", err)
	}
	if report.compared != 10 || report.written != 0 {
		t.Fatalf("expected 10 documents compared and none written, got %+v", report)
	}
	if len(second.bulks) != 0 || len(second.putData) != 0 {
		t.Fatalf("expected no writes, got %d bulk and %d single writes", len(second.bulks), len(second.putData))
	}
	if len(second.reads) != len(subscriberSyncCollections) {
		t.Fatalf("expected one read per collection, got %d", len(second.reads))
	}
}

func TestSyncProvisionedSubscribersRewritesOnlyWhatDiffers(t *testing.T) {
	imsis := testImsis(2)
	first := &bulkSubscriberSyncMockDBClient{}
	useSyncClients(t, first, &credentialsMockDBClient{imsis: imsis})
	if _, err := syncProvisionedSubscribers(imsis, nil, testSnssai(), testDnnMap(), testSyncMcc, testSyncMnc, testAggregatedQoS()); err != nil {
		t.Fatalf("unexpected error on the first sync: %v", err)
	}
	stored := storedAfter(t, first.bulks)
	// The operator raised the group's rate: the second subscriber's SM data is stale.
	changed := stored[smDataColl][1]
	changed["dnnconfigurations"].(map[string]any)[dnnInternet].(map[string]any)["sessionAmbr"] = map[string]any{uplinkKey: "1 Mbps", downlinkKey: "1 Mbps"}

	second := &bulkSubscriberSyncMockDBClient{subscriberSyncMockDBClient: subscriberSyncMockDBClient{stored: stored}}
	useSyncClients(t, second, &credentialsMockDBClient{imsis: imsis})
	report, err := syncProvisionedSubscribers(imsis, nil, testSnssai(), testDnnMap(), testSyncMcc, testSyncMnc, testAggregatedQoS())
	if err != nil {
		t.Fatalf("unexpected error on the second sync: %v", err)
	}
	if report.written != 1 {
		t.Fatalf("expected exactly one document written, got %+v", report)
	}
	ops := opsOn(t, second.bulks, smDataColl)
	if len(second.bulks) != 1 || len(ops) != 1 || ops[0].Filter[ueIdKey] != "imsi-"+imsis[1] {
		t.Fatalf("expected the stale SM data of %s to be the one write, got %v", imsis[1], second.bulks)
	}
}

// A subscriber POST creates the AM data with the ueId alone; the sync must
// complete that document, as the $or filter has always selected it, rather
// than leave it and add a second one.
func TestSyncProvisionedSubscribersCompletesTheAmDataASubscriberPostLeft(t *testing.T) {
	imsis := testImsis(1)
	stored := map[string][]map[string]any{amDataColl: {{ueIdKey: "imsi-" + imsis[0]}}}
	common := &bulkSubscriberSyncMockDBClient{subscriberSyncMockDBClient: subscriberSyncMockDBClient{stored: stored}}
	useSyncClients(t, common, &credentialsMockDBClient{imsis: imsis})

	report, err := syncProvisionedSubscribers(imsis, nil, testSnssai(), testDnnMap(), testSyncMcc, testSyncMnc, testAggregatedQoS())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.written != 5 {
		t.Fatalf("expected all five documents written, got %+v", report)
	}
	ops := opsOn(t, common.bulks, amDataColl)
	if len(ops) != 1 || ops[0].Data[servingPlmnIdKey] != testSyncMcc+testSyncMnc {
		t.Fatalf("expected the AM data to be completed with the serving PLMN, got %v", ops)
	}
}

func TestSyncProvisionedSubscribersReadsAndWritesInBatches(t *testing.T) {
	imsis := testImsis(2*subscriberSyncBatchSize + subscriberSyncBatchSize/2)
	common := &bulkSubscriberSyncMockDBClient{}
	auth := &credentialsMockDBClient{imsis: imsis}
	useSyncClients(t, common, auth)

	report, err := syncProvisionedSubscribers(imsis, nil, testSnssai(), testDnnMap(), testSyncMcc, testSyncMnc, testAggregatedQoS())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.provisioned != len(imsis) || report.written != 5*len(imsis) {
		t.Fatalf("expected every document of every IMSI written, got %+v", report)
	}
	wantBatches := []int{subscriberSyncBatchSize, subscriberSyncBatchSize, subscriberSyncBatchSize / 2}
	if len(auth.reads) != len(wantBatches) {
		t.Fatalf("expected %d credential reads, got %d", len(wantBatches), len(auth.reads))
	}
	for i, want := range wantBatches {
		if got := len(requestedUeIds(auth.reads[i])); got != want {
			t.Errorf("expected credential read %d to ask for %d IMSIs, got %d", i, want, got)
		}
	}
	if len(common.reads) != len(wantBatches)*len(subscriberSyncCollections) {
		t.Errorf("expected one read per collection per batch, got %d", len(common.reads))
	}
	if len(common.bulks) != len(wantBatches)*len(subscriberSyncCollections) {
		t.Fatalf("expected one bulk write per collection per batch, got %d", len(common.bulks))
	}
	for i, bulk := range common.bulks {
		want := wantBatches[i/len(subscriberSyncCollections)]
		if got := len(bulk[opsKey].([]dbadapter.UpsertOperation)); got != want {
			t.Errorf("expected bulk write %d to carry %d documents, got %d", i, want, got)
		}
	}
}

func TestSyncProvisionedSubscribersSkipsImsisWithoutCredentials(t *testing.T) {
	imsis := testImsis(3)
	common := &bulkSubscriberSyncMockDBClient{}
	useSyncClients(t, common, &credentialsMockDBClient{imsis: imsis[1:2]})

	report, err := syncProvisionedSubscribers(imsis, nil, testSnssai(), testDnnMap(), testSyncMcc, testSyncMnc, testAggregatedQoS())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.provisioned != 1 || report.written != 5 {
		t.Fatalf("expected only the IMSI with credentials provisioned, got %+v", report)
	}
	for _, bulk := range common.bulks {
		for _, op := range bulk[opsKey].([]dbadapter.UpsertOperation) {
			if op.Filter[ueIdKey] != "imsi-"+imsis[1] {
				t.Errorf("expected writes for %s only, got %v", imsis[1], op.Filter)
			}
		}
	}
}

func TestSyncProvisionedSubscribersWritesOneAtATimeWithoutBulkWrite(t *testing.T) {
	imsis := testImsis(2)
	common := &subscriberSyncMockDBClient{}
	useSyncClients(t, common, &credentialsMockDBClient{imsis: imsis})

	report, err := syncProvisionedSubscribers(imsis, nil, testSnssai(), testDnnMap(), testSyncMcc, testSyncMnc, testAggregatedQoS())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.written != 10 || len(common.putData) != 10 {
		t.Fatalf("expected 10 single writes, got report %+v and %d writes", report, len(common.putData))
	}
	for i, write := range common.putData {
		if want := subscriberSyncCollections[i/2]; write[collKey] != want {
			t.Errorf("expected write %d on %s, got %s", i, want, write[collKey])
		}
	}
}

func TestSyncProvisionedSubscribersStopsOnAReadError(t *testing.T) {
	imsis := testImsis(2)
	readErr := errors.New("connection reset")

	common := &bulkSubscriberSyncMockDBClient{}
	useSyncClients(t, common, &credentialsMockDBClient{imsis: imsis, err: readErr})
	_, err := syncProvisionedSubscribers(imsis, nil, testSnssai(), testDnnMap(), testSyncMcc, testSyncMnc, testAggregatedQoS())
	if !errors.Is(err, readErr) {
		t.Fatalf("expected the credential read error, got %v", err)
	}
	if len(common.bulks) != 0 {
		t.Fatalf("expected no writes after a failed credential read, got %d", len(common.bulks))
	}

	common = &bulkSubscriberSyncMockDBClient{subscriberSyncMockDBClient: subscriberSyncMockDBClient{readErr: readErr}}
	useSyncClients(t, common, &credentialsMockDBClient{imsis: imsis})
	_, err = syncProvisionedSubscribers(imsis, nil, testSnssai(), testDnnMap(), testSyncMcc, testSyncMnc, testAggregatedQoS())
	if !errors.Is(err, readErr) || !strings.Contains(err.Error(), amPolicyDataColl) {
		t.Fatalf("expected the read error naming the collection, got %v", err)
	}
	if len(common.bulks) != 0 {
		t.Fatalf("expected no writes after a failed collection read, got %d", len(common.bulks))
	}
}

func TestDocumentHas(t *testing.T) {
	testCases := []struct {
		name   string
		stored map[string]any
		data   map[string]any
		want   bool
	}{
		{
			name:   "same fields, same values",
			stored: map[string]any{"a": "x", "n": float64(8)},
			data:   map[string]any{"a": "x", "n": float64(8)},
			want:   true,
		},
		{
			name:   "fields the sync does not own are left alone",
			stored: map[string]any{"a": "x", "extra": "kept"},
			data:   map[string]any{"a": "x"},
			want:   true,
		},
		{
			name:   "missing field",
			stored: map[string]any{"a": "x"},
			data:   map[string]any{"a": "x", "b": "y"},
			want:   false,
		},
		{
			name:   "changed value",
			stored: map[string]any{"a": "x"},
			data:   map[string]any{"a": "y"},
			want:   false,
		},
		{
			name:   "same number in another type is a change the database would make",
			stored: map[string]any{"n": float64(8)},
			data:   map[string]any{"n": int32(8)},
			want:   false,
		},
		{
			name:   "nested documents compare by content, not by key order",
			stored: map[string]any{"d": map[string]any{"p": "1", "q": "2"}},
			data:   map[string]any{"d": bson.M{"q": "2", "p": "1"}},
			want:   true,
		},
		{
			name:   "nested document with a changed leaf",
			stored: map[string]any{"d": map[string]any{"p": "1", "q": "2"}},
			data:   map[string]any{"d": map[string]any{"p": "1", "q": "3"}},
			want:   false,
		},
		{
			name:   "arrays compare element by element",
			stored: map[string]any{"l": bson.A{"a", "b"}},
			data:   map[string]any{"l": []string{"a", "b"}},
			want:   true,
		},
		{
			name:   "arrays of different length",
			stored: map[string]any{"l": bson.A{"a"}},
			data:   map[string]any{"l": []string{"a", "b"}},
			want:   false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := documentHas(tc.stored, tc.data)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestFilterMatches(t *testing.T) {
	plmnFilter := bson.M{ueIdKey: testSubscriberImsi, servingPlmnIdKey: "20893"}
	amDataFilter := bson.M{
		ueIdKey: testSubscriberImsi,
		"$or":   []bson.M{{servingPlmnIdKey: "20893"}, {servingPlmnIdKey: bson.M{"$exists": false}}},
	}
	testCases := []struct {
		name   string
		doc    map[string]any
		filter bson.M
		want   bool
	}{
		{"equality on every field", map[string]any{ueIdKey: testSubscriberImsi, servingPlmnIdKey: "20893"}, plmnFilter, true},
		{"another PLMN", map[string]any{ueIdKey: testSubscriberImsi, servingPlmnIdKey: "00101"}, plmnFilter, false},
		{"field missing", map[string]any{ueIdKey: testSubscriberImsi}, plmnFilter, false},
		{"$or on the PLMN", map[string]any{ueIdKey: testSubscriberImsi, servingPlmnIdKey: "20893"}, amDataFilter, true},
		{"$or on the missing PLMN", map[string]any{ueIdKey: testSubscriberImsi}, amDataFilter, true},
		{"$or with another PLMN", map[string]any{ueIdKey: testSubscriberImsi, servingPlmnIdKey: "00101"}, amDataFilter, false},
		{"an operator this does not know is no match", map[string]any{ueIdKey: testSubscriberImsi}, bson.M{ueIdKey: bson.M{"$regex": "imsi-"}}, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := filterMatches(tc.doc, tc.filter); got != tc.want {
				t.Errorf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestSubscriberDocumentsFailWithoutATrafficClass(t *testing.T) {
	dnnMap := map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos{dnnInternet: {{DnnMbrUplink: 1}}}
	if _, err := subscriberDocuments("208930000000001", "", testSnssai(), dnnMap, testSyncMcc, testSyncMnc, testAggregatedQoS()); err == nil {
		t.Fatal("expected an error for a DNN without a traffic class")
	}
}
