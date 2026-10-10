// SPDX-FileCopyrightText: 2026 Benjamin Grewell
// SPDX-License-Identifier: Apache-2.0

package configapi

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/omec-project/openapi/v2"
	"github.com/omec-project/openapi/v2/models"
	"github.com/omec-project/webconsole/backend/logger"
	"github.com/omec-project/webconsole/configmodels"
	"github.com/omec-project/webconsole/dbadapter"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// subscriberSyncBatchSize is how many IMSIs one round trip covers: one $in
// read of their credentials, then one $in read and one bulk write per
// collection. At this size the largest read answer and the largest write are
// about a megabyte, far inside the driver's message limit, and a failed batch
// names a manageable set of IMSIs.
const subscriberSyncBatchSize = 1000

// subscriberSyncCollections are the per-subscriber collections a device group
// provisions, in the order the per-IMSI code used to write them.
var subscriberSyncCollections = []string{amPolicyDataColl, smPolicyDataColl, amDataColl, smDataColl, smfSelDataColl}

// subscriberDocument is one document a device group provisions for one IMSI:
// the collection it lives in, the filter that selects it, and what it has to
// contain.
type subscriberDocument struct {
	coll   string
	filter bson.M
	data   map[string]any
}

// subscriberSyncReport says what a sync did, for the one log line that
// replaces the per-IMSI ones.
type subscriberSyncReport struct {
	imsis       int
	provisioned int
	compared    int
	written     int
	elapsed     time.Duration
}

func (r subscriberSyncReport) String() string {
	return fmt.Sprintf("%d IMSIs, %d with credentials, %d documents compared, %d written, %d unchanged, in %s",
		r.imsis, r.provisioned, r.compared, r.written, r.compared-r.written, r.elapsed.Round(time.Millisecond))
}

// syncProvisionedSubscribers writes the policy and provisioned data of every
// IMSI in imsis that has credentials, for the slice snssai on PLMN mcc/mnc with
// the group's DNNs and QoS, and reports what it did. msisdns pairs with imsis
// by position; an IMSI past its end has no GPSI.
//
// It reads before it writes: the credentials of a batch of IMSIs in one
// query, then each collection's stored documents for that batch in one query,
// and it writes only the documents that are missing or differ from what the
// group and slice say, in one bulk write per collection. A re-POST of an
// unchanged group or slice, which SimApp sends on every restart, therefore
// costs reads only, and a first provisioning costs a handful of round trips
// per thousand IMSIs instead of six per IMSI. A document that differs is still
// written whole, so this remains the way a subscriber provisioned after its
// group is picked up.
func syncProvisionedSubscribers(imsis, msisdns []string, snssai *models.Snssai, dnnMap map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, mcc, mnc string, aggregatedQoS configmodels.DeviceGroupsIpDomainExpandedUeDnnQos) (subscriberSyncReport, error) {
	started := time.Now()
	report := subscriberSyncReport{imsis: len(imsis)}
	for start := 0; start < len(imsis); start += subscriberSyncBatchSize {
		end := min(start+subscriberSyncBatchSize, len(imsis))
		provisioned, err := provisionedImsis(imsis[start:end])
		if err != nil {
			return report, err
		}
		desired := make(map[string][]subscriberDocument, len(subscriberSyncCollections))
		for i := start; i < end; i++ {
			if !provisioned[imsis[i]] {
				continue
			}
			report.provisioned++
			var gpsi string
			if i < len(msisdns) {
				gpsi = msisdns[i]
			}
			docs, err := subscriberDocuments(imsis[i], gpsi, snssai, dnnMap, mcc, mnc, aggregatedQoS)
			if err != nil {
				return report, err
			}
			for _, doc := range docs {
				desired[doc.coll] = append(desired[doc.coll], doc)
			}
		}
		for _, coll := range subscriberSyncCollections {
			written, err := writeChangedDocuments(coll, desired[coll])
			report.compared += len(desired[coll])
			report.written += written
			if err != nil {
				return report, err
			}
		}
	}
	report.elapsed = time.Since(started)
	return report, nil
}

// provisionedImsis returns which of imsis have credentials, read from the
// authentication database in one query.
func provisionedImsis(imsis []string) (map[string]bool, error) {
	ueIds := make([]string, len(imsis))
	for i, imsi := range imsis {
		ueIds[i] = "imsi-" + imsi
	}
	docs, err := dbadapter.AuthDBClient.RestfulAPIGetMany(authSubsDataColl, bson.M{ueIdKey: bson.M{"$in": ueIds}})
	if err != nil {
		return nil, fmt.Errorf("failed to read the credentials of %d subscribers: %w", len(imsis), err)
	}
	provisioned := make(map[string]bool, len(docs))
	for _, doc := range docs {
		if ueId, ok := doc[ueIdKey].(string); ok {
			provisioned[strings.TrimPrefix(ueId, "imsi-")] = true
		}
	}
	return provisioned, nil
}

// writeChangedDocuments reads what coll stores for the subjects of docs in one
// query and upserts the documents that are missing or differ, in one bulk
// write. It returns how many it wrote.
func writeChangedDocuments(coll string, docs []subscriberDocument) (int, error) {
	if len(docs) == 0 {
		return 0, nil
	}
	ueIds := make([]string, 0, len(docs))
	for _, doc := range docs {
		if ueId, ok := doc.filter[ueIdKey].(string); ok {
			ueIds = append(ueIds, ueId)
		}
	}
	stored, err := dbadapter.CommonDBClient.RestfulAPIGetMany(coll, bson.M{ueIdKey: bson.M{"$in": ueIds}})
	if err != nil {
		return 0, fmt.Errorf("failed to read %d documents from %s: %w", len(docs), coll, err)
	}
	byUeId := make(map[string][]map[string]any, len(stored))
	for _, doc := range stored {
		if ueId, ok := doc[ueIdKey].(string); ok {
			byUeId[ueId] = append(byUeId[ueId], doc)
		}
	}
	var ops []dbadapter.UpsertOperation
	for _, doc := range docs {
		ueId, _ := doc.filter[ueIdKey].(string)
		unchanged := false
		for _, candidate := range byUeId[ueId] {
			if !filterMatches(candidate, doc.filter) {
				continue
			}
			unchanged, err = documentHas(candidate, doc.data)
			if err != nil {
				return 0, fmt.Errorf("failed to compare %s for %s: %w", coll, ueId, err)
			}
			break
		}
		if unchanged {
			continue
		}
		logger.DbLog.Debugf("writing %s for %s", coll, ueId)
		ops = append(ops, dbadapter.UpsertOperation{Filter: doc.filter, Data: doc.data})
	}
	if err := dbadapter.BulkUpsert(context.Background(), dbadapter.CommonDBClient, coll, ops); err != nil {
		return 0, fmt.Errorf("failed to write %d documents to %s: %w", len(ops), coll, err)
	}
	return len(ops), nil
}

// filterMatches says whether doc is a document filter selects, for the forms
// the subscriber documents use: equality on a field, $exists on a field, and
// $or over such clauses. Any other form is reported as no match, so the
// document is written rather than wrongly skipped.
func filterMatches(doc map[string]any, filter bson.M) bool {
	for key, want := range filter {
		switch want := want.(type) {
		case []bson.M:
			if key != "$or" {
				return false
			}
			matched := false
			for _, clause := range want {
				if filterMatches(doc, clause) {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		case bson.M:
			exists, ok := want["$exists"].(bool)
			if !ok || len(want) != 1 {
				return false
			}
			if _, present := doc[key]; present != exists {
				return false
			}
		default:
			if !reflect.DeepEqual(doc[key], want) {
				return false
			}
		}
	}
	return true
}

// documentHas says whether stored already carries every field of data as the
// database would store it, so that a $set of data would change nothing. Both
// sides are compared in their BSON form: data comes from JSON (every number a
// float64) or from Go values (an int32 QoS field), a stored document comes
// back with whatever type was written, and the same value in two types is a
// change the database would make.
func documentHas(stored, data map[string]any) (bool, error) {
	storedBSON, err := bsonRoundTrip(stored)
	if err != nil {
		return false, err
	}
	dataBSON, err := bsonRoundTrip(data)
	if err != nil {
		return false, err
	}
	for key, want := range dataBSON {
		got, ok := storedBSON[key]
		if !ok || !reflect.DeepEqual(got, want) {
			return false, nil
		}
	}
	return true, nil
}

// bsonRoundTrip returns doc as a read of it would, with nested documents as
// maps the way the clients decode them.
func bsonRoundTrip(doc map[string]any) (map[string]any, error) {
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}
	decoder := bson.NewDecoder(bson.NewDocumentReader(bytes.NewReader(raw)))
	decoder.DefaultDocumentMap()
	var out map[string]any
	if err := decoder.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// subscriberDocuments are the five documents that provision one IMSI for the
// slice: its AM and SM policy, keyed by ueId, and its AM, SM and SMF-selection
// provisioned data, keyed by ueId and serving PLMN.
func subscriberDocuments(imsi, gpsi string, snssai *models.Snssai, dnnMap map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos, mcc, mnc string, aggregatedQoS configmodels.DeviceGroupsIpDomainExpandedUeDnnQos) ([]subscriberDocument, error) {
	ueId := "imsi-" + imsi
	plmnId := mcc + mnc
	smData, err := buildSmProvisionedDataDocument(snssai, dnnMap, mcc, mnc, imsi)
	if err != nil {
		return nil, err
	}
	return []subscriberDocument{
		{amPolicyDataColl, bson.M{ueIdKey: ueId}, amPolicyDocument(ueId)},
		{smPolicyDataColl, bson.M{ueIdKey: ueId}, smPolicyDocument(ueId, snssai, dnnMap)},
		// A subscriber POST creates the AM data with the ueId alone, so that
		// document is the one to complete rather than one to add beside.
		{amDataColl, bson.M{
			ueIdKey: ueId,
			"$or": []bson.M{
				{servingPlmnIdKey: plmnId},
				{servingPlmnIdKey: bson.M{"$exists": false}},
			},
		}, amProvisionedDocument(ueId, gpsi, plmnId, snssai, aggregatedQoS)},
		{smDataColl, bson.M{ueIdKey: ueId, servingPlmnIdKey: plmnId}, smData},
		{smfSelDataColl, bson.M{ueIdKey: ueId, servingPlmnIdKey: plmnId}, smfSelectionDocument(ueId, plmnId, snssai, dnnMap)},
	}, nil
}

func amPolicyDocument(ueId string) map[string]any {
	var amPolicy models.AmPolicyData
	amPolicy.SubscCats = append(amPolicy.SubscCats, subscCatAether)
	doc := configmodels.ToBsonM(amPolicy)
	doc[ueIdKey] = ueId
	return doc
}

func smPolicyDocument(ueId string, snssai *models.Snssai, dnnMap map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos) map[string]any {
	dnnData := &map[string]models.SmPolicyDnnData{}
	for dnn := range dnnMap {
		(*dnnData)[dnn] = models.SmPolicyDnnData{
			Dnn: dnn,
		}
	}
	var smPolicyData models.SmPolicyData
	var smPolicySnssaiData models.SmPolicySnssaiData
	smPolicySnssaiData.Snssai = *snssai
	smPolicySnssaiData.SmPolicyDnnData = dnnData
	smPolicyData.SmPolicySnssaiData = make(map[string]models.SmPolicySnssaiData)
	smPolicyData.SmPolicySnssaiData[SnssaiModelsToHex(*snssai)] = smPolicySnssaiData
	doc := configmodels.ToBsonM(smPolicyData)
	doc[ueIdKey] = ueId
	return doc
}

func amProvisionedDocument(ueId, gpsi, plmnId string, snssai *models.Snssai, aggregatedQoS configmodels.DeviceGroupsIpDomainExpandedUeDnnQos) map[string]any {
	var gpsis []string
	if gpsi != "" {
		gpsis = []string{gpsi}
	}
	amData := models.AccessAndMobilitySubscriptionData{
		Gpsis: gpsis,
		Nssai: *models.NewNullableNssai(&models.Nssai{
			DefaultSingleNssais: []models.Snssai{*snssai},
			SingleNssais:        []models.Snssai{*snssai},
		}),
		SubscribedUeAmbr: models.NewAmbr(ConvertToString(uint64(aggregatedQoS.DnnMbrUplink)), ConvertToString(uint64(aggregatedQoS.DnnMbrDownlink))),
	}
	doc := configmodels.ToBsonM(amData)
	doc[ueIdKey] = ueId
	doc[servingPlmnIdKey] = plmnId
	return doc
}

func smfSelectionDocument(ueId, plmnId string, snssai *models.Snssai, dnnMap map[string][]configmodels.DeviceGroupsIpDomainExpandedUeDnnQos) map[string]any {
	dnns := make([]string, 0, len(dnnMap))
	for dnn := range dnnMap {
		dnns = append(dnns, dnn)
	}
	// Sorted so that the same DNNs always produce the same document.
	sort.Strings(dnns)
	snssaiInfo := models.SnssaiInfo{
		DnnInfos: []models.DnnInfo{},
	}
	for _, dnn := range dnns {
		snssaiInfo.DnnInfos = append(snssaiInfo.DnnInfos, models.DnnInfo{
			Dnn: models.AccessAndMobilitySubscriptionDataSubscribedDnnListInner{
				String: openapi.PtrString(dnn),
			},
		})
	}
	smfSelData := models.SmfSelectionSubscriptionData{
		SubscribedSnssaiInfos: &map[string]models.SnssaiInfo{},
	}
	(*smfSelData.SubscribedSnssaiInfos)[SnssaiModelsToHex(*snssai)] = snssaiInfo
	doc := configmodels.ToBsonM(smfSelData)
	doc[ueIdKey] = ueId
	doc[servingPlmnIdKey] = plmnId
	return doc
}
