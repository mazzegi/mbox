package blobix

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/mazzegi/mbox/query"
	"github.com/mazzegi/mbox/testx"
)

func cloneStrings(sl []string) []string {
	csl := make([]string, len(sl))
	copy(csl, sl)
	return csl
}

func stringsEqual(sl1, sl2 []string) bool {
	csl1 := cloneStrings(sl1)
	csl2 := cloneStrings(sl2)
	sort.Strings(csl1)
	sort.Strings(csl2)
	return reflect.DeepEqual(csl1, csl2)
}

func TestSqlite(t *testing.T) {
	dsn := "test_sqlite.db"
	s, err := NewSqliteXStore(dsn)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer func() {
		s.Close()
		os.Remove(dsn)
		os.Remove(dsn + "-shm")
		os.Remove(dsn + "-wal")
	}()
	testStore(t, s)
	testKeysPaging(t, s)
	testKeysPageStream(t, s)
	testQuery(t, s)
}

func testStore(t *testing.T, s Store) {
	testStoreBasicOps(t, s)
}

func testStoreBasicOps(t *testing.T, s Store) {

	keys := []string{"key22", "key12", "key93", "key99", "key01"}
	value := func(key string) string {
		return "value_for_" + key
	}
	bucket := s.Bucket("bucket42")

	for _, k := range keys {
		err := bucket.PutJSON(k, value(k))
		testx.AssertNoErr(t, err)
	}

	// read stuff
	rkeys, err := bucket.Keys()
	if err != nil {
		t.Fatalf("read-keys: %v", err)
	}
	if !stringsEqual(keys, rkeys) {
		t.Fatalf("keys, rkeys not equal")
	}

	// check vals
	for _, k := range keys {
		var rv string
		_, err := bucket.JSON(k, &rv)
		testx.AssertNoErr(t, err)
		testx.AssertEqual(t, value(k), rv)
	}

	//delete key
	delKeys := keys[:2]
	adkeys := cloneStrings(keys[2:])
	err = bucket.Delete(delKeys...)
	if err != nil {
		t.Fatalf("delete keys %v: %v", delKeys, err)
	}

	adrkeys, err := bucket.Keys()
	if err != nil {
		t.Fatalf("read-keys: %v", err)
	}
	if !stringsEqual(adkeys, adrkeys) {
		t.Fatalf("keys, rkeys not equal %v != %v", adkeys, adrkeys)
	}

	//test prefix
	prefixkeys, err := bucket.KeysWithPrefix("key9")
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, stringsEqual(prefixkeys, []string{"key93", "key99"}), true)

	var values []string
	for _, k := range adrkeys {
		values = append(values, value(k))
	}

	rvalues, err := AllValues[string](bucket)
	if err != nil {
		t.Fatalf("all-values failed with: %v", err)
	}
	if !stringsEqual(values, rvalues) {
		t.Fatalf("each-json values, rvalues not equal %v != %v", values, rvalues)
	}

}

func testKeysPaging(t *testing.T, store Store) {
	bucket := store.Bucket("paging_bucket")
	count := 723
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key_%05d", i)
		keys[i] = key
		bucket.PutJSON(key, key)
	}

	var readKeys []string
	const limit = 50
	skip := 0
	for {
		ks, err := bucket.KeysPage(skip, limit, query.SortASC)
		testx.AssertNoErr(t, err)
		readKeys = append(readKeys, ks...)
		if len(ks) < limit {
			break
		}
		skip += limit
	}
	if !stringsEqual(keys, readKeys) {
		t.Fatalf("keys paging: input and output not equal")
	}
}

func testKeysPageStream(t *testing.T, store Store) {
	bucket := store.Bucket("stream_paging_bucket")
	count := 723
	keys := make([]string, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key_%05d", i)
		keys[i] = key
		bucket.PutJSON(key, key)
	}

	var readKeys []string
	for kp := range StreamKeys(bucket, 52) {
		if kp.Error != nil {
			testx.AssertNoErr(t, kp.Error)
		}
		readKeys = append(readKeys, kp.Keys...)
	}

	if !stringsEqual(keys, readKeys) {
		t.Fatalf("keys paging: input and output not equal")
	}
}

func testQuery(t *testing.T, store Store) {
	type testQueryType struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}

	bucket := store.Bucket("query_bucket")
	bucket.AddIndex("test",
		IF("name", "name"),
		IF("value", "value"),
	)

	// generate combis
	tx := testx.NewTx(t)
	nameCount := 5
	valueCount := 10
	for in := range nameCount {
		name := fmt.Sprintf("name_%05d", in+1)
		for iv := range valueCount {
			value := fmt.Sprintf("value_%05d", iv+1)
			err := bucket.PutJSON(fmt.Sprintf("key_%05d", in*valueCount+iv), testQueryType{
				Name:  name,
				Value: value,
			})
			tx.AssertNoErr(err)
		}
	}

	//
	q := query.Query{
		LimitOffset: query.LO(100, 0),
		Conditions: []query.Condition{
			query.C("name", query.ComparatorIn, []string{
				"name_00001",
				"name_00005",
			}),
			query.C("value", query.ComparatorEqual, "value_00003"),
		},
	}
	keys, err := bucket.QueryKeys("test", q.LimitOffset, q.Conditions, q.Sorts, q.Search)
	tx.AssertNoErr(err)
	wantKeys := []string{"key_00002", "key_00042"}
	tx.AssertEqual(wantKeys, keys)

	qVals, err := Values[testQueryType](bucket, keys...)
	tx.AssertNoErr(err)
	wantVals := []testQueryType{
		{Name: "name_00001", Value: "value_00003"},
		{Name: "name_00005", Value: "value_00003"},
	}
	tx.AssertEqual(wantVals, qVals)
}
