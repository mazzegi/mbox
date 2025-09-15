package blobix_v2

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/mazzegi/mbox/mathx"
	"github.com/mazzegi/mbox/query"
	"github.com/mazzegi/mbox/testx"
)

type TestStoreType struct {
	Key     string
	String1 string  `json:"string_1"`
	String2 string  `json:"string_2"`
	String3 string  `json:"string_3"`
	Int1    int     `json:"int_1"`
	Int2    int     `json:"int_2"`
	Int3    int     `json:"int_3"`
	Float1  float64 `json:"float_1"`
	Float2  float64 `json:"float_2"`
	Float3  float64 `json:"float_3"`
	Bool1   bool    `json:"bool_1"`
	Bool2   bool    `json:"bool_2"`
}

func NewTestStoreType(key string, n int) TestStoreType {
	return TestStoreType{
		Key:     key,
		String1: fmt.Sprintf("string_1_val_%d", n),
		String2: fmt.Sprintf("string_2_val_%d", n%10),
		String3: fmt.Sprintf("string_3_val_%d", n%50),
		Int1:    n,
		Int2:    n % 10,
		Int3:    n % 50,
		Float1:  mathx.RoundPlaces(1.001+float64(n), 6),
		Float2:  mathx.RoundPlaces(2.002+float64(n%10), 6),
		Float3:  mathx.RoundPlaces(3.003+float64(n%50), 6),
		Bool1:   n%2 == 0,
		Bool2:   n%2 != 0,
	}
}

func TestStoreBase(t *testing.T) {
	tx := testx.NewTx(t)

	tmpFolderName := fmt.Sprintf("test_%s", time.Now().Format("20060102_150405"))
	err := os.MkdirAll(tmpFolderName, os.ModePerm)
	tx.AssertNoErr(err)
	defer os.RemoveAll(tmpFolderName)

	storeFile := filepath.Join(tmpFolderName, "test.db")
	store, err := NewSqliteXStore(storeFile)
	tx.AssertNoErr(err)
	defer store.Close()

	bucket := NewBucket[TestStoreType](store, "test_type")

	// create records
	numRecords := 100
	for n := range numRecords {
		key := fmt.Sprintf("test_key_%06d", n)
		t := NewTestStoreType(key, n+1)
		err := bucket.Save(key, t)
		tx.AssertNoErr(err)
	}
	// test if records are present in store
	for n := range numRecords {
		key := fmt.Sprintf("test_key_%06d", n)
		expectt := NewTestStoreType(key, n+1)
		havet, ok, err := bucket.Find(key)
		tx.AssertNoErr(err)
		tx.AssertEqual(query.Found(true), ok)
		tx.AssertEqual(expectt, havet)
	}

	// create records to save many
	numRecords = 100
	tuples := make([]Tuple[string, TestStoreType], numRecords)
	keys := make([]string, numRecords)
	saved := map[string]TestStoreType{}
	for n := range numRecords {
		key := fmt.Sprintf("test_many_key_%06d", n)
		t := NewTestStoreType(key, n+1)
		tuples[n] = MkTuple(key, t)
		keys[n] = key
		saved[key] = t
	}
	err = bucket.SaveMany(tuples)
	tx.AssertNoErr(err)
	// test if records are present in store
	kvs, err := bucket.KeyValues(keys...)
	tx.AssertNoErr(err)
	for _, key := range keys {
		t, ok := kvs[key]
		tx.AssertEqual(true, ok)
		expectt, ok := saved[key]
		tx.AssertEqual(true, ok)
		tx.AssertEqual(expectt, t)
	}
}

func TestStoreIndex(t *testing.T) {
	tx := testx.NewTx(t)

	tmpFolderName := fmt.Sprintf("test_index_%s", time.Now().Format("20060102_150405"))
	err := os.MkdirAll(tmpFolderName, os.ModePerm)
	tx.AssertNoErr(err)
	defer os.RemoveAll(tmpFolderName)

	storeFile := filepath.Join(tmpFolderName, "test.db")
	store, err := NewSqliteXStore(storeFile)
	tx.AssertNoErr(err)
	defer store.Close()

	bucket := NewBucket[TestStoreType](store, "test_type")
	err = bucket.AddOrUpdateIndex("default",
		IF("string_1", IndexFieldString, "v1", func(t TestStoreType) any { return t.String1 }),
		IF("int_2", IndexFieldString, "v1", func(t TestStoreType) any { return t.Int2 }),
		IF("float_3", IndexFieldString, "v1", func(t TestStoreType) any { return t.Float3 }),
		IF("bool_1", IndexFieldString, "v1", func(t TestStoreType) any { return t.Bool1 }),
		IF("strings_2_3_added", IndexFieldString, "v1", func(t TestStoreType) any { return t.String2 + t.String3 }),
	)
	tx.AssertNoErr(err)

	// create records
	numRecords := 100
	int2Values := map[int][]TestStoreType{}
	strings23AddedValues := map[string][]TestStoreType{}
	for n := range numRecords {
		key := fmt.Sprintf("test_key_%06d", n)
		t := NewTestStoreType(key, n+1)
		err := bucket.Save(key, t)
		tx.AssertNoErr(err)

		int2Values[t.Int2] = append(int2Values[t.Int2], t)
		strings23AddedValues[t.String2+t.String3] = append(strings23AddedValues[t.String2+t.String3], t)
	}

	//test a representative sample
	checkValue := 5
	int2_5_Values, ok := int2Values[checkValue]
	tx.AssertEqual(true, ok)
	q := query.Query{
		LimitOffset: query.LO(1_000, 0),
		Conditions: []query.Condition{
			query.C("int_2", query.ComparatorEqual, checkValue),
		},
	}
	qValues, err := bucket.Query("default", q)
	tx.AssertNoErr(err)
	findQValueByKey := func(key string) (TestStoreType, bool) {
		for _, qv := range qValues {
			if qv.Key == key {
				return qv, true
			}
		}
		return TestStoreType{}, false
	}

	// check if we have all expected values
	for _, expVal := range int2_5_Values {
		qv, ok := findQValueByKey(expVal.Key)
		tx.AssertEqual(true, ok)
		tx.AssertEqual(expVal, qv)
	}
}

func TestStoreIterKeys(t *testing.T) {
	tx := testx.NewTx(t)

	tmpFolderName := fmt.Sprintf("test_iter_keys_%s", time.Now().Format("20060102_150405"))
	err := os.MkdirAll(tmpFolderName, os.ModePerm)
	tx.AssertNoErr(err)
	defer os.RemoveAll(tmpFolderName)

	storeFile := filepath.Join(tmpFolderName, "test.db")
	store, err := NewSqliteXStore(storeFile)
	tx.AssertNoErr(err)
	defer store.Close()

	bucket := NewBucket[TestStoreType](store, "test_type")

	numRecords := 812
	allKeys := make([]string, numRecords)
	for n := range numRecords {
		key := fmt.Sprintf("test_key_%06d", n)
		allKeys[n] = key
		t := NewTestStoreType(key, n+1)
		err := bucket.Save(key, t)
		tx.AssertNoErr(err)
	}
	sort.Strings(allKeys)
	//
	// now iter keys
	var foundKeys []string
	for key, err := range bucket.IterKeys() {
		tx.AssertNoErr(err)
		foundKeys = append(foundKeys, key)
	}
	sort.Strings(foundKeys)
	tx.AssertEqual(allKeys, foundKeys)

	// now with errFunc

	errFunc := func(n int) error {
		if n == 112 {
			return fmt.Errorf("112 is enough")
		}
		return nil
	}
	idx := 0
	for _, err := range bucket.testIterKeysWithErrFunc(errFunc) {
		if idx < 112 {
			tx.AssertNoErr(err)
		} else {
			tx.AssertErr(err)
		}
		idx++
	}

	// now with break
	idx = 0
	for _, err := range bucket.testIterKeysWithErrFunc(func(int) error { return nil }) {
		tx.AssertNoErr(err)
		if idx >= 112 {
			break
		}
		idx++
	}
	// to come until here is enough to pass the test - if the iterator doesn't react correctly to break a panic occurs
}
