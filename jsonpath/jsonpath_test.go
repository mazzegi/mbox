package jsonpath

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/mazzegi/mbox/clock"
	"github.com/mazzegi/mbox/date"
	"github.com/mazzegi/mbox/testx"
)

type TT1 struct {
	F1 string
	F2 int
	F3 float64
	T2 TT2
	EmbeddedT
}

type TT2 struct {
	G1 string
	G2 int
	G3 float64
	S1 []int
	S2 []TT3
}

type TT3 struct {
	H1   string
	H2   int
	M1   map[string]string
	M2   map[string]TT1
	M3   map[string][]int
	TT2s []TT2
}

type EmbeddedT struct {
	E1 string
}

func TestQuery(t *testing.T) {
	tx := testx.NewTx(t)

	v := TT1{
		F1: "a-string",
		F2: 42,
		F3: 0.42,
		T2: TT2{
			G1: "a-g-string",
			G2: 433,
			G3: 12.433,
			S1: []int{2, 3, 4},
			S2: []TT3{
				{
					H1: "h1-s",
					H2: 1,
				},
				{
					H1: "h2-s",
					H2: 2,
					M1: map[string]string{
						"cows": "are-flying",
						"cats": "are-swimming",
					},
					M2: map[string]TT1{
						"moon_01": {
							F1: "moon_string",
							F2: 23,
						},
					},
					TT2s: []TT2{
						{
							S1: []int{34, 35, 36},
						},
					},
				},
			},
		},
		EmbeddedT: EmbeddedT{
			E1: "embedded_1",
		},
	}

	r, err := Query(v, "F1")
	tx.AssertNoErr(err)
	tx.AssertEqual("a-string", r)

	r, err = Query(v, "EmbeddedT/E1")
	tx.AssertNoErr(err)
	tx.AssertEqual("embedded_1", r)

	_, err = Query(v, "C1")
	tx.AssertErr(err)

	r, err = Query(v, "T2/G2")
	tx.AssertNoErr(err)
	tx.AssertEqual(433, r)

	r, err = Query(v, "T2/G3")
	tx.AssertNoErr(err)
	tx.AssertEqual(12.433, r)

	r, err = Query(v, "T2/S1/1")
	tx.AssertNoErr(err)
	tx.AssertEqual(3, r)

	r, err = Query(v, "T2/S2/0/H1")
	tx.AssertNoErr(err)
	tx.AssertEqual("h1-s", r)

	r, err = Query(v, "T2/S2/1/M1/cats")
	tx.AssertNoErr(err)
	tx.AssertEqual("are-swimming", r)
}

func TestSet(t *testing.T) {
	tx := testx.NewTx(t)
	v := TT1{
		F1: "a-string",
		F2: 42,
		F3: 0.42,
		T2: TT2{
			G1: "a-g-string",
			G2: 433,
			G3: 12.433,
			S1: []int{2, 3, 4},
			S2: []TT3{
				{
					H1: "h1-s",
					H2: 1,
				},
				{
					H1: "h2-s",
					H2: 2,
					M1: map[string]string{
						"cows": "are-flying",
						"cats": "are-swimming",
					},
					M2: map[string]TT1{
						"moon_01": {
							F1: "moon_string",
							F2: 23,
						},
					},
					M3: map[string][]int{
						"num_1":  {1, 42},
						"num_42": {42, 1},
					},
					TT2s: []TT2{
						{
							S1: []int{34, 35, 36},
						},
					},
				},
			},
		},
		EmbeddedT: EmbeddedT{
			E1: "embedded_1",
		},
	}

	err := Set(&v, "T2/S2/0/H2", 2)
	tx.AssertNoErr(err)
	tx.AssertEqual(2, v.T2.S2[0].H2)

	err = Set(&v, "EmbeddedT/E1", "em2")
	tx.AssertNoErr(err)
	tx.AssertEqual("em2", v.EmbeddedT.E1)

	err = Set(&v, "F1", "hola")
	tx.AssertNoErr(err)
	tx.AssertEqual("hola", v.F1)

	err = Set(&v, "T2/S2/1/TT2s/0/S1/2", 42)
	tx.AssertNoErr(err)
	tx.AssertEqual(42, v.T2.S2[1].TT2s[0].S1[2])

	err = Set(&v, "T2/S2/1/M1/cats", "get milk")
	tx.AssertNoErr(err)
	tx.AssertEqual("get milk", v.T2.S2[1].M1["cats"])

	err = Set(&v, "T2/S2/1/M2/moon_01/F1", "mars_string")
	tx.AssertNoErr(err)
	tx.AssertEqual("mars_string", v.T2.S2[1].M2["moon_01"].F1)

	err = Set(&v, "T2/S2/1/M3/num_42/0", 2)
	tx.AssertNoErr(err)
	tx.AssertEqual(2, v.T2.S2[1].M3["num_42"][0])

	err = Set(&v, "T2/S2/1/M3/num_1/1", 112)
	tx.AssertNoErr(err)
	tx.AssertEqual(112, v.T2.S2[1].M3["num_1"][1])
}

func TestMapValue(t *testing.T) {
	m := map[string]string{
		"k1": "v1",
		"k2": "v2",
	}

	rv := reflect.ValueOf(m)
	mrv1 := rv.MapIndex(reflect.ValueOf("k1"))
	fmt.Printf("%s\n", mrv1.String())

	if !mrv1.CanAddr() {
		fmt.Printf("value cannot addr\n")
	}
	if !mrv1.CanSet() {
		fmt.Printf("value cannot be set\n")
	}
}

type destType struct {
	I16  int16
	PI16 *int16
}

func TestSetTypes(t *testing.T) {
	tx := testx.NewTx(t)
	var v int16 = 8
	dest := destType{
		I16:  v,
		PI16: &v,
	}
	assign(t, &dest, "PI16", int64(42))
	tx.AssertEqual(int(42), int(*dest.PI16))

}

func assign(t *testing.T, dest any, field string, value any) {
	rvd := reflect.ValueOf(dest)
	if rvd.Kind() != reflect.Pointer {
		t.Fatalf("no pointer")
	}
	rvd = rvd.Elem()
	if !rvd.CanSet() {
		t.Fatalf("cannot set dest")
	}
	kind := rvd.Kind()
	if kind != reflect.Struct {
		t.Fatalf("no struct")
	}
	frv := rvd.FieldByName(field)
	if isRVZero(frv) {
		t.Fatalf("field is zero")
	}
	if frv.Kind() == reflect.Pointer {
		frv = frv.Elem()
	}
	if !frv.CanSet() {
		t.Fatalf("cannot set field")
	}

	setVal := reflect.ValueOf(value)
	if !setVal.CanConvert(frv.Type()) {
		t.Fatalf("cannot convert")
	}
	setValConv := setVal.Convert(frv.Type())
	frv.Set(setValConv)
}

type TestSetData struct {
	IntData    int         `json:"int_data"`
	BoolData   bool        `json:"bool_data"`
	FloatData  float64     `json:"float_data"`
	StringData string      `json:"string_data"`
	TimeData   time.Time   `json:"time_data"`
	DateData   date.Date   `json:"date_data"`
	ClockData  clock.Clock `json:"clock_data"`
}

func TestSetValue(t *testing.T) {
	tx := testx.NewTx(t)

	tsd := TestSetData{}
	var err error

	// test set all with strings
	err = Set(&tsd, "int_data", "42")
	tx.AssertNoErr(err)
	tx.AssertEqual(42, tsd.IntData)

	err = Set(&tsd, "bool_data", "on")
	tx.AssertNoErr(err)
	tx.AssertEqual(true, tsd.BoolData)

	err = Set(&tsd, "float_data", "3.1415")
	tx.AssertNoErr(err)
	tx.AssertEqual(3.1415, tsd.FloatData)

	err = Set(&tsd, "string_data", "dubidu")
	tx.AssertNoErr(err)
	tx.AssertEqual("dubidu", tsd.StringData)

	err = Set(&tsd, "time_data", "2024-06-24T14:56:23.345Z")
	tx.AssertNoErr(err)
	tx.AssertEqual("24062024145623", tsd.TimeData.Format("02012006150405"))

	err = Set(&tsd, "date_data", "2024-06-24")
	tx.AssertNoErr(err)
	tx.AssertEqual("24.06.2024", tsd.DateData.FormatInLayout("02.01.2006"))

	err = Set(&tsd, "clock_data", "14:56")
	tx.AssertNoErr(err)
	tx.AssertEqual("14_56_00", tsd.ClockData.FormatInLayout("15_04_05"))

	// test set integers
	err = Set(&tsd, "float_data", 43)
	tx.AssertNoErr(err)
	tx.AssertEqual(43.0, tsd.FloatData)

	err = Set(&tsd, "string_data", 44)
	tx.AssertNoErr(err)
	tx.AssertEqual("44", tsd.StringData)

	// test set floats
	err = Set(&tsd, "int_data", 4.13)
	tx.AssertNoErr(err)
	tx.AssertEqual(4, tsd.IntData)

	err = Set(&tsd, "string_data", 4.13)
	tx.AssertNoErr(err)
	tx.AssertEqual("4.13", tsd.StringData)
}
