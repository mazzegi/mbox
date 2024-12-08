package blobix

import (
	"testing"

	"github.com/mazzegi/mbox/testx"
)

type TT1 struct {
	F1 string  `json:"f_1"`
	F2 int     `json:"f_2"`
	F3 float64 `json:"f_3"`
	T2 TT2     `json:"t_2"`
}

type TT2 struct {
	G1 string  `json:"g_1"`
	G2 int     `json:"g_2"`
	G3 float64 `json:"g_3"`
	S1 []int   `json:"s_1"`
	S2 []TT3   `json:"s_2,omitempty"`
}

type TT3 struct {
	H1   string            `json:"h_1"`
	H2   int               `json:"h_2"`
	M1   map[string]string `json:"m_1"`
	TT2s []TT2             `json:"tt_2_s"`
}

func TestQuery(t *testing.T) {
	// fi := FooItem{}
	// res, err := JSONQuery(fi, "Item/name")

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
					TT2s: []TT2{
						{
							S1: []int{34, 35, 36},
						},
					},
				},
			},
		},
	}

	r, err := JSONQuery(v, "F1")
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, "a-string", r)

	// find by json name
	r, err = JSONQuery(v, "f_1")
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, "a-string", r)

	_, err = JSONQuery(v, "C1")
	testx.AssertErr(t, err)

	r, err = JSONQuery(v, "T2/G2")
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, 433, r)

	r, err = JSONQuery(v, "T2/G3")
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, 12.433, r)

	r, err = JSONQuery(v, "T2/S1/1")
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, 3, r)

	r, err = JSONQuery(v, "T2/S2/0/H1")
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, "h1-s", r)

	// find by json name mixed
	r, err = JSONQuery(v, "T2/s_2/0/H1")
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, "h1-s", r)

	r, err = JSONQuery(v, "T2/S2/1/M1/cats")
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, "are-swimming", r)
}

type Item struct {
	Name string `json:"name"`
}

type FooItem struct {
	Item `json:"item"`
}

func TestQueryEmbedded(t *testing.T) {
	tx := testx.NewTx(t)

	v := FooItem{
		Item: Item{
			Name: "bar",
		},
	}

	r, err := JSONQuery(v, "item/name")
	tx.AssertNoErr(err)
	tx.AssertEqual("bar", r)
}

func TestQueryMap(t *testing.T) {
	in := map[string]any{
		"data": map[string]any{
			"foo": "bar",
		},
	}
	val, err := JSONQuery(in, "data/foo")
	testx.AssertNoErr(t, err)
	testx.AssertEqual(t, "bar", val)
}
