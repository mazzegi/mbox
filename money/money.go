package money

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// Currency represents the corresponding Unit of the Money Amount
type Currency string

const (
	EUR Currency = "EUR"
	USD Currency = "USD"
	GBP Currency = "GBP"
)

func (c Currency) MarshalJSON() ([]byte, error) {
	switch c {
	case EUR:
		return []byte(`"EUR"`), nil
	case USD:
		return []byte(`"USD"`), nil
	case GBP:
		return []byte(`"GBP"`), nil
	default:
		return []byte(`"NA"`), nil
	}
}

// Amount represents the Value of Money in corresponding Cents
type Amount int64

// Money represents an amount of money in a particular currency
type Money struct {
	Amount   Amount   `json:"amount"`
	Currency Currency `json:"currency"`
}

func New(a Amount, c Currency) Money {
	return Money{
		Amount:   a,
		Currency: c,
	}
}

func Decimal(v float64, c Currency) Money {
	return New(Amount(math.Round(v*100.0)), c)
}

func Euro(v float64) Money {
	return Decimal(v, EUR)
}

func Cents(v int64, c Currency) Money {
	return New(Amount(v), c)
}

func (m Money) IsZero() bool {
	return m.Amount == 0
}

func (m Money) String() string {
	return fmt.Sprintf("%.2f%s", m.AmountFloat64(), m.Currency)
}

func (m Money) GermanString() string {
	s := fmt.Sprintf("%.2f%s", m.AmountFloat64(), m.Currency)
	s = strings.ReplaceAll(s, ".", ",")
	return s
}

func (m Money) FormatHR(currencySymbol string) string {
	p := message.NewPrinter(language.German)
	return fmt.Sprintf("%s %s", p.Sprintf("%.2f", m.AmountFloat64()), currencySymbol)
}

func (m Money) AmountFloat64() float64 {
	return float64(m.Amount) / 100.0
}

func (m Money) Add(om Money) Money {
	//TODO: check currency
	return New(m.Amount+om.Amount, m.Currency)
}

func (m Money) Sub(om Money) Money {
	//TODO: check currency
	return New(m.Amount-om.Amount, m.Currency)
}

func (m Money) SubNegToZero(om Money) Money {
	//TODO: check currency
	sm := New(m.Amount-om.Amount, m.Currency)
	if sm.Amount < 0 {
		sm.Amount = 0
	}
	return sm
}

func (m Money) Div(v float64) Money {
	//TODO: check currency and calculation method
	return Decimal(m.AmountFloat64()/v, m.Currency)
}

func (m Money) Mult(v float64) Money {
	//TODO: check currency and calculation method
	return Decimal(m.AmountFloat64()*v, m.Currency)
}

func (m Money) Times(n int) Money {
	return New(m.Amount*Amount(n), m.Currency)
}

//JSON
// func (m Money) MarshalJSON() ([]byte, error) {
// 	return []byte(fmt.Sprintf(`{"amount":%d,"currency":"%s"}`, m.Amount, m.Currency)), nil
// }

type umjMoney Money

func (m *Money) UnmarshalJSON(data []byte) error {
	// try default
	var um umjMoney
	if err := json.Unmarshal(data, &um); err == nil {
		*m = Money(um)
		return nil
	}

	// test if data is float or int
	if v, err := strconv.ParseFloat(string(data), 64); err == nil {
		*m = Decimal(v, EUR)
		return nil
	}
	if v, err := strconv.ParseInt(string(data), 10, 64); err == nil {
		*m = Decimal(float64(v), EUR)
		return nil
	}

	return fmt.Errorf("UnmarshalJSON: cannot unmarshal %q into money.Money", string(data))
}
