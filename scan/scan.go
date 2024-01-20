package scan

import (
	"fmt"
	"reflect"
)

func Slice[T any, S any](srcSl []S) (T, error) {
	var t T
	if reflect.TypeOf(t).Kind() != reflect.Struct {
		return t, fmt.Errorf("cannot scan into non-struct types")
	}

	rv := reflect.ValueOf(&t).Elem()
	for i := 0; i < rv.NumField(); i++ {
		if i >= len(srcSl) {
			break
		}
		src := srcSl[i]

		toElem := rv.Field(i)
		if !toElem.CanSet() {
			return t, fmt.Errorf("cannot set %s", toElem.Type().String())
		}

		rvSrc := reflect.ValueOf(src)
		if !rvSrc.CanConvert(toElem.Type()) {
			return t, fmt.Errorf("cannot convert %T to %s", src, toElem.Type().String())
		}
		crv := rvSrc.Convert(toElem.Type())
		toElem.Set(crv)
	}

	return t, nil
}
