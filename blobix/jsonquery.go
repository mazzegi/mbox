package blobix

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

var (
	ErrNotFound = fmt.Errorf("not-found")
	ErrBadArgs  = fmt.Errorf("bad-args")
)

func isRVZero(rv reflect.Value) bool {
	zv := reflect.Value{}
	return rv == zv
}

// match on either field-name or json-name
func structFieldByName(sv reflect.Value, name string) reflect.Value {
	ty := sv.Type()
	for i := 0; i < ty.NumField(); i++ {
		sf := ty.Field(i)
		if sf.Name == name {
			return sv.Field(i)
		}
		if js := sf.Tag.Get("json"); js != "" {
			jname, _, _ := strings.Cut(js, ",")
			if jname == name {
				return sv.Field(i)
			}
		}
	}
	return reflect.Value{}
}

func jsonQueryValue(in any, spath string) (reflect.Value, error) {
	path := strings.Split(spath, "/")
	crv := reflect.ValueOf(in)
	for _, elt := range path {
		if elt == "" {
			//skip empty
			continue
		}
		rty := crv.Type()
		kind := rty.Kind()
		if kind == reflect.Pointer {
			crv = crv.Elem()
			kind = crv.Kind()
		}

		switch kind {
		case reflect.Struct:
			crv = structFieldByName(crv, elt)
			if isRVZero(crv) {
				return reflect.Value{}, errors.Join(ErrNotFound, fmt.Errorf("no such struct field %q", elt))
			}
		case reflect.Slice:
			ix, err := strconv.ParseInt(elt, 10, 64)
			if err != nil {
				return reflect.Value{}, errors.Join(ErrBadArgs, fmt.Errorf("cannot parse %q as int for slice index: %w", elt, err))
			}
			if ix < 0 {
				return reflect.Value{}, errors.Join(ErrBadArgs, fmt.Errorf("invalid slice index %d", ix))
			}
			if ix >= int64(crv.Len()) {
				return reflect.Value{}, errors.Join(ErrBadArgs, fmt.Errorf("invalid slice index %d ( >= len=%d)", ix, crv.Len()))
			}
			crv = crv.Index(int(ix))
		case reflect.Map:
			if crv.Type().Key().Kind() != reflect.String {
				return reflect.Value{}, fmt.Errorf("cannot query non-string map keys. map keys are %s", crv.Type().Kind().String())
			}
			crv = crv.MapIndex(reflect.ValueOf(elt))
			if isRVZero(crv) {
				return reflect.Value{}, errors.Join(ErrNotFound, fmt.Errorf("no such map key %q", elt))
			}
		case reflect.Interface:
			// is it maybe a map[string]any again?
			ui := crv.Interface()
			if msa, ok := ui.(map[string]any); ok {
				if mapValue, ok := msa[elt]; ok {
					crv = reflect.ValueOf(mapValue)
				} else {
					return reflect.Value{}, errors.Join(ErrNotFound, fmt.Errorf("no such map key %q", elt))
				}
			} else {
				return reflect.Value{}, errors.Join(ErrNotFound, fmt.Errorf("cannot query interface-kind %T", crv.Interface()))
			}

			// msaType := reflect.TypeOf(map[string]any{})
			// if crv.CanConvert(msaType) {
			// 	msa := crv.Convert(msaType)
			// 	crv = msa.MapIndex(reflect.ValueOf(elt))
			// 	if isRVZero(crv) {
			// 		return reflect.Value{}, errors.Join(ErrNotFound, fmt.Errorf("no such map key %q", elt))
			// 	}
			// } else {
			// 	return reflect.Value{}, errors.Join(ErrNotFound, fmt.Errorf("cannot query interface-kind %T", crv.Interface()))
			// }

		default:
			return reflect.Value{}, errors.Join(ErrNotFound, fmt.Errorf("cannot query reflect-kind %T", kind))
		}
	}
	return crv, nil
}

func JSONQuery(in any, spath string) (any, error) {
	rv, err := jsonQueryValue(in, spath)
	if err != nil {
		return nil, fmt.Errorf("query-value: %w", err)
	}
	return rv.Interface(), nil
}
