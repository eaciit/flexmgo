package flexmgo

import "reflect"

func createPtrFromType(t reflect.Type) reflect.Value {
	isPtr := t.Kind() == reflect.Ptr
	elemType := t

	if isPtr {
		elemType = elemType.Elem()
	}

	if elemType.Kind() == reflect.Map {
		ptr := reflect.New(elemType)
		m := reflect.MakeMap(elemType)
		ptr.Elem().Set(m)
		return ptr
	}

	return reflect.New(elemType)
}
