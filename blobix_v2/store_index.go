package blobix_v2

type IndexFieldType string

const (
	IndexFieldAny    IndexFieldType = ""
	IndexFieldString IndexFieldType = "string"
	IndexFieldInt    IndexFieldType = "int"
	IndexFieldFloat  IndexFieldType = "float"
)

type IndexFieldDescriptor struct {
	Name string         `json:"name"`
	Type IndexFieldType `json:"type"`
	Tag  string         `json:"tag"`
}

type IndexField[T any] struct {
	Descriptor IndexFieldDescriptor
	ValueFunc  func(T) any
}

func IF[T any](name string, typ IndexFieldType, tag string, valueFnc func(T) any) IndexField[T] {
	return IndexField[T]{
		Descriptor: IndexFieldDescriptor{
			Name: name,
			Type: typ,
			Tag:  tag,
		},
		ValueFunc: valueFnc,
	}
}

type IndexDescriptor struct {
	BucketName string
	IndexName  string
	Fields     []IndexFieldDescriptor
}

func IndexDescriptorsEqual(id1, id2 IndexDescriptor) bool {
	if id1.BucketName != id2.BucketName ||
		id1.IndexName != id2.IndexName {
		return false
	}
	if len(id1.Fields) != len(id2.Fields) {
		return false
	}
	for i, f1 := range id1.Fields {
		f2 := id2.Fields[i]
		if f1 != f2 {
			return false
		}
	}
	return true
}
