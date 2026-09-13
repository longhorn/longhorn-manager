package util

import (
	"fmt"
)

type StructName string

type StructFields []StructField

// StructField contains information about a single field in a struct.
type StructField struct {
	Name  string
	Value any
	Tag   string
}

func (sf *StructFields) Append(name StructName, value any) {
	*sf = append(*sf, StructField{
		Name:  string(name),
		Value: value,
		Tag:   sf.ConvertTag(name),
	})
}

func (sf *StructFields) AppendCounted(structMap map[StructName]int) {
	for name, value := range structMap {
		sf.Append(name, value)
	}
}

func (sf *StructFields) ConvertTag(name StructName) string {
	return ConvertFirstCharToLower(fmt.Sprint(name))
}

// ToMap flattens the StructFields slice into a map keyed by JSON tag,
// suitable for direct json.Marshal encoding. Tag uniqueness is ensured
// during appending structField via ConvertTag.
func (sf *StructFields) ToMap() map[string]any {
	result := make(map[string]any, len(*sf))
	for _, field := range *sf {
		result[field.Tag] = field.Value
	}
	return result
}
