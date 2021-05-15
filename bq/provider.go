package bq

import (
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

type typeProvider struct {
	schemas map[string]bigquery.Schema
}

func NewTypeProvider(schemas map[string]bigquery.Schema) *typeProvider {
	return &typeProvider{schemas: schemas}
}

func (p *typeProvider) EnumValue(enumName string) ref.Val {
	return types.NewErr("unknown enum name '%s'", enumName)
}

func (p *typeProvider) FindIdent(identName string) (ref.Val, bool) {
	return nil, false
}

func (p *typeProvider) findSchema(typeName string) (bigquery.Schema, bool) {
	typeNames := strings.Split(typeName, ".")
	schema, found := p.schemas[typeNames[0]]
	if !found {
		return nil, false
	}
	for _, tn := range typeNames[1:] {
		var s *bigquery.Schema = nil
		for _, fieldSchema := range schema {
			if fieldSchema.Name == tn {
				s = &fieldSchema.Schema
				break
			}
		}
		if s == nil {
			return nil, false
		}
		schema = *s
	}
	return schema, true
}

func (p *typeProvider) FindType(typeName string) (*exprpb.Type, bool) {
	_, found := p.findSchema(typeName)
	if !found {
		return nil, false
	}
	return decls.NewTypeType(decls.NewObjectType(typeName)), true
}

func (p *typeProvider) FindFieldType(messageType string, fieldName string) (*ref.FieldType, bool) {
	schema, found := p.findSchema(messageType)
	if !found {
		return nil, false
	}
	var field *bigquery.FieldSchema = nil
	for _, fieldSchema := range schema {
		if fieldSchema.Name == fieldName {
			field = fieldSchema
			break
		}
	}
	if field == nil {
		return nil, false
	}
	var typ *exprpb.Type
	switch field.Type {
	case bigquery.StringFieldType:
		typ = decls.String
	case bigquery.BytesFieldType:
		typ = decls.Bytes
	case bigquery.BooleanFieldType:
		typ = decls.Bool
	case bigquery.IntegerFieldType:
		typ = decls.Int
	case bigquery.FloatFieldType:
		typ = decls.Double
	case bigquery.RecordFieldType:
		typ = decls.NewObjectType(strings.Join([]string{messageType, fieldName}, "."))
	}
	if field.Repeated {
		typ = decls.NewListType(typ)
	}
	return &ref.FieldType{
		Type: typ,
	}, true
}

func (p *typeProvider) NewValue(typeName string, fields map[string]ref.Val) ref.Val {
	return types.NewErr("unknown type '%s'", typeName)
}

var _ ref.TypeProvider = new(typeProvider)
