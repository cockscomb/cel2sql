package bq_test

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/common/types/ref"
	"github.com/stretchr/testify/assert"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"github.com/cockscomb/cel2sql/bq"
	"github.com/cockscomb/cel2sql/test"
)

func Test_typeProvider_FindType(t *testing.T) {
	typeProvider := bq.NewTypeProvider(map[string]bigquery.Schema{
		"trigrams":  test.NewTrigramsTableMetadata().Schema,
		"wikipedia": test.NewWikipediaTableMetadata().Schema,
	})

	type args struct {
		typeName string
	}
	tests := []struct {
		name      string
		args      args
		want      *exprpb.Type
		wantFound bool
	}{
		{
			name: "trigrams",
			args: args{typeName: "trigrams"},
			want: &exprpb.Type{
				TypeKind: &exprpb.Type_Type{
					Type: &exprpb.Type{
						TypeKind: &exprpb.Type_MessageType{
							MessageType: "trigrams",
						},
					},
				},
			},
			wantFound: true,
		},
		{
			name: "trigrams.cell",
			args: args{typeName: "trigrams.cell"},
			want: &exprpb.Type{
				TypeKind: &exprpb.Type_Type{
					Type: &exprpb.Type{
						TypeKind: &exprpb.Type_MessageType{
							MessageType: "trigrams.cell",
						},
					},
				},
			},
			wantFound: true,
		},
		{
			name: "trigrams.cell.value",
			args: args{typeName: "trigrams.cell.value"},
			want: &exprpb.Type{
				TypeKind: &exprpb.Type_Type{
					Type: &exprpb.Type{
						TypeKind: &exprpb.Type_MessageType{
							MessageType: "trigrams.cell.value",
						},
					},
				},
			},
			wantFound: true,
		},
		{
			name:      "not_exists",
			args:      args{typeName: "not_exists"},
			want:      nil,
			wantFound: false,
		},
		{
			name:      "not_exists",
			args:      args{typeName: "trigrams.cell.not_exists"},
			want:      nil,
			wantFound: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotFound := typeProvider.FindType(tt.args.typeName)
			if assert.Equal(t, tt.wantFound, gotFound) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func Test_typeProvider_FindFieldType(t *testing.T) {
	typeProvider := bq.NewTypeProvider(map[string]bigquery.Schema{
		"trigrams":  test.NewTrigramsTableMetadata().Schema,
		"wikipedia": test.NewWikipediaTableMetadata().Schema,
	})

	type args struct {
		messageType string
		fieldName   string
	}
	tests := []struct {
		name      string
		args      args
		want      *ref.FieldType
		wantFound bool
	}{
		{
			name: "wikipedia.title",
			args: args{
				messageType: "wikipedia",
				fieldName:   "title",
			},
			want: &ref.FieldType{
				Type: decls.String,
			},
			wantFound: true,
		},
		{
			name: "wikipedia.id",
			args: args{
				messageType: "wikipedia",
				fieldName:   "id",
			},
			want: &ref.FieldType{
				Type: decls.Int,
			},
			wantFound: true,
		},
		{
			name: "wikipedia.is_redirect",
			args: args{
				messageType: "wikipedia",
				fieldName:   "is_redirect",
			},
			want: &ref.FieldType{
				Type: decls.Bool,
			},
			wantFound: true,
		},
		{
			name: "trigrams.cell",
			args: args{
				messageType: "trigrams",
				fieldName:   "cell",
			},
			want: &ref.FieldType{
				Type: decls.NewListType(decls.NewObjectType("trigrams.cell")),
			},
			wantFound: true,
		},
		{
			name: "trigrams.cell",
			args: args{
				messageType: "trigrams.cell",
				fieldName:   "value",
			},
			want: &ref.FieldType{
				Type: decls.NewListType(decls.String),
			},
			wantFound: true,
		},
		{
			name: "trigrams.cell.sample",
			args: args{
				messageType: "trigrams.cell",
				fieldName:   "sample",
			},
			want: &ref.FieldType{
				Type: decls.NewListType(decls.NewObjectType("trigrams.cell.sample")),
			},
			wantFound: true,
		},
		{
			name: "trigrams.cell.sample.id",
			args: args{
				messageType: "trigrams.cell.sample",
				fieldName:   "id",
			},
			want: &ref.FieldType{
				Type: decls.String,
			},
			wantFound: true,
		},
		{
			name: "not_exists_message",
			args: args{
				messageType: "not_exists",
				fieldName:   "",
			},
			want:      nil,
			wantFound: false,
		},
		{
			name: "not_exists_field",
			args: args{
				messageType: "wikipedia",
				fieldName:   "not_exists",
			},
			want:      nil,
			wantFound: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotFound := typeProvider.FindFieldType(tt.args.messageType, tt.args.fieldName)
			if assert.Equal(t, tt.wantFound, gotFound) {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
