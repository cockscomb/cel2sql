package cel2sql_test

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cockscomb/cel2sql"
	"github.com/cockscomb/cel2sql/bq"
	"github.com/cockscomb/cel2sql/sqltypes"
	"github.com/cockscomb/cel2sql/test"
)

func TestConvert(t *testing.T) {
	env, err := cel.NewEnv(
		cel.CustomTypeProvider(bq.NewTypeProvider(map[string]bigquery.Schema{
			"trigrams":  test.NewTrigramsTableMetadata().Schema,
			"wikipedia": test.NewWikipediaTableMetadata().Schema,
		})),
		sqltypes.SQLTypeDeclarations,
		cel.Declarations(
			decls.NewVar("name", decls.String),
			decls.NewVar("age", decls.Int),
			decls.NewVar("adult", decls.Bool),
			decls.NewVar("height", decls.Double),
			decls.NewVar("string_list", decls.NewListType(decls.String)),
			decls.NewVar("string_int_map", decls.NewMapType(decls.String, decls.Int)),
			decls.NewVar("null_var", decls.Null),
			decls.NewVar("birthday", sqltypes.Date),
			decls.NewVar("fixed_time", sqltypes.Time),
			decls.NewVar("scheduled_at", sqltypes.DateTime),
			decls.NewVar("created_at", sqltypes.Timestamp),
			decls.NewVar("trigram", decls.NewObjectType("trigrams")),
			decls.NewVar("page", decls.NewObjectType("wikipedia")),
		),
	)
	require.NoError(t, err)
	type args struct {
		source string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name:    "startsWith",
			args:    args{source: `name.startsWith("a")`},
			want:    "STARTS_WITH(`name`, \"a\")",
			wantErr: false,
		},
		{
			name:    "endsWith",
			args:    args{source: `name.endsWith("z")`},
			want:    "ENDS_WITH(`name`, \"z\")",
			wantErr: false,
		},
		{
			name:    "matches",
			args:    args{source: `name.matches("a+")`},
			want:    "REGEXP_CONTAINS(`name`, \"a+\")",
			wantErr: false,
		},
		{
			name:    "contains",
			args:    args{source: `name.contains("abc")`},
			want:    "INSTR(`name`, \"abc\") != 0",
			wantErr: false,
		},
		{
			name:    "&&",
			args:    args{source: `name.startsWith("a") && name.endsWith("z")`},
			want:    "STARTS_WITH(`name`, \"a\") AND ENDS_WITH(`name`, \"z\")",
			wantErr: false,
		},
		{
			name:    "||",
			args:    args{source: `name.startsWith("a") || name.endsWith("z")`},
			want:    "STARTS_WITH(`name`, \"a\") OR ENDS_WITH(`name`, \"z\")",
			wantErr: false,
		},
		{
			name:    "()",
			args:    args{source: `age >= 10 && (name.startsWith("a") || name.endsWith("z"))`},
			want:    "`age` >= 10 AND (STARTS_WITH(`name`, \"a\") OR ENDS_WITH(`name`, \"z\"))",
			wantErr: false,
		},
		{
			name:    "IF",
			args:    args{source: `name == "a" ? "a" : "b"`},
			want:    "IF(`name` = \"a\", \"a\", \"b\")",
			wantErr: false,
		},
		{
			name:    "==",
			args:    args{source: `name == "a"`},
			want:    "`name` = \"a\"",
			wantErr: false,
		},
		{
			name:    "!=",
			args:    args{source: `age != 20`},
			want:    "`age` != 20",
			wantErr: false,
		},
		{
			name:    "IS NULL",
			args:    args{source: `null_var == null`},
			want:    "`null_var` IS NULL",
			wantErr: false,
		},
		{
			name:    "IS NOT TRUE",
			args:    args{source: `adult != true`},
			want:    "`adult` IS NOT TRUE",
			wantErr: false,
		},
		{
			name:    "<",
			args:    args{source: `age < 20`},
			want:    "`age` < 20",
			wantErr: false,
		},
		{
			name:    ">=",
			args:    args{source: `height >= 1.6180339887`},
			want:    "`height` >= 1.6180339887",
			wantErr: false,
		},
		{
			name:    "NOT",
			args:    args{source: `!adult`},
			want:    "NOT `adult`",
			wantErr: false,
		},
		{
			name:    "-",
			args:    args{source: `-1`},
			want:    "-1",
			wantErr: false,
		},
		{
			name:    "list",
			args:    args{source: `[1, 2, 3][0] == 1`},
			want:    "[1, 2, 3][OFFSET(0)] = 1",
			wantErr: false,
		},
		{
			name:    "list_var",
			args:    args{source: `string_list[0] == "a"`},
			want:    "`string_list`[OFFSET(0)] = \"a\"",
			wantErr: false,
		},
		{
			name:    "map",
			args:    args{source: `{"one": 1, "two": 2, "three": 3}["one"] == 1`},
			want:    "STRUCT(1 AS one, 2 AS two, 3 AS three).`one` = 1",
			wantErr: false,
		},
		{
			name:    "map_var",
			args:    args{source: `string_int_map["one"] == 1`},
			want:    "`string_int_map`.`one` = 1",
			wantErr: false,
		},
		{
			name:    "invalidFieldType",
			args:    args{source: `{1: 1}[1]`},
			want:    "",
			wantErr: true,
		},
		{
			name:    "invalidFieldName",
			args:    args{source: `{"on e": 1}["on e"]`},
			want:    "",
			wantErr: true,
		},
		{
			name:    "add",
			args:    args{source: `1 + 2 == 3`},
			want:    "1 + 2 = 3",
			wantErr: false,
		},
		{
			name:    "concatString",
			args:    args{source: `"a" + "b" == "ab"`},
			want:    "\"a\" || \"b\" = \"ab\"",
			wantErr: false,
		},
		{
			name:    "concatList",
			args:    args{source: `1 in [1] + [2, 3]`},
			want:    "1 IN UNNEST([1] || [2, 3])",
			wantErr: false,
		},
		{
			name:    "date",
			args:    args{source: `birthday > date(2000, 1, 1) + 1`},
			want:    "`birthday` > DATE(2000, 1, 1) + 1",
			wantErr: false,
		},
		{
			name:    "time",
			args:    args{source: `fixed_time == time("18:00:00")`},
			want:    "`fixed_time` = TIME(\"18:00:00\")",
			wantErr: false,
		},
		{
			name:    "datetime",
			args:    args{source: `scheduled_at != datetime(date("2021-09-01"), fixed_time)`},
			want:    "`scheduled_at` != DATETIME(DATE(\"2021-09-01\"), `fixed_time`)",
			wantErr: false,
		},
		{
			name:    "timestamp",
			args:    args{source: `created_at <= timestamp(datetime("2021-09-01 18:00:00"), "Asia/Tokyo")`},
			want:    "`created_at` <= TIMESTAMP(DATETIME(\"2021-09-01 18:00:00\"), \"Asia/Tokyo\")",
			wantErr: false,
		},
		{
			name:    "duration_second",
			args:    args{source: `duration("10s")`},
			want:    "INTERVAL 10 SECOND",
			wantErr: false,
		},
		{
			name:    "duration_minute",
			args:    args{source: `duration("1h1m")`},
			want:    "INTERVAL 61 MINUTE",
			wantErr: false,
		},
		{
			name:    "duration_hour",
			args:    args{source: `duration("60m")`},
			want:    "INTERVAL 1 HOUR",
			wantErr: false,
		},
		{
			name:    "interval",
			args:    args{source: `interval(1, MONTH)`},
			want:    "INTERVAL 1 MONTH",
			wantErr: false,
		},
		{
			name:    "fieldSelect",
			args:    args{source: `page.title == "test"`},
			want:    "`page`.`title` = \"test\"",
			wantErr: false,
		},
		{
			name:    "fieldSelect_startsWith",
			args:    args{source: `page.title.startsWith("test")`},
			want:    "STARTS_WITH(`page`.`title`, \"test\")",
			wantErr: false,
		},
		{
			name:    "fieldSelect_add",
			args:    args{source: `trigram.cell[0].page_count + 1`},
			want:    "`trigram`.`cell`[OFFSET(0)].`page_count` + 1",
			wantErr: false,
		},
		{
			name:    "fieldSelect_concatString",
			args:    args{source: `trigram.cell[0].sample[0].title + "test"`},
			want:    "`trigram`.`cell`[OFFSET(0)].`sample`[OFFSET(0)].`title` || \"test\"",
			wantErr: false,
		},
		{
			name:    "fieldSelect_in",
			args:    args{source: `"test" in trigram.cell[0].value`},
			want:    "\"test\" IN UNNEST(`trigram`.`cell`[OFFSET(0)].`value`)",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, issues := env.Compile(tt.args.source)
			require.Empty(t, issues)

			got, err := cel2sql.Convert(ast)
			if !tt.wantErr && assert.NoError(t, err) {
				assert.Equal(t, tt.want, got)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
