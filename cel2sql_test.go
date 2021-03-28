package cel2sql_test

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cockscomb/cel2sql"
)

func TestConvert(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Declarations(
			decls.NewVar("name", decls.String),
			decls.NewVar("age", decls.Int),
			decls.NewVar("adult", decls.Bool),
			decls.NewVar("height", decls.Double),
			decls.NewVar("string_list", decls.NewListType(decls.String)),
			decls.NewVar("string_int_map", decls.NewMapType(decls.String, decls.Int)),
			decls.NewVar("null_var", decls.Null),
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
			want:    "STRUCT(1 AS one, 2 AS two, 3 AS three).one = 1",
			wantErr: false,
		},
		{
			name:    "map_var",
			args:    args{source: `string_int_map["one"] == 1`},
			want:    "`string_int_map`.one = 1",
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
