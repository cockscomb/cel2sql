package cel2sql_test

import (
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cockscomb/cel2sql"
)

func TestConvertCellToSqlCondition(t *testing.T) {
	env, err := cel.NewEnv(
		cel.Declarations(
			decls.NewVar("name", decls.String),
			decls.NewVar("age", decls.Int),
			decls.NewVar("adult", decls.Bool),
			decls.NewVar("height", decls.Double),
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
			name: "startsWith",
			args: args{source: `name.startsWith("a")`},
			want: "STARTS_WITH(`name`, \"a\")",
			wantErr: false,
		},
		{
			name: "&&",
			args: args{source: `name.startsWith("a") && name.endsWith("z")`},
			want: "STARTS_WITH(`name`, \"a\") AND ENDS_WITH(`name`, \"z\")",
			wantErr: false,
		},
		{
			name: "||",
			args: args{source: `name.startsWith("a") || name.endsWith("z")`},
			want: "STARTS_WITH(`name`, \"a\") OR ENDS_WITH(`name`, \"z\")",
			wantErr: false,
		},
		{
			name: "()",
			args: args{source: `age >= 10 && (name.startsWith("a") || name.endsWith("z"))`},
			want: "`age` >= 10 AND (STARTS_WITH(`name`, \"a\") OR ENDS_WITH(`name`, \"z\"))",
			wantErr: false,
		},
		{
			name: "==",
			args: args{source: `name == "a"`},
			want: "`name` = \"a\"",
			wantErr: false,
		},
		{
			name: "IS NULL",
			args: args{source: `null_var == null`},
			want: "`null_var` IS NULL",
			wantErr: false,
		},
		{
			name: "!=",
			args: args{source: `adult != true`},
			want: "`adult` != TRUE",
			wantErr: false,
		},
		{
			name: "<",
			args: args{source: `age < 20`},
			want: "`age` < 20",
			wantErr: false,
		},
		{
			name: ">=",
			args: args{source: `height >= 1.6180339887`},
			want: "`height` >= 1.6180339887",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, issues := env.Compile(tt.args.source)
			require.Empty(t, issues)

			got, err := cel2sql.Convert(ast.Expr())
			if !tt.wantErr && assert.NoError(t, err) {
				assert.Equal(t, got, tt.want)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
