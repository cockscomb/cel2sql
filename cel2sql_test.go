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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ast, issues := env.Compile(tt.args.source)
			require.Empty(t, issues)

			got, err := cel2sql.ConvertCellToSqlCondition(ast)
			if !tt.wantErr && assert.NoError(t, err) {
				assert.Equal(t, got, tt.want)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
