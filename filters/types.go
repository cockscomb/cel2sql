package filters

import (
	"fmt"

	"github.com/cockscomb/cel2sql"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

const (
	ExistsEquals   = "existsEquals"
	ExistsEqualsCI = "existsEqualsCI"
	ExistsRegexp   = "existsRegexp"
	ExistsRegexpCI = "existsRegexpCI"
)

var Declarations = cel.Declarations(
	decls.NewFunction(ExistsEquals,
		decls.NewInstanceOverload("string_to_string", []*expr.Type{decls.String, decls.String}, decls.Bool),
		decls.NewInstanceOverload("string_to_list", []*expr.Type{decls.String, decls.NewListType(decls.String)}, decls.Bool),
		decls.NewInstanceOverload("list_to_string", []*expr.Type{decls.NewListType(decls.String), decls.String}, decls.Bool),
		decls.NewInstanceOverload("list_to_list", []*expr.Type{decls.NewListType(decls.String), decls.NewListType(decls.String)}, decls.Bool),
	),
	decls.NewFunction(ExistsEqualsCI,
		decls.NewInstanceOverload("string_to_string", []*expr.Type{decls.String, decls.String}, decls.Bool),
		decls.NewInstanceOverload("string_to_list", []*expr.Type{decls.String, decls.NewListType(decls.String)}, decls.Bool),
		decls.NewInstanceOverload("list_to_string", []*expr.Type{decls.NewListType(decls.String), decls.String}, decls.Bool),
		decls.NewInstanceOverload("list_to_list", []*expr.Type{decls.NewListType(decls.String), decls.NewListType(decls.String)}, decls.Bool),
	),
	decls.NewFunction(ExistsRegexp,
		decls.NewInstanceOverload("string_to_string", []*expr.Type{decls.String, decls.String}, decls.Bool),
		decls.NewInstanceOverload("string_to_list", []*expr.Type{decls.String, decls.NewListType(decls.String)}, decls.Bool),
		decls.NewInstanceOverload("list_to_string", []*expr.Type{decls.NewListType(decls.String), decls.String}, decls.Bool),
		decls.NewInstanceOverload("list_to_list", []*expr.Type{decls.NewListType(decls.String), decls.NewListType(decls.String)}, decls.Bool),
	),
	decls.NewFunction(ExistsRegexpCI,
		decls.NewInstanceOverload("string_to_string", []*expr.Type{decls.String, decls.String}, decls.Bool),
		decls.NewInstanceOverload("string_to_list", []*expr.Type{decls.String, decls.NewListType(decls.String)}, decls.Bool),
		decls.NewInstanceOverload("list_to_string", []*expr.Type{decls.NewListType(decls.String), decls.String}, decls.Bool),
		decls.NewInstanceOverload("list_to_list", []*expr.Type{decls.NewListType(decls.String), decls.NewListType(decls.String)}, decls.Bool),
	),
)

type Extension struct{}

func (ext *Extension) ImplementsFunction(fun string) bool {
	switch fun {
	case ExistsEquals, ExistsEqualsCI, ExistsRegexp, ExistsRegexpCI:
		return true
	}
	return false
}

func (ext *Extension) CallFunction(con *cel2sql.Converter, function string, target *expr.Expr, args []*expr.Expr) error {
	tgtType := con.GetType(target)
	argType := con.GetType(args[0])
	switch function {
	case ExistsEquals, ExistsEqualsCI:
		switch {
		case tgtType.GetPrimitive() == expr.Type_STRING:
			if function == ExistsEqualsCI {
				con.WriteString("COLLATE(")
			}
			if err := con.Visit(target); err != nil {
				return err
			}
			if function == ExistsEqualsCI {
				con.WriteString(", \"und:ci\")")
			}
			switch {
			case argType.GetPrimitive() == expr.Type_STRING:
				con.WriteString(" = ")
				return con.Visit(args[0])
			case cel2sql.IsListType(argType):
				con.WriteString(" in UNNEST(")
				if err := con.Visit(args[0]); err != nil {
					return err
				}
				con.WriteString(")")
				return nil
			}
		case cel2sql.IsListType(tgtType):
			switch {
			case argType.GetPrimitive() == expr.Type_STRING:
				return ext.CallFunction(con, function, args[0], []*expr.Expr{target})
			case cel2sql.IsListType(argType):
				//TODO: implement this
			}
		}
	default:
		return fmt.Errorf("unsupported filter: %v", function)
	}
	return fmt.Errorf("unsupported types: %v.(%v)", tgtType, argType)
}
