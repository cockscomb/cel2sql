package filters

import (
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
