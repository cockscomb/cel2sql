package filters

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/cockscomb/cel2sql"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

const (
	ExistsEquals     = "existsEquals"
	ExistsEqualsCI   = "existsEqualsCI"
	ExistsStarts     = "existsStarts"
	ExistsStartsCI   = "existsStartsCI"
	ExistsEnds       = "existsEnds"
	ExistsEndsCI     = "existsEndsCI"
	ExistsContains   = "existsContains"
	ExistsContainsCI = "existsContainsCI"
	ExistsRegexp     = "existsRegexp"
	ExistsRegexpCI   = "existsRegexpCI"
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
	decls.NewFunction(ExistsStarts,
		decls.NewInstanceOverload("string_to_string", []*expr.Type{decls.String, decls.String}, decls.Bool),
		decls.NewInstanceOverload("string_to_list", []*expr.Type{decls.String, decls.NewListType(decls.String)}, decls.Bool),
		decls.NewInstanceOverload("list_to_string", []*expr.Type{decls.NewListType(decls.String), decls.String}, decls.Bool),
		decls.NewInstanceOverload("list_to_list", []*expr.Type{decls.NewListType(decls.String), decls.NewListType(decls.String)}, decls.Bool),
	),
	decls.NewFunction(ExistsStartsCI,
		decls.NewInstanceOverload("string_to_string", []*expr.Type{decls.String, decls.String}, decls.Bool),
		decls.NewInstanceOverload("string_to_list", []*expr.Type{decls.String, decls.NewListType(decls.String)}, decls.Bool),
		decls.NewInstanceOverload("list_to_string", []*expr.Type{decls.NewListType(decls.String), decls.String}, decls.Bool),
		decls.NewInstanceOverload("list_to_list", []*expr.Type{decls.NewListType(decls.String), decls.NewListType(decls.String)}, decls.Bool),
	),
	decls.NewFunction(ExistsEnds,
		decls.NewInstanceOverload("string_to_string", []*expr.Type{decls.String, decls.String}, decls.Bool),
		decls.NewInstanceOverload("string_to_list", []*expr.Type{decls.String, decls.NewListType(decls.String)}, decls.Bool),
		decls.NewInstanceOverload("list_to_string", []*expr.Type{decls.NewListType(decls.String), decls.String}, decls.Bool),
		decls.NewInstanceOverload("list_to_list", []*expr.Type{decls.NewListType(decls.String), decls.NewListType(decls.String)}, decls.Bool),
	),
	decls.NewFunction(ExistsEndsCI,
		decls.NewInstanceOverload("string_to_string", []*expr.Type{decls.String, decls.String}, decls.Bool),
		decls.NewInstanceOverload("string_to_list", []*expr.Type{decls.String, decls.NewListType(decls.String)}, decls.Bool),
		decls.NewInstanceOverload("list_to_string", []*expr.Type{decls.NewListType(decls.String), decls.String}, decls.Bool),
		decls.NewInstanceOverload("list_to_list", []*expr.Type{decls.NewListType(decls.String), decls.NewListType(decls.String)}, decls.Bool),
	),
	decls.NewFunction(ExistsContains,
		decls.NewInstanceOverload("string_to_string", []*expr.Type{decls.String, decls.String}, decls.Bool),
		decls.NewInstanceOverload("string_to_list", []*expr.Type{decls.String, decls.NewListType(decls.String)}, decls.Bool),
		decls.NewInstanceOverload("list_to_string", []*expr.Type{decls.NewListType(decls.String), decls.String}, decls.Bool),
		decls.NewInstanceOverload("list_to_list", []*expr.Type{decls.NewListType(decls.String), decls.NewListType(decls.String)}, decls.Bool),
	),
	decls.NewFunction(ExistsContainsCI,
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
	case ExistsEquals, ExistsEqualsCI, ExistsStarts, ExistsStartsCI, ExistsEnds, ExistsEndsCI, ExistsContains, ExistsContainsCI, ExistsRegexp, ExistsRegexpCI:
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
				con.WriteString(" IN UNNEST(")
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
				return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsEqualsCI, start: true, end: true, regexEscape: true})
			}
		}
	case ExistsStarts, ExistsStartsCI:
		return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsStartsCI, start: true, regexEscape: true})
	case ExistsEnds, ExistsEndsCI:
		return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsEndsCI, end: true, regexEscape: true})
	case ExistsContains, ExistsContainsCI:
		return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsContainsCI, regexEscape: true})
	case ExistsRegexp, ExistsRegexpCI:
		return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsRegexpCI, start: true, end: true})
	default:
		return fmt.Errorf("unsupported filter: %v", function)
	}
	return fmt.Errorf("unsupported types: %v.(%v)", tgtType, argType)
}

type regexpOptions struct {
	caseInsensitive bool
	start           bool
	end             bool
	regexEscape     bool
}

//REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(target, "\x00") || "\x00", r"\x00(arg1|arg2|arg3)\x00")
func (ext *Extension) callRegexp(con *cel2sql.Converter, target *expr.Expr, args []*expr.Expr, opts regexpOptions) error {
	con.WriteString("REGEXP_CONTAINS(\"\\x00\" || ")
	tgtType := con.GetType(target)
	switch {
	case tgtType.GetPrimitive() == expr.Type_STRING:
		if err := con.Visit(target); err != nil {
			return err
		}
	case cel2sql.IsListType(tgtType):
		con.WriteString("ARRAY_TO_STRING(")
		if err := con.Visit(target); err != nil {
			return err
		}
		con.WriteString(", \"\\x00\")")
	}
	con.WriteString(" || \"\\x00\", ")
	regexp, err := buildRegex(args[0], opts)
	if err != nil {
		return err
	}
	//replace con.WriteValue with this if params don't work for some reason
	//con.WriteString(fmt.Sprintf("%q", regexp))
	con.WriteValue(regexp)
	con.WriteString(")")
	return nil
}

func buildRegex(expression *expr.Expr, opts regexpOptions) (string, error) {
	builder := strings.Builder{}
	if opts.caseInsensitive {
		builder.WriteString("(?i)")
	}
	if opts.start {
		builder.WriteString("\x00")
	}
	builder.WriteString("(")

	arg, err := cel2sql.GetConstValue(expression)
	if err != nil {
		return "", err
	}
	switch value := arg.(type) {
	case string:
		builder.WriteString(joinRegexps([]string{value}, opts.regexEscape))
	case []interface{}:
		patterns := make([]string, 0, len(value))
		for _, val := range value {
			if pattern, ok := val.(string); ok {
				patterns = append(patterns, pattern)
			} else {
				return "", fmt.Errorf("wrong const value: %v", pattern)
			}
		}
		builder.WriteString(joinRegexps(patterns, opts.regexEscape))
	default:
		return "", fmt.Errorf("wrong const value: %v", value)
	}
	builder.WriteString(")")
	if opts.end {
		builder.WriteString("\x00")
	}
	return builder.String(), nil
}

func joinRegexps(patterns []string, escapeItems bool) string {
	parts := make([]string, 0, len(patterns))
	for _, p := range patterns {
		if escapeItems {
			p = regexp.QuoteMeta(p)
		} else {
			p = fmt.Sprintf("(%s)", p)
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, "|")
}
