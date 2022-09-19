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
	ExistsEquals           = "existsEquals"
	ExistsEqualsCI         = "existsEqualsCI"
	ExistsStarts           = "existsStarts"
	ExistsStartsCI         = "existsStartsCI"
	ExistsEnds             = "existsEnds"
	ExistsEndsCI           = "existsEndsCI"
	ExistsContains         = "existsContains"
	ExistsContainsCI       = "existsContainsCI"
	ExistsRegexp           = "existsRegexp"   // REGEXP_CONTAINS, not anchored.
	ExistsRegexpCI         = "existsRegexpCI" // REGEXP_CONTAINS, not anchored.
	ExistsContainsKeywords = "existsContainsKeywords"
)

var ciFuncs = map[string]struct{}{
	ExistsEqualsCI:   {},
	ExistsStartsCI:   {},
	ExistsEndsCI:     {},
	ExistsContainsCI: {},
	ExistsRegexpCI:   {},
}

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
	decls.NewFunction(ExistsContainsKeywords,
		decls.NewInstanceOverload("string_to_string", []*expr.Type{decls.String, decls.String}, decls.Bool),
		decls.NewInstanceOverload("list_to_string", []*expr.Type{decls.NewListType(decls.String), decls.String}, decls.Bool),
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
	case ExistsEquals, ExistsEqualsCI, ExistsStarts, ExistsStartsCI, ExistsEnds, ExistsEndsCI, ExistsContains, ExistsContainsCI, ExistsContainsKeywords, ExistsRegexp, ExistsRegexpCI:
		return true
	}
	return false
}

func (ext *Extension) CallFunction(con *cel2sql.Converter, function string, target *expr.Expr, args []*expr.Expr) error {
	// Optimization: exists*([x]) = exists*(x)
	if cel2sql.IsListType(con.GetType(args[0])) {
		list := args[0].ExprKind.(*expr.Expr_ListExpr).ListExpr
		if len(list.Elements) == 0 {
			con.WriteString("FALSE")
			return nil
		}
		if len(list.Elements) == 1 {
			args = []*expr.Expr{
				list.Elements[0],
			}
		}
	}
	return ext.callFunction(con, function, target, args)
}

func (ext *Extension) callFunction(con *cel2sql.Converter, function string, target *expr.Expr, args []*expr.Expr) error {
	tgtType := con.GetType(target)
	argType := con.GetType(args[0])
	switch function {
	case ExistsEquals, ExistsEqualsCI:
		switch {
		case tgtType.GetPrimitive() == expr.Type_STRING:
			if err := writeTarget(con, function, target); err != nil {
				return err
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
				return ext.callFunction(con, function, args[0], []*expr.Expr{target})
			case cel2sql.IsListType(argType):
				return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsEqualsCI, startAnchor: true, endAnchor: true, regexEscape: true})
			}
		}
	case ExistsStarts, ExistsStartsCI:
		if tgtType.GetPrimitive() == expr.Type_STRING && argType.GetPrimitive() == expr.Type_STRING {
			if err := writeSimpleCall("STARTS_WITH", con, function, target, args[0]); err != nil {
				return err
			}
			return nil
		}
		return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsStartsCI, startAnchor: true, regexEscape: true})
	case ExistsEnds, ExistsEndsCI:
		if tgtType.GetPrimitive() == expr.Type_STRING && argType.GetPrimitive() == expr.Type_STRING {
			if err := writeSimpleCall("ENDS_WITH", con, function, target, args[0]); err != nil {
				return err
			}
			return nil
		}
		return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsEndsCI, endAnchor: true, regexEscape: true})
	case ExistsContains, ExistsContainsCI:
		if tgtType.GetPrimitive() == expr.Type_STRING && argType.GetPrimitive() == expr.Type_STRING {
			if err := writeSimpleCall("0 != INSTR", con, function, target, args[0]); err != nil {
				return err
			}
			return nil
		}
		return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsContainsCI, regexEscape: true})
	case ExistsContainsKeywords:
		con.WriteString("SEARCH(")
		if err := con.Visit(target); err != nil {
			return err
		}
		con.WriteString(", ")
		if err := con.Visit(args[0]); err != nil {
			return err
		}
		con.WriteString(")")
		return nil
	case ExistsRegexp, ExistsRegexpCI:
		return ext.callRegexp(con, target, args, regexpOptions{caseInsensitive: function == ExistsRegexpCI})
	default:
		return fmt.Errorf("unsupported filter: %v", function)
	}
	return fmt.Errorf("unsupported types: %v.(%v)", tgtType, argType)
}

type regexpOptions struct {
	caseInsensitive bool
	startAnchor     bool
	endAnchor       bool
	regexEscape     bool
}

func writeTarget(con *cel2sql.Converter, function string, target *expr.Expr) error {
	if _, has := ciFuncs[function]; has {
		con.WriteString("COLLATE(")
	}
	if err := con.Visit(target); err != nil {
		return err
	}
	if _, has := ciFuncs[function]; has {
		con.WriteString(", \"und:ci\")")
	}
	return nil
}

func writeSimpleCall(sqlFunc string, con *cel2sql.Converter, function string, target, arg *expr.Expr) error {
	con.WriteString(sqlFunc + "(")
	if err := writeTarget(con, function, target); err != nil {
		return err
	}
	con.WriteString(", ")
	if err := con.Visit(arg); err != nil {
		return err
	}
	con.WriteString(")")
	return nil
}

// REGEXP_CONTAINS("\x00" || ARRAY_TO_STRING(target, "\x00") || "\x00", r"\x00(arg1|arg2|arg3)\x00")
func (ext *Extension) callRegexp(con *cel2sql.Converter, target *expr.Expr, args []*expr.Expr, opts regexpOptions) error {
	tgtType := con.GetType(target)
	useZeroes := cel2sql.IsListType(tgtType)

	con.WriteString("REGEXP_CONTAINS(")
	if useZeroes {
		con.WriteString("\"\\x00\" || ")
	}
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
	if useZeroes {
		con.WriteString(" || \"\\x00\"")
	}
	con.WriteString(", ")
	regexp, err := buildRegex(args[0], opts, useZeroes)
	if err != nil {
		return err
	}
	//replace con.WriteValue with this if params don't work for some reason
	//con.WriteString(fmt.Sprintf("%q", regexp))
	con.WriteValue(regexp)
	con.WriteString(")")
	return nil
}

func buildRegex(expression *expr.Expr, opts regexpOptions, useZeroes bool) (string, error) {
	builder := strings.Builder{}
	if opts.caseInsensitive {
		builder.WriteString("(?i)")
	}
	if opts.startAnchor {
		if useZeroes {
			builder.WriteString("\x00")
		} else {
			builder.WriteString("^")
		}
	}
	builder.WriteString("(")

	arg, err := cel2sql.GetConstValue(expression)
	if err != nil {
		return "", err
	}
	switch value := arg.(type) {
	case string:
		builder.WriteString(joinRegexps([]string{preprocessRegexp(value, useZeroes)}, opts.regexEscape))
	case []interface{}:
		patterns := make([]string, 0, len(value))
		for _, val := range value {
			if pattern, ok := val.(string); ok {
				patterns = append(patterns, preprocessRegexp(pattern, useZeroes))
			} else {
				return "", fmt.Errorf("wrong const value: %v", pattern)
			}
		}
		builder.WriteString(joinRegexps(patterns, opts.regexEscape))
	default:
		return "", fmt.Errorf("wrong const value: %v", value)
	}
	builder.WriteString(")")
	if opts.endAnchor {
		if useZeroes {
			builder.WriteString("\x00")
		} else {
			builder.WriteString("$")
		}
	}
	return builder.String(), nil
}

func preprocessRegexp(pattern string, useZeroes bool) string {
	if !useZeroes {
		return pattern
	}
	if strings.HasPrefix(pattern, "^") {
		pattern = "\x00" + pattern[1:]
	}
	if strings.HasSuffix(pattern, "$") {
		pattern = pattern[:len(pattern)-1] + "\x00"
	}
	return pattern
}

func joinRegexps(patterns []string, escapeItems bool) string {
	if len(patterns) == 1 && !escapeItems {
		return patterns[0]
	}
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
