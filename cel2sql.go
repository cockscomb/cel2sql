package cel2sql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockscomb/cel2sql/filters"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/overloads"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Implementations based on `google/cel-go`'s unparser
// https://github.com/google/cel-go/blob/master/parser/unparser.go

func Convert(ast *cel.Ast, opts ...ConvertOption) (string, error) {
	checkedExpr, err := cel.AstToCheckedExpr(ast)
	if err != nil {
		return "", err
	}
	un := &converter{
		typeMap:      checkedExpr.TypeMap,
		valueTracker: &embedTracker{},
	}
	for _, opt := range opts {
		opt(un)
	}
	if err := un.visit(checkedExpr.Expr); err != nil {
		return "", err
	}
	return un.str.String(), nil
}

type ConvertOption func(*converter)

func WithValueTracker(tracker ValueTracker) ConvertOption {
	return func(con *converter) {
		con.valueTracker = tracker
	}
}

type ValueTracker interface {
	AddValue(val interface{}) string
}

type embedTracker struct{}

func (t *embedTracker) AddValue(val interface{}) string {
	return ValueToString(val)
}

func ValueToString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return strconv.Quote(v)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case []byte:
		return `b"` + bytesToOctets(v) + `"`
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case int64:
		return strconv.FormatInt(v, 10)
	case nil:
		return "NULL"
	case uint64:
		return strconv.FormatUint(v, 10)
	default:
		panic("unsupported type")
	}
}

type converter struct {
	str          strings.Builder
	typeMap      map[int64]*exprpb.Type
	valueTracker ValueTracker
}

func (con *converter) visit(expr *exprpb.Expr) error {
	switch expr.ExprKind.(type) {
	case *exprpb.Expr_CallExpr:
		return con.visitCall(expr)
	// TODO: Comprehensions are currently not supported.
	case *exprpb.Expr_ComprehensionExpr:
		return con.visitComprehension(expr)
	case *exprpb.Expr_ConstExpr:
		return con.visitConst(expr)
	case *exprpb.Expr_IdentExpr:
		return con.visitIdent(expr)
	case *exprpb.Expr_ListExpr:
		return con.visitList(expr)
	case *exprpb.Expr_SelectExpr:
		return con.visitSelect(expr)
	case *exprpb.Expr_StructExpr:
		return con.visitStruct(expr)
	}
	return fmt.Errorf("unsupported expr: %v", expr)
}

func (con *converter) visitCall(expr *exprpb.Expr) error {
	c := expr.GetCallExpr()
	fun := c.GetFunction()
	switch fun {
	// ternary operator
	case operators.Conditional:
		return con.visitCallConditional(expr)
	// index operator
	case operators.Index:
		return con.visitCallIndex(expr)
	// unary operators
	case operators.LogicalNot, operators.Negate:
		return con.visitCallUnary(expr)
	// binary operators
	case operators.Add,
		operators.Divide,
		operators.Equals,
		operators.Greater,
		operators.GreaterEquals,
		operators.In,
		operators.Less,
		operators.LessEquals,
		operators.LogicalAnd,
		operators.LogicalOr,
		operators.Multiply,
		operators.NotEquals,
		operators.OldIn,
		operators.Subtract:
		return con.visitCallBinary(expr)
	// standard function calls.
	default:
		return con.visitCallFunc(expr)
	}
}

var standardSQLBinaryOperators = map[string]string{
	operators.LogicalAnd: "AND",
	operators.LogicalOr:  "OR",
	operators.Equals:     "=",
	operators.In:         "IN",
}

func (con *converter) visitCallBinary(expr *exprpb.Expr) error {
	c := expr.GetCallExpr()
	fun := c.GetFunction()
	args := c.GetArgs()
	lhs := args[0]
	// add parens if the current operator is lower precedence than the lhs expr operator.
	lhsParen := isComplexOperatorWithRespectTo(fun, lhs)
	rhs := args[1]
	// add parens if the current operator is lower precedence than the rhs expr operator,
	// or the same precedence and the operator is left recursive.
	rhsParen := isComplexOperatorWithRespectTo(fun, rhs)
	lhsType := con.getType(lhs)
	rhsType := con.getType(rhs)
	if (isTimestampRelatedType(lhsType) && isDurationRelatedType(rhsType)) ||
		(isTimestampRelatedType(rhsType) && isDurationRelatedType(lhsType)) {
		return con.callTimestampOperation(fun, lhs, rhs)
	}
	if !rhsParen && isLeftRecursive(fun) {
		rhsParen = isSamePrecedence(fun, rhs)
	}
	if err := con.visitMaybeNested(lhs, lhsParen); err != nil {
		return err
	}
	var operator string
	if fun == operators.Add && (lhsType.GetPrimitive() == exprpb.Type_STRING && rhsType.GetPrimitive() == exprpb.Type_STRING) {
		operator = "||"
	} else if fun == operators.Add && (rhsType.GetPrimitive() == exprpb.Type_BYTES && lhsType.GetPrimitive() == exprpb.Type_BYTES) {
		operator = "||"
	} else if fun == operators.Add && (isListType(lhsType) && isListType(rhsType)) {
		operator = "||"
	} else if fun == operators.Equals && (isNullLiteral(rhs) || isBoolLiteral(rhs)) {
		operator = "IS"
	} else if fun == operators.NotEquals && (isNullLiteral(rhs) || isBoolLiteral(rhs)) {
		operator = "IS NOT"
	} else if op, found := standardSQLBinaryOperators[fun]; found {
		operator = op
	} else if op, found := operators.FindReverseBinaryOperator(fun); found {
		operator = op
	} else {
		return fmt.Errorf("cannot unmangle operator: %s", fun)
	}
	con.str.WriteString(" ")
	con.str.WriteString(operator)
	con.str.WriteString(" ")
	if fun == operators.In && isListType(rhsType) {
		con.str.WriteString("UNNEST(")
	}
	if err := con.visitMaybeNested(rhs, rhsParen); err != nil {
		return err
	}
	if fun == operators.In && isListType(rhsType) {
		con.str.WriteString(")")
	}
	return nil
}

func isTimestampRelatedType(typ *exprpb.Type) bool {
	abstractType := typ.GetAbstractType()
	if abstractType != nil {
		name := abstractType.GetName()
		return name == "DATE" || name == "TIME" || name == "DATETIME"
	}
	return typ.GetWellKnown() == exprpb.Type_TIMESTAMP
}

func isDateType(typ *exprpb.Type) bool {
	return typ.GetAbstractType() != nil && typ.GetAbstractType().GetName() == "DATE"
}

func isTimeType(typ *exprpb.Type) bool {
	return typ.GetAbstractType() != nil && typ.GetAbstractType().GetName() == "TIME"
}

func isDateTimeType(typ *exprpb.Type) bool {
	return typ.GetAbstractType() != nil && typ.GetAbstractType().GetName() == "DATETIME"
}

func isTimestampType(typ *exprpb.Type) bool {
	return typ.GetWellKnown() == exprpb.Type_TIMESTAMP
}

func isDurationRelatedType(typ *exprpb.Type) bool {
	abstractType := typ.GetAbstractType()
	if abstractType != nil {
		name := abstractType.GetName()
		return name == "INTERVAL"
	}
	return typ.GetWellKnown() == exprpb.Type_DURATION
}

func (con *converter) callTimestampOperation(fun string, lhs *exprpb.Expr, rhs *exprpb.Expr) error {
	lhsParen := isComplexOperatorWithRespectTo(fun, lhs)
	rhsParen := isComplexOperatorWithRespectTo(fun, rhs)
	lhsType := con.getType(lhs)
	rhsType := con.getType(rhs)

	var timestampType *exprpb.Type
	var timestamp, duration *exprpb.Expr
	var timestampParen, durationParen bool
	switch {
	case isTimestampRelatedType(lhsType):
		timestampType = lhsType
		timestamp, duration = lhs, rhs
		timestampParen, durationParen = lhsParen, rhsParen
	case isTimestampRelatedType(rhsType):
		timestampType = rhsType
		timestamp, duration = rhs, lhs
		timestampParen, durationParen = rhsParen, lhsParen
	default:
		panic("lhs or rhs must be timestamp related type")
	}

	var sqlFun string
	switch fun {
	case operators.Add:
		switch {
		case isTimeType(timestampType):
			sqlFun = "TIME_ADD"
		case isDateType(timestampType):
			sqlFun = "DATE_ADD"
		case isDateTimeType(timestampType):
			sqlFun = "DATETIME_ADD"
		default:
			sqlFun = "TIMESTAMP_ADD"
		}
	case operators.Subtract:
		switch {
		case isTimeType(timestampType):
			sqlFun = "TIME_SUB"
		case isDateType(timestampType):
			sqlFun = "DATE_SUB"
		case isDateTimeType(timestampType):
			sqlFun = "DATETIME_SUB"
		default:
			sqlFun = "TIMESTAMP_SUB"
		}
	default:
		return fmt.Errorf("unsupported operation (%s)", fun)
	}
	con.str.WriteString(sqlFun)
	con.str.WriteString("(")
	if err := con.visitMaybeNested(timestamp, timestampParen); err != nil {
		return err
	}
	con.str.WriteString(", ")
	if err := con.visitMaybeNested(duration, durationParen); err != nil {
		return err
	}
	con.str.WriteString(")")
	return nil
}

func (con *converter) visitCallConditional(expr *exprpb.Expr) error {
	c := expr.GetCallExpr()
	args := c.GetArgs()
	con.str.WriteString("IF(")
	if err := con.visit(args[0]); err != nil {
		return err
	}
	con.str.WriteString(", ")
	if err := con.visit(args[1]); err != nil {
		return err
	}
	con.str.WriteString(", ")
	if err := con.visit(args[2]); err != nil {
		return nil
	}
	con.str.WriteString(")")
	return nil
}

var standardSQLFunctions = map[string]string{
	operators.Modulo:     "MOD",
	overloads.StartsWith: "STARTS_WITH",
	overloads.EndsWith:   "ENDS_WITH",
	overloads.Matches:    "REGEXP_CONTAINS",
	"lowerAscii":         "LOWER",
}

func (con *converter) callContains(target *exprpb.Expr, args []*exprpb.Expr) error {
	con.str.WriteString("INSTR(")
	if target != nil {
		nested := isBinaryOrTernaryOperator(target)
		err := con.visitMaybeNested(target, nested)
		if err != nil {
			return err
		}
		con.str.WriteString(", ")
	}
	for i, arg := range args {
		err := con.visit(arg)
		if err != nil {
			return err
		}
		if i < len(args)-1 {
			con.str.WriteString(", ")
		}
	}
	con.str.WriteString(") != 0")
	return nil
}

func (con *converter) callDuration(target *exprpb.Expr, args []*exprpb.Expr) error {
	if len(args) != 1 {
		return fmt.Errorf("arguments must be single")
	}
	arg := args[0]
	var durationString string
	switch arg.ExprKind.(type) {
	case *exprpb.Expr_ConstExpr:
		switch arg.GetConstExpr().ConstantKind.(type) {
		case *exprpb.Constant_StringValue:
			durationString = arg.GetConstExpr().GetStringValue()
		default:
			return fmt.Errorf("unsupported constant kind %t", arg.GetConstExpr().ConstantKind)
		}
	default:
		return fmt.Errorf("unsupported kind %t", arg.ExprKind)
	}
	d, err := time.ParseDuration(durationString)
	if err != nil {
		return err
	}
	con.str.WriteString("INTERVAL ")
	switch d {
	case d.Round(time.Hour):
		con.str.WriteString(strconv.FormatFloat(d.Hours(), 'f', 0, 64))
		con.str.WriteString(" HOUR")
	case d.Round(time.Minute):
		con.str.WriteString(strconv.FormatFloat(d.Minutes(), 'f', 0, 64))
		con.str.WriteString(" MINUTE")
	case d.Round(time.Second):
		con.str.WriteString(strconv.FormatFloat(d.Seconds(), 'f', 0, 64))
		con.str.WriteString(" SECOND")
	case d.Round(time.Millisecond):
		con.str.WriteString(strconv.FormatInt(d.Milliseconds(), 10))
		con.str.WriteString(" MILLISECOND")
	default:
		con.str.WriteString(strconv.FormatInt(d.Truncate(time.Microsecond).Microseconds(), 10))
		con.str.WriteString(" MICROSECOND")
	}
	return nil
}

func (con *converter) callInterval(target *exprpb.Expr, args []*exprpb.Expr) error {
	con.str.WriteString("INTERVAL ")
	if err := con.visit(args[0]); err != nil {
		return err
	}
	con.str.WriteString(" ")
	datePart := args[1]
	con.str.WriteString(datePart.GetIdentExpr().GetName())
	return nil
}

func (con *converter) callExtractFromTimestamp(function string, target *exprpb.Expr, args []*exprpb.Expr) error {
	con.str.WriteString("EXTRACT(")
	switch function {
	case overloads.TimeGetFullYear:
		con.str.WriteString("YEAR")
	case overloads.TimeGetMonth:
		con.str.WriteString("MONTH")
	case overloads.TimeGetDate:
		con.str.WriteString("DAY")
	case overloads.TimeGetHours:
		con.str.WriteString("HOUR")
	case overloads.TimeGetMinutes:
		con.str.WriteString("MINUTE")
	case overloads.TimeGetSeconds:
		con.str.WriteString("SECOND")
	case overloads.TimeGetMilliseconds:
		con.str.WriteString("MILLISECOND")
	case overloads.TimeGetDayOfYear:
		con.str.WriteString("DAYOFYEAR")
	case overloads.TimeGetDayOfMonth:
		con.str.WriteString("DAY")
	case overloads.TimeGetDayOfWeek:
		con.str.WriteString("DAYOFWEEK")
	}
	con.str.WriteString(" FROM ")
	if err := con.visit(target); err != nil {
		return err
	}
	if isTimestampType(con.getType(target)) && len(args) == 1 {
		con.str.WriteString(" AT ")
		if err := con.visit(args[0]); err != nil {
			return err
		}
	}
	con.str.WriteString(")")
	if function == overloads.TimeGetMonth || function == overloads.TimeGetDayOfYear || function == overloads.TimeGetDayOfMonth || function == overloads.TimeGetDayOfWeek {
		con.str.WriteString(" - 1")
	}
	return nil
}

func (con *converter) callCasting(function string, target *exprpb.Expr, args []*exprpb.Expr) error {
	arg := args[0]
	if function == overloads.TypeConvertInt && isTimestampType(con.getType(arg)) {
		con.str.WriteString("UNIX_SECONDS(")
		if err := con.visit(arg); err != nil {
			return err
		}
		con.str.WriteString(")")
		return nil
	}
	con.str.WriteString("CAST(")
	if err := con.visit(arg); err != nil {
		return err
	}
	con.str.WriteString(" AS ")
	switch function {
	case overloads.TypeConvertBool:
		con.str.WriteString("BOOL")
	case overloads.TypeConvertBytes:
		con.str.WriteString("BYTES")
	case overloads.TypeConvertDouble:
		con.str.WriteString("FLOAT64")
	case overloads.TypeConvertInt:
		con.str.WriteString("INT64")
	case overloads.TypeConvertString:
		con.str.WriteString("STRING")
	case overloads.TypeConvertUint:
		con.str.WriteString("INT64")
	}
	con.str.WriteString(")")
	return nil
}

// TODO:
func (con *converter) callFilter(function string, target *exprpb.Expr, args []*exprpb.Expr) error {
	tgtType := con.getType(target)
	argType := con.getType(args[0])
	switch function {
	case filters.ExistsEquals, filters.ExistsEqualsCI:
		switch {
		case tgtType.GetPrimitive() == exprpb.Type_STRING:
			if function == filters.ExistsEqualsCI {
				con.str.WriteString("COLLATE(")
			}
			if err := con.visit(target); err != nil {
				return err
			}
			if function == filters.ExistsEqualsCI {
				con.str.WriteString(", \"und:ci\")")
			}
			switch {
			case argType.GetPrimitive() == exprpb.Type_STRING:
				con.str.WriteString(" = ")
				return con.visit(args[0])
			case isListType(argType):
				con.str.WriteString(" in UNNEST(")
				if err := con.visit(args[0]); err != nil {
					return err
				}
				con.str.WriteString(")")
				return nil
			}
		case isListType(tgtType):
			switch {
			case argType.GetPrimitive() == exprpb.Type_STRING:
				return con.callFilter(function, args[0], []*exprpb.Expr{target})
			case isListType(argType):
				//TODO: implement this
			}
		}
	default:
		return fmt.Errorf("unsupported filter: %v", function)
	}
	return fmt.Errorf("unsupported types: %v.(%v)", tgtType, argType)
}

func (con *converter) visitCallFunc(expr *exprpb.Expr) error {
	c := expr.GetCallExpr()
	fun := c.GetFunction()
	target := c.GetTarget()
	args := c.GetArgs()
	switch fun {
	case overloads.Contains:
		return con.callContains(target, args)
	case overloads.TypeConvertDuration:
		return con.callDuration(target, args)
	case "interval":
		return con.callInterval(target, args)
	case overloads.TimeGetFullYear,
		overloads.TimeGetMonth,
		overloads.TimeGetDate,
		overloads.TimeGetHours,
		overloads.TimeGetMinutes,
		overloads.TimeGetSeconds,
		overloads.TimeGetMilliseconds,
		overloads.TimeGetDayOfYear,
		overloads.TimeGetDayOfMonth,
		overloads.TimeGetDayOfWeek:
		return con.callExtractFromTimestamp(fun, target, args)
	case overloads.TypeConvertBool,
		overloads.TypeConvertBytes,
		overloads.TypeConvertDouble,
		overloads.TypeConvertInt,
		overloads.TypeConvertString,
		overloads.TypeConvertUint:
		return con.callCasting(fun, target, args)
	case filters.ExistsEquals,
		filters.ExistsEqualsCI,
		filters.ExistsRegexp,
		filters.ExistsRegexpCI:
		return con.callFilter(fun, target, args)
	}
	sqlFun, ok := standardSQLFunctions[fun]
	if !ok {
		if fun == overloads.Size {
			argType := con.getType(args[0])
			switch {
			case argType.GetPrimitive() == exprpb.Type_STRING:
				sqlFun = "LENGTH"
			case argType.GetPrimitive() == exprpb.Type_BYTES:
				sqlFun = "LENGTH"
			case isListType(argType):
				sqlFun = "ARRAY_LENGTH"
			default:
				return fmt.Errorf("unsupported type: %v", argType)
			}
		} else {
			sqlFun = strings.ToUpper(fun)
		}
	}
	con.str.WriteString(sqlFun)
	con.str.WriteString("(")
	if target != nil {
		nested := isBinaryOrTernaryOperator(target)
		err := con.visitMaybeNested(target, nested)
		if err != nil {
			return err
		}
		if len(args) > 0 {
			con.str.WriteString(", ")
		}
	}
	for i, arg := range args {
		err := con.visit(arg)
		if err != nil {
			return err
		}
		if i < len(args)-1 {
			con.str.WriteString(", ")
		}
	}
	con.str.WriteString(")")
	return nil
}

func (con *converter) visitCallIndex(expr *exprpb.Expr) error {
	if isMapType(con.getType(expr.GetCallExpr().GetArgs()[0])) {
		return con.visitCallMapIndex(expr)
	}
	return con.visitCallListIndex(expr)
}

func (con *converter) visitCallMapIndex(expr *exprpb.Expr) error {
	c := expr.GetCallExpr()
	args := c.GetArgs()
	m := args[0]
	nested := isBinaryOrTernaryOperator(m)
	if err := con.visitMaybeNested(m, nested); err != nil {
		return err
	}
	fieldName, err := extractFieldName(args[1])
	if err != nil {
		return err
	}
	con.str.WriteString(".`")
	con.str.WriteString(fieldName)
	con.str.WriteString("`")
	return nil
}

func (con *converter) visitCallListIndex(expr *exprpb.Expr) error {
	c := expr.GetCallExpr()
	args := c.GetArgs()
	l := args[0]
	nested := isBinaryOrTernaryOperator(l)
	if err := con.visitMaybeNested(l, nested); err != nil {
		return err
	}
	con.str.WriteString("[OFFSET(")
	index := args[1]
	if err := con.visit(index); err != nil {
		return err
	}
	con.str.WriteString(")]")
	return nil
}

var standardSQLUnaryOperators = map[string]string{
	operators.LogicalNot: "NOT ",
}

func (con *converter) visitCallUnary(expr *exprpb.Expr) error {
	c := expr.GetCallExpr()
	fun := c.GetFunction()
	args := c.GetArgs()
	var operator string
	if op, found := standardSQLUnaryOperators[fun]; found {
		operator = op
	} else if op, found := operators.FindReverse(fun); found {
		operator = op
	} else {
		return fmt.Errorf("cannot unmangle operator: %s", fun)
	}
	con.str.WriteString(operator)
	nested := isComplexOperator(args[0])
	return con.visitMaybeNested(args[0], nested)
}

func (con *converter) visitComprehension(expr *exprpb.Expr) error {
	// TODO: introduce a macro expansion map between the top-level comprehension id and the
	// function call that the macro replaces.

	// Comprehenions like:
	//   array.exists(x, expr(x))
	// are transformed into
	// 	 EXISTS (SELECT * FROM UNNEST(array) AS x WHERE expr_sql(x)))
	// where expr_sql() is the SQL equivalent of the expr() CEL expression
	// TODO: Test more extensively and add more checks.
	e := expr.GetComprehensionExpr()
	con.str.WriteString("EXISTS (SELECT * FROM UNNEST(")
	con.visit(e.GetIterRange())
	con.str.WriteString(fmt.Sprintf(") AS %s WHERE ", e.GetIterVar()))
	con.visit(e.GetLoopStep().GetCallExpr().GetArgs()[1])
	con.str.WriteString(")")
	return nil
}

func getConstValue(expr *exprpb.Expr) (interface{}, error) {
	c := expr.GetConstExpr()
	switch c.ConstantKind.(type) {
	case *exprpb.Constant_BoolValue:
		return c.GetBoolValue(), nil
	case *exprpb.Constant_BytesValue:
		return c.GetBytesValue(), nil
	case *exprpb.Constant_DoubleValue:
		return c.GetDoubleValue(), nil
	case *exprpb.Constant_Int64Value:
		return c.GetInt64Value(), nil
	case *exprpb.Constant_NullValue:
		return nil, nil
	case *exprpb.Constant_StringValue:
		return c.GetStringValue(), nil
	case *exprpb.Constant_Uint64Value:
		return c.GetUint64Value(), nil
	default:
		return "", fmt.Errorf("unimplemented : %v", expr)
	}
}

func (con *converter) visitConst(expr *exprpb.Expr) error {
	value, err := getConstValue(expr)
	if err != nil {
		return err
	}
	con.str.WriteString(con.valueTracker.AddValue(value))
	return nil
}

func (con *converter) visitIdent(expr *exprpb.Expr) error {
	con.str.WriteString("`")
	con.str.WriteString(expr.GetIdentExpr().GetName())
	con.str.WriteString("`")
	return nil
}

func (con *converter) visitList(expr *exprpb.Expr) error {
	// TODO: implement list support
	l := expr.GetListExpr()
	elems := l.GetElements()
	con.str.WriteString("[")
	for i, elem := range elems {
		err := con.visit(elem)
		if err != nil {
			return err
		}
		if i < len(elems)-1 {
			con.str.WriteString(", ")
		}
	}
	con.str.WriteString("]")
	return nil
}

func (con *converter) visitSelect(expr *exprpb.Expr) error {
	sel := expr.GetSelectExpr()
	// handle the case when the select expression was generated by the has() macro.
	if sel.GetTestOnly() {
		con.str.WriteString("has(")
	}
	nested := !sel.GetTestOnly() && isBinaryOrTernaryOperator(sel.GetOperand())
	err := con.visitMaybeNested(sel.GetOperand(), nested)
	if err != nil {
		return err
	}
	con.str.WriteString(".`")
	con.str.WriteString(sel.GetField())
	con.str.WriteString("`")
	if sel.GetTestOnly() {
		con.str.WriteString(")")
	}
	return nil
}

func (con *converter) visitStruct(expr *exprpb.Expr) error {
	s := expr.GetStructExpr()
	// If the message name is non-empty, then this should be treated as message construction.
	if s.GetMessageName() != "" {
		return con.visitStructMsg(expr)
	}
	// Otherwise, build a map.
	return con.visitStructMap(expr)
}

func (con *converter) visitStructMsg(expr *exprpb.Expr) error {
	m := expr.GetStructExpr()
	entries := m.GetEntries()
	con.str.WriteString(m.GetMessageName())
	con.str.WriteString("{")
	for i, entry := range entries {
		f := entry.GetFieldKey()
		con.str.WriteString(f)
		con.str.WriteString(": ")
		v := entry.GetValue()
		err := con.visit(v)
		if err != nil {
			return err
		}
		if i < len(entries)-1 {
			con.str.WriteString(", ")
		}
	}
	con.str.WriteString("}")
	return nil
}

func (con *converter) visitStructMap(expr *exprpb.Expr) error {
	m := expr.GetStructExpr()
	entries := m.GetEntries()
	con.str.WriteString("STRUCT(")
	for i, entry := range entries {
		v := entry.GetValue()
		if err := con.visit(v); err != nil {
			return err
		}
		con.str.WriteString(" AS ")
		fieldName, err := extractFieldName(entry.GetMapKey())
		if err != nil {
			return err
		}
		con.str.WriteString(fieldName)
		if i < len(entries)-1 {
			con.str.WriteString(", ")
		}
	}
	con.str.WriteString(")")
	return nil
}

func (con *converter) visitMaybeNested(expr *exprpb.Expr, nested bool) error {
	if nested {
		con.str.WriteString("(")
	}
	err := con.visit(expr)
	if err != nil {
		return err
	}
	if nested {
		con.str.WriteString(")")
	}
	return nil
}

func (con *converter) getType(node *exprpb.Expr) *exprpb.Type {
	return con.typeMap[node.GetId()]
}

func isMapType(typ *exprpb.Type) bool {
	_, ok := typ.TypeKind.(*exprpb.Type_MapType_)
	return ok
}

func isListType(typ *exprpb.Type) bool {
	_, ok := typ.TypeKind.(*exprpb.Type_ListType_)
	return ok
}

// isLeftRecursive indicates whether the parser resolves the call in a left-recursive manner as
// this can have an effect of how parentheses affect the order of operations in the AST.
func isLeftRecursive(op string) bool {
	return op != operators.LogicalAnd && op != operators.LogicalOr
}

// isSamePrecedence indicates whether the precedence of the input operator is the same as the
// precedence of the (possible) operation represented in the input Expr.
//
// If the expr is not a Call, the result is false.
func isSamePrecedence(op string, expr *exprpb.Expr) bool {
	if expr.GetCallExpr() == nil {
		return false
	}
	c := expr.GetCallExpr()
	other := c.GetFunction()
	return operators.Precedence(op) == operators.Precedence(other)
}

// isLowerPrecedence indicates whether the precedence of the input operator is lower precedence
// than the (possible) operation represented in the input Expr.
//
// If the expr is not a Call, the result is false.
func isLowerPrecedence(op string, expr *exprpb.Expr) bool {
	if expr.GetCallExpr() == nil {
		return false
	}
	c := expr.GetCallExpr()
	other := c.GetFunction()
	return operators.Precedence(op) < operators.Precedence(other)
}

// Indicates whether the expr is a complex operator, i.e., a call expression
// with 2 or more arguments.
func isComplexOperator(expr *exprpb.Expr) bool {
	if expr.GetCallExpr() != nil && len(expr.GetCallExpr().GetArgs()) >= 2 {
		return true
	}
	return false
}

// Indicates whether it is a complex operation compared to another.
// expr is *not* considered complex if it is not a call expression or has
// less than two arguments, or if it has a higher precedence than op.
func isComplexOperatorWithRespectTo(op string, expr *exprpb.Expr) bool {
	if expr.GetCallExpr() == nil || len(expr.GetCallExpr().GetArgs()) < 2 {
		return false
	}
	return isLowerPrecedence(op, expr)
}

// Indicate whether this is a binary or ternary operator.
func isBinaryOrTernaryOperator(expr *exprpb.Expr) bool {
	if expr.GetCallExpr() == nil || len(expr.GetCallExpr().GetArgs()) < 2 {
		return false
	}
	_, isBinaryOp := operators.FindReverseBinaryOperator(expr.GetCallExpr().GetFunction())
	return isBinaryOp || isSamePrecedence(operators.Conditional, expr)
}

func isNullLiteral(node *exprpb.Expr) bool {
	_, isConst := node.ExprKind.(*exprpb.Expr_ConstExpr)
	if !isConst {
		return false
	}
	_, isNull := node.GetConstExpr().ConstantKind.(*exprpb.Constant_NullValue)
	return isNull
}

func isBoolLiteral(node *exprpb.Expr) bool {
	_, isConst := node.ExprKind.(*exprpb.Expr_ConstExpr)
	if !isConst {
		return false
	}
	_, isBool := node.GetConstExpr().ConstantKind.(*exprpb.Constant_BoolValue)
	return isBool
}

func isStringLiteral(node *exprpb.Expr) bool {
	_, isConst := node.ExprKind.(*exprpb.Expr_ConstExpr)
	if !isConst {
		return false
	}
	_, isString := node.GetConstExpr().ConstantKind.(*exprpb.Constant_StringValue)
	return isString
}

// bytesToOctets converts byte sequences to a string using a three digit octal encoded value
// per byte.
func bytesToOctets(byteVal []byte) string {
	var b strings.Builder
	for _, c := range byteVal {
		_, _ = fmt.Fprintf(&b, "\\%03o", c)
	}
	return b.String()
}

var fieldNameRegexp = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,127}$`)

func validateFieldName(name string) error {
	if !fieldNameRegexp.MatchString(name) {
		return fmt.Errorf("invalid field name \"%s\"", name)
	}
	return nil
}

func extractFieldName(node *exprpb.Expr) (string, error) {
	if !isStringLiteral(node) {
		return "", fmt.Errorf("unsupported type: %v", node)
	}
	fieldName := node.GetConstExpr().GetStringValue()
	if err := validateFieldName(fieldName); err != nil {
		return "", err
	}
	return fieldName, nil
}
