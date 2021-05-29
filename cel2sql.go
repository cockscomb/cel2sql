package cel2sql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/operators"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Implementations based on `google/cel-go`'s unparser
// https://github.com/google/cel-go/blob/master/parser/unparser.go

func Convert(ast *cel.Ast) (string, error) {
	checkedExpr, err := cel.AstToCheckedExpr(ast)
	if err != nil {
		return "", err
	}
	un := &converter{
		typeMap: checkedExpr.TypeMap,
	}
	if err := un.visit(checkedExpr.Expr); err != nil {
		return "", err
	}
	return un.str.String(), nil
}

type converter struct {
	str     strings.Builder
	typeMap map[int64]*exprpb.Type
}

func (con *converter) visit(expr *exprpb.Expr) error {
	switch expr.ExprKind.(type) {
	case *exprpb.Expr_CallExpr:
		return con.visitCall(expr)
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
		operators.Modulo,
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
	if !rhsParen && isLeftRecursive(fun) {
		rhsParen = isSamePrecedence(fun, rhs)
	}
	if err := con.visitMaybeNested(lhs, lhsParen); err != nil {
		return err
	}
	var operator string
	lhsType := con.getType(lhs)
	rhsType := con.getType(rhs)
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
	"startsWith": "STARTS_WITH",
	"endsWith":   "ENDS_WITH",
	"matches":    "REGEXP_CONTAINS",
	"contains":   "INSTR",

	"date":      "DATE",
	"time":      "TIME",
	"datetime":  "DATETIME",
	"timestamp": "TIMESTAMP",
}

func (con *converter) visitCallFunc(expr *exprpb.Expr) error {
	c := expr.GetCallExpr()
	fun := c.GetFunction()
	args := c.GetArgs()
	sqlFun, ok := standardSQLFunctions[fun]
	if !ok {
		return fmt.Errorf("unsupported function: %s", fun)
	}
	con.str.WriteString(sqlFun)
	con.str.WriteString("(")
	if c.GetTarget() != nil {
		nested := isBinaryOrTernaryOperator(c.GetTarget())
		err := con.visitMaybeNested(c.GetTarget(), nested)
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
	con.str.WriteString(")")
	if fun == "contains" {
		con.str.WriteString(" != 0")
	}
	return nil
}

func (con *converter) visitCallIndex(expr *exprpb.Expr) error {
	if isMapType(con.getType(expr.GetCallExpr().GetArgs()[0])) {
		return con.visitCallMapIndex(expr)
	} else {
		return con.visitCallListIndex(expr)
	}
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
	c := expr.GetComprehensionExpr()

	loopStep := c.GetLoopStep().GetCallExpr()
	if loopStep == nil {
		return fmt.Errorf("unsupported macro")
	}
	loopFunction := loopStep.GetFunction()
	loopArgs := loopStep.GetArgs()

	con.str.WriteString("SELECT ")
	switch loopFunction {
	case operators.LogicalAnd:
		con.str.WriteString("COUNT(*) = 0")
	case operators.LogicalOr:
		con.str.WriteString("COUNT(*) > 0")
	case operators.Add:
		// map
		con.str.WriteString("ARRAY_AGG(")
		if err := con.visit(loopArgs[1].GetListExpr().GetElements()[0]); err != nil {
			return err
		}
		con.str.WriteString(")")
	case operators.Conditional:
		switch loopArgs[1].GetCallExpr().GetArgs()[1].ExprKind.(type) {
		case *exprpb.Expr_ConstExpr:
			// exists_one
			con.str.WriteString("COUNT(*) = 1")
		case *exprpb.Expr_ListExpr:
			// filter
			con.str.WriteString("ARRAY_AGG(")
			if err := con.visit(loopArgs[1].GetCallExpr().GetArgs()[1].GetListExpr().GetElements()[0]); err != nil {
				return err
			}
			con.str.WriteString(")")
		default:
			return fmt.Errorf("unsupported macro")
		}
	default:
		return fmt.Errorf("unsupported macro")
	}
	con.str.WriteString(" FROM UNNEST(")
	if err := con.visit(c.GetIterRange()); err != nil {
		return err
	}
	con.str.WriteString(") AS `")
	con.str.WriteString(c.GetIterVar())
	con.str.WriteString("`")
	switch loopFunction {
	case operators.LogicalAnd, operators.LogicalOr, operators.Conditional:
		con.str.WriteString(" WHERE ")
	}
	switch loopFunction {
	case operators.LogicalAnd:
		con.str.WriteString("NOT (")
		if err := con.visit(loopArgs[1]); err != nil {
			return err
		}
		con.str.WriteString(")")
	case operators.LogicalOr:
		if err := con.visit(loopArgs[1]); err != nil {
			return err
		}
	case operators.Conditional:
		if err := con.visit(loopArgs[0]); err != nil {
			return err
		}
	}
	return nil
}

func (con *converter) visitConst(expr *exprpb.Expr) error {
	c := expr.GetConstExpr()
	switch c.ConstantKind.(type) {
	case *exprpb.Constant_BoolValue:
		if c.GetBoolValue() {
			con.str.WriteString("TRUE")
		} else {
			con.str.WriteString("FALSE")
		}
	case *exprpb.Constant_BytesValue:
		b := c.GetBytesValue()
		con.str.WriteString(`b"`)
		con.str.WriteString(bytesToOctets(b))
		con.str.WriteString(`"`)
	case *exprpb.Constant_DoubleValue:
		d := strconv.FormatFloat(c.GetDoubleValue(), 'g', -1, 64)
		con.str.WriteString(d)
	case *exprpb.Constant_Int64Value:
		i := strconv.FormatInt(c.GetInt64Value(), 10)
		con.str.WriteString(i)
	case *exprpb.Constant_NullValue:
		con.str.WriteString("NULL")
	case *exprpb.Constant_StringValue:
		con.str.WriteString(strconv.Quote(c.GetStringValue()))
	case *exprpb.Constant_Uint64Value:
		ui := strconv.FormatUint(c.GetUint64Value(), 10)
		con.str.WriteString(ui)
	default:
		return fmt.Errorf("unimplemented : %v", expr)
	}
	return nil
}

func (con *converter) visitIdent(expr *exprpb.Expr) error {
	con.str.WriteString("`")
	con.str.WriteString(expr.GetIdentExpr().GetName())
	con.str.WriteString("`")
	return nil
}

func (con *converter) visitList(expr *exprpb.Expr) error {
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
	switch typ.TypeKind.(type) {
	case *exprpb.Type_MapType_:
		return true
	}
	return false
}

func isListType(typ *exprpb.Type) bool {
	switch typ.TypeKind.(type) {
	case *exprpb.Type_ListType_:
		return true
	}
	return false
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
	switch node.ExprKind.(type) {
	case *exprpb.Expr_ConstExpr:
		switch node.GetConstExpr().ConstantKind.(type) {
		case *exprpb.Constant_NullValue:
			return true
		}
	}
	return false
}

func isBoolLiteral(node *exprpb.Expr) bool {
	switch node.ExprKind.(type) {
	case *exprpb.Expr_ConstExpr:
		switch node.GetConstExpr().ConstantKind.(type) {
		case *exprpb.Constant_BoolValue:
			return true
		}
	}
	return false
}

func isStringLiteral(node *exprpb.Expr) bool {
	switch node.ExprKind.(type) {
	case *exprpb.Expr_ConstExpr:
		switch node.GetConstExpr().ConstantKind.(type) {
		case *exprpb.Constant_StringValue:
			return true
		}
	}
	return false
}

// bytesToOctets converts byte sequences to a string using a three digit octal encoded value
// per byte.
func bytesToOctets(byteVal []byte) string {
	var b strings.Builder
	for _, c := range byteVal {
		fmt.Fprintf(&b, "\\%03o", c)
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
