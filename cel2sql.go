package cel2sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/google/cel-go/common/operators"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// Implementations based on `google/cel-go`'s unparser
// https://github.com/google/cel-go/blob/master/parser/unparser.go

func Convert(expr *exprpb.Expr) (string, error) {
	un := &converter{}
	err := un.visit(expr)
	if err != nil {
		return "", err
	}
	return un.str.String(), nil
}

type converter struct {
	str strings.Builder
}

func (un *converter) visit(expr *exprpb.Expr) error {
	switch expr.ExprKind.(type) {
	case *exprpb.Expr_CallExpr:
		return un.visitCall(expr)
	// TODO: Comprehensions are currently not supported.
	case *exprpb.Expr_ComprehensionExpr:
		return un.visitComprehension(expr)
	case *exprpb.Expr_ConstExpr:
		return un.visitConst(expr)
	case *exprpb.Expr_IdentExpr:
		return un.visitIdent(expr)
	case *exprpb.Expr_ListExpr:
		return un.visitList(expr)
	case *exprpb.Expr_SelectExpr:
		return un.visitSelect(expr)
	case *exprpb.Expr_StructExpr:
		return un.visitStruct(expr)
	}
	return fmt.Errorf("unsupported expr: %v", expr)
}

func (un *converter) visitCall(expr *exprpb.Expr) error {
	c := expr.GetCallExpr()
	fun := c.GetFunction()
	switch fun {
	// ternary operator
	case operators.Conditional:
		return un.visitCallConditional(expr)
	// index operator
	case operators.Index:
		return un.visitCallIndex(expr)
	// unary operators
	case operators.LogicalNot, operators.Negate:
		return un.visitCallUnary(expr)
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
		return un.visitCallBinary(expr)
	// standard function calls.
	default:
		return un.visitCallFunc(expr)
	}
}

var standardSQLBinaryOperators = map[string]string{
	operators.LogicalAnd: "AND",
	operators.LogicalOr:  "OR",
	operators.Equals:     "=",
}

func (un *converter) visitCallBinary(expr *exprpb.Expr) error {
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
	err := un.visitMaybeNested(lhs, lhsParen)
	if err != nil {
		return err
	}
	var operator string
	if fun == operators.Equals && (isNullLiteral(rhs) || isBoolLiteral(rhs)) {
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
	un.str.WriteString(" ")
	un.str.WriteString(operator)
	un.str.WriteString(" ")
	return un.visitMaybeNested(rhs, rhsParen)
}

func (un *converter) visitCallConditional(expr *exprpb.Expr) error {
	c := expr.GetCallExpr()
	args := c.GetArgs()
	un.str.WriteString("IF(")
	if err := un.visit(args[0]); err != nil {
		return err
	}
	un.str.WriteString(", ")
	if err := un.visit(args[1]); err != nil {
		return err
	}
	un.str.WriteString(", ")
	if err := un.visit(args[2]); err != nil {
		return nil
	}
	un.str.WriteString(")")
	return nil
}

var standardSQLFunctions = map[string]string{
	"startsWith": "STARTS_WITH",
	"endsWith":   "ENDS_WITH",
	"matches":    "REGEXP_CONTAINS",
	"contains":   "INSTR",
}

func (un *converter) visitCallFunc(expr *exprpb.Expr) error {
	c := expr.GetCallExpr()
	fun := c.GetFunction()
	args := c.GetArgs()
	sqlFun, ok := standardSQLFunctions[fun]
	if !ok {
		return fmt.Errorf("unsupported function: %s", fun)
	}
	un.str.WriteString(sqlFun)
	un.str.WriteString("(")
	if c.GetTarget() != nil {
		nested := isBinaryOrTernaryOperator(c.GetTarget())
		err := un.visitMaybeNested(c.GetTarget(), nested)
		if err != nil {
			return err
		}
		un.str.WriteString(", ")
	}
	for i, arg := range args {
		err := un.visit(arg)
		if err != nil {
			return err
		}
		if i < len(args)-1 {
			un.str.WriteString(", ")
		}
	}
	un.str.WriteString(")")
	if fun == "contains" {
		un.str.WriteString(" != 0")
	}
	return nil
}

func (un *converter) visitCallIndex(expr *exprpb.Expr) error {
	c := expr.GetCallExpr()
	args := c.GetArgs()
	nested := isBinaryOrTernaryOperator(args[0])
	err := un.visitMaybeNested(args[0], nested)
	if err != nil {
		return err
	}
	un.str.WriteString("[OFFSET(")
	err = un.visit(args[1])
	if err != nil {
		return err
	}
	un.str.WriteString(")]")
	return nil
}

var standardSQLUnaryOperators = map[string]string{
	operators.LogicalNot: "NOT ",
}

func (un *converter) visitCallUnary(expr *exprpb.Expr) error {
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
	un.str.WriteString(operator)
	nested := isComplexOperator(args[0])
	return un.visitMaybeNested(args[0], nested)
}

func (un *converter) visitComprehension(expr *exprpb.Expr) error {
	// TODO: introduce a macro expansion map between the top-level comprehension id and the
	// function call that the macro replaces.
	return fmt.Errorf("unimplemented : %v", expr)
}

func (un *converter) visitConst(expr *exprpb.Expr) error {
	c := expr.GetConstExpr()
	switch c.ConstantKind.(type) {
	case *exprpb.Constant_BoolValue:
		if c.GetBoolValue() {
			un.str.WriteString("TRUE")
		} else {
			un.str.WriteString("FALSE")
		}
	case *exprpb.Constant_BytesValue:
		b := c.GetBytesValue()
		un.str.WriteString(`b"`)
		un.str.WriteString(bytesToOctets(b))
		un.str.WriteString(`"`)
	case *exprpb.Constant_DoubleValue:
		d := strconv.FormatFloat(c.GetDoubleValue(), 'g', -1, 64)
		un.str.WriteString(d)
	case *exprpb.Constant_Int64Value:
		i := strconv.FormatInt(c.GetInt64Value(), 10)
		un.str.WriteString(i)
	case *exprpb.Constant_NullValue:
		un.str.WriteString("NULL")
	case *exprpb.Constant_StringValue:
		un.str.WriteString(strconv.Quote(c.GetStringValue()))
	case *exprpb.Constant_Uint64Value:
		ui := strconv.FormatUint(c.GetUint64Value(), 10)
		un.str.WriteString(ui)
	default:
		return fmt.Errorf("unimplemented : %v", expr)
	}
	return nil
}

func (un *converter) visitIdent(expr *exprpb.Expr) error {
	un.str.WriteString("`")
	un.str.WriteString(expr.GetIdentExpr().GetName())
	un.str.WriteString("`")
	return nil
}

func (un *converter) visitList(expr *exprpb.Expr) error {
	l := expr.GetListExpr()
	elems := l.GetElements()
	un.str.WriteString("[")
	for i, elem := range elems {
		err := un.visit(elem)
		if err != nil {
			return err
		}
		if i < len(elems)-1 {
			un.str.WriteString(", ")
		}
	}
	un.str.WriteString("]")
	return nil
}

func (un *converter) visitSelect(expr *exprpb.Expr) error {
	sel := expr.GetSelectExpr()
	// handle the case when the select expression was generated by the has() macro.
	if sel.GetTestOnly() {
		un.str.WriteString("has(")
	}
	nested := !sel.GetTestOnly() && isBinaryOrTernaryOperator(sel.GetOperand())
	err := un.visitMaybeNested(sel.GetOperand(), nested)
	if err != nil {
		return err
	}
	un.str.WriteString(".")
	un.str.WriteString(sel.GetField())
	if sel.GetTestOnly() {
		un.str.WriteString(")")
	}
	return nil
}

func (un *converter) visitStruct(expr *exprpb.Expr) error {
	s := expr.GetStructExpr()
	// If the message name is non-empty, then this should be treated as message construction.
	if s.GetMessageName() != "" {
		return un.visitStructMsg(expr)
	}
	// Otherwise, build a map.
	return un.visitStructMap(expr)
}

func (un *converter) visitStructMsg(expr *exprpb.Expr) error {
	m := expr.GetStructExpr()
	entries := m.GetEntries()
	un.str.WriteString(m.GetMessageName())
	un.str.WriteString("{")
	for i, entry := range entries {
		f := entry.GetFieldKey()
		un.str.WriteString(f)
		un.str.WriteString(": ")
		v := entry.GetValue()
		err := un.visit(v)
		if err != nil {
			return err
		}
		if i < len(entries)-1 {
			un.str.WriteString(", ")
		}
	}
	un.str.WriteString("}")
	return nil
}

func (un *converter) visitStructMap(expr *exprpb.Expr) error {
	m := expr.GetStructExpr()
	entries := m.GetEntries()
	un.str.WriteString("{")
	for i, entry := range entries {
		k := entry.GetMapKey()
		err := un.visit(k)
		if err != nil {
			return err
		}
		un.str.WriteString(": ")
		v := entry.GetValue()
		err = un.visit(v)
		if err != nil {
			return err
		}
		if i < len(entries)-1 {
			un.str.WriteString(", ")
		}
	}
	un.str.WriteString("}")
	return nil
}

func (un *converter) visitMaybeNested(expr *exprpb.Expr, nested bool) error {
	if nested {
		un.str.WriteString("(")
	}
	err := un.visit(expr)
	if err != nil {
		return err
	}
	if nested {
		un.str.WriteString(")")
	}
	return nil
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
