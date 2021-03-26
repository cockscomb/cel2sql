package cel2sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/google/cel-go/cel"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func ConvertCellToSqlCondition(ast *cel.Ast) (string, error) {
	builder := strings.Builder{}

	if err := processNode(ast.Expr(), &builder); err != nil {
		return "", err
	}

	return builder.String(), nil
}

var callProcessors map[string]func(call *exprpb.Expr_Call, builder *strings.Builder) error

func init() {
	callProcessors = map[string]func(call *exprpb.Expr_Call, builder *strings.Builder) error{
		"_==_":       processRelationCall,
		"_!=_":       processRelationCall,
		"_<_":        processRelationCall,
		"_<=_":       processRelationCall,
		"_>_":        processRelationCall,
		"_>=_":       processRelationCall,
		"startsWith": processFunctionCall,
	}
}

func processNode(node *exprpb.Expr, builder *strings.Builder) error {
	switch node.ExprKind.(type) {
	case *exprpb.Expr_ConstExpr:
		return processConst(node.GetConstExpr(), builder)
	case *exprpb.Expr_IdentExpr:
		return processIdent(node.GetIdentExpr(), builder)
	case *exprpb.Expr_CallExpr:
		return processCall(node.GetCallExpr(), builder)
	default:
		panic(fmt.Sprintf("unsupported node: %+v", node.ExprKind))
	}
	return nil
}

func processNodes(nodes []*exprpb.Expr, builder *strings.Builder) error {
	length := len(nodes)
	for i, node := range nodes {
		if err := processNode(node, builder); err != nil {
			return err
		}
		if i < length-1 {
			builder.WriteString(", ")
		}
	}
	return nil
}

func processConst(literal *exprpb.Constant, builder *strings.Builder) error {
	switch literal.ConstantKind.(type) {
	case *exprpb.Constant_BoolValue:
		if literal.GetBoolValue() {
			builder.WriteString("TRUE")
		} else {
			builder.WriteString("FALSE")
		}
	case *exprpb.Constant_DoubleValue:
		builder.WriteString(strconv.FormatFloat(literal.GetDoubleValue(), 'f', -1, 64))
	case *exprpb.Constant_Int64Value:
		builder.WriteString(strconv.FormatInt(literal.GetInt64Value(), 10))
	case *exprpb.Constant_NullValue:
		builder.WriteString("NULL")
	case *exprpb.Constant_StringValue:
		builder.WriteString(`"`)
		builder.WriteString(literal.GetStringValue())
		builder.WriteString(`"`)
	case *exprpb.Constant_Uint64Value:
		builder.WriteString(strconv.FormatUint(literal.GetUint64Value(), 10))
	default:
		panic(fmt.Sprintf("unsupported literal: %+v", literal.ConstantKind))
	}
	return nil
}

func processIdent(ident *exprpb.Expr_Ident, builder *strings.Builder) error {
	builder.WriteString("`")
	builder.WriteString(ident.GetName())
	builder.WriteString("`")
	return nil
}

func processCall(call *exprpb.Expr_Call, builder *strings.Builder) error {
	function := call.GetFunction()
	processor, ok := callProcessors[function]
	if !ok {
		return fmt.Errorf("unsupported function: %s", function)
	}
	return processor(call, builder)
}

func processRelationCall(call *exprpb.Expr_Call, builder *strings.Builder) error {
	function := call.GetFunction()
	args := call.GetArgs()
	if len(args) != 2 {
		panic(fmt.Sprintf("unexpected argument count: %d", len(args)))
	}
	lhs := args[0]
	rhs := args[1]
	if err := processNode(lhs, builder); err != nil {
		return err
	}
	switch function {
	case "_==_":
		if isNullLiteral(lhs) || isNullLiteral(rhs) {
			builder.WriteString(" IS ")
		} else {
			builder.WriteString(" = ")
		}
	case "_!=_":
		if isNullLiteral(lhs) || isNullLiteral(rhs) {
			builder.WriteString(" IS NOT ")
		} else {
			builder.WriteString(" != ")
		}
	case "_<_":
		builder.WriteString(" < ")
	case "_<=_":
		builder.WriteString(" <= ")
	case "_>_":
		builder.WriteString(" > ")
	case "_>=_":
		builder.WriteString(" >= ")
	}
	if err := processNode(rhs, builder); err != nil {
		return err
	}
	return nil
}

func processFunctionCall(call *exprpb.Expr_Call, builder *strings.Builder) error {
	function := call.GetFunction()
	switch function {
	case "startsWith":
		builder.WriteString("STARTS_WITH")
	}
	builder.WriteString("(")
	if err := processNode(call.GetTarget(), builder); err != nil {
		return err
	}
	builder.WriteString(", ")
	if err := processNodes(call.GetArgs(), builder); err != nil {
		return err
	}
	builder.WriteString(")")
	return nil
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
