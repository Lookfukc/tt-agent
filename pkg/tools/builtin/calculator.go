// Package builtin provides a ready-to-use set of built-in tools.
package builtin

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// Calculator is an arithmetic and modulo calculator.
//
// Expressions are parsed via go/ast and evaluated against a whitelist of
// node types; no code is assembled or executed. Exponentiation (^) is not
// supported; operands and results follow floating-point semantics.
type Calculator struct{}

// NewCalculator constructs a calculator.
// returns: a registrable tool instance
func NewCalculator() *Calculator { return &Calculator{} }

// Name returns the tool name.
func (Calculator) Name() string { return "calculator" }

// Description returns the tool description.
func (Calculator) Description() string {
	return "计算算术表达式，支持 + - * / %（取模按浮点语义），例如 (1+2)*3"
}

// Parameters returns the parameter schema.
func (Calculator) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"expression":{"type":"string","description":"算术表达式，如 (1+2)*3"}},"required":["expression"]}`)
}

// Execute evaluates the expression.
// returns: the computed result, or a description of the syntax error
func (Calculator) Execute(_ context.Context, args json.RawMessage) (core.ToolResult, error) {
	var in struct {
		Expression string `json:"expression"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return core.ToolResult{}, fmt.Errorf("invalid args: %w", err)
	}
	val, err := evalExpr(in.Expression)
	if err != nil {
		return core.ToolResult{}, err
	}
	// Overflow/illegal operations produce Inf/NaN, which serialize to JSON
	// as null or invalid literals; silently returning them would let the
	// model keep reasoning on empty values, so report explicitly
	if math.IsInf(val, 0) || math.IsNaN(val) {
		return core.ToolResult{}, fmt.Errorf("result is not a finite number: %v", val)
	}
	return core.ToolResult{Data: map[string]any{"value": val}}, nil
}

// evalExpr parses and evaluates an arithmetic expression.
// returns: the numeric result; an error if any non-whitelisted node is present
func evalExpr(expr string) (float64, error) {
	if expr == "" {
		return 0, fmt.Errorf("empty expression")
	}
	file, err := parser.ParseExpr(expr)
	if err != nil {
		return 0, fmt.Errorf("invalid expression: %w", err)
	}
	return evalNode(file)
}

// evalNode recursively evaluates an AST node, admitting only numeric
// literals and arithmetic operations.
func evalNode(n ast.Expr) (float64, error) {
	switch node := n.(type) {
	case *ast.BinaryExpr:
		l, err := evalNode(node.X)
		if err != nil {
			return 0, err
		}
		r, err := evalNode(node.Y)
		if err != nil {
			return 0, err
		}
		switch node.Op {
		case token.ADD:
			return l + r, nil
		case token.SUB:
			return l - r, nil
		case token.MUL:
			return l * r, nil
		case token.QUO:
			if r == 0 {
				return 0, fmt.Errorf("division by zero")
			}
			return l / r, nil
		case token.REM:
			if r == 0 {
				return 0, fmt.Errorf("division by zero")
			}
			// math.Mod rather than int64 modulo: int64(r) would truncate
			// 0<r<1 to 0, triggering an integer division-by-zero panic,
			// and non-integer operands would also be silently truncated
			return math.Mod(l, r), nil
		default:
			return 0, fmt.Errorf("unsupported operator: %s", node.Op)
		}
	case *ast.UnaryExpr:
		v, err := evalNode(node.X)
		if err != nil {
			return 0, err
		}
		if node.Op == token.SUB {
			return -v, nil
		}
		if node.Op == token.ADD {
			return v, nil
		}
		return 0, fmt.Errorf("unsupported unary operator: %s", node.Op)
	case *ast.BasicLit:
		if node.Kind != token.INT && node.Kind != token.FLOAT {
			return 0, fmt.Errorf("unsupported literal: %s", node.Value)
		}
		var v float64
		if _, err := fmt.Sscanf(node.Value, "%g", &v); err != nil {
			return 0, fmt.Errorf("bad literal: %s", node.Value)
		}
		return v, nil
	case *ast.ParenExpr:
		return evalNode(node.X)
	default:
		return 0, fmt.Errorf("expression contains disallowed syntax")
	}
}
