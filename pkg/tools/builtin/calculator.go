// Package builtin 提供开箱即用的内置工具集
package builtin

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// Calculator 四则运算与取模计算器
//
// 表达式经 go/ast 解析后按白名单节点求值，不拼接执行任何代码；
// 不支持幂运算（^），操作数与结果均为浮点语义
type Calculator struct{}

// NewCalculator 构造计算器
// returns: 可注册的工具实例
func NewCalculator() *Calculator { return &Calculator{} }

// Name 工具名
func (Calculator) Name() string { return "calculator" }

// Description 工具描述
func (Calculator) Description() string {
	return "计算算术表达式，支持 + - * / %（取模按浮点语义），例如 (1+2)*3"
}

// Parameters 参数 schema
func (Calculator) Parameters() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"expression":{"type":"string","description":"算术表达式，如 (1+2)*3"}},"required":["expression"]}`)
}

// Execute 求值表达式
// returns: 计算结果或语法错误说明
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
	// 溢出/非法运算会产生 Inf/NaN，序列化成 JSON 只能得到 null 或
	// 非法字面量，静默返回会让模型拿空值继续推理，显式报错
	if math.IsInf(val, 0) || math.IsNaN(val) {
		return core.ToolResult{}, fmt.Errorf("result is not a finite number: %v", val)
	}
	return core.ToolResult{Data: map[string]any{"value": val}}, nil
}

// evalExpr 解析并求值算术表达式
// returns: 数值结果；含非白名单节点时返回错误
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

// evalNode 递归求值 AST 节点，只放行数值字面量与算术运算
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
			// math.Mod 而非 int64 取模：int64(r) 会把 0<r<1 截成 0
			// 触发整型除零 panic，非整数操作数也会被静默截断
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
