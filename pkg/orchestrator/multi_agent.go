package orchestrator

import (
	"context"
	"fmt"
	"strings"
)

// routerPromptPrefix 路由指令前缀，约束分类 Agent 只输出候选名
const routerPromptPrefix = "从以下候选中选择最合适处理该输入的一个，只输出名字本身，不要任何其他内容。候选："

// RouterStep 路由步骤
//
// 先用分类 Agent 从候选中选出一个执行 Agent，再把输入交给它；
// 分类输出不在候选内时步骤失败，不做模糊匹配
type RouterStep struct {
	// Name 步骤名
	Name string
	// Router 分类 Agent 名，其输出必须是 Candidates 之一
	Router string
	// Candidates 可选执行 Agent 名单
	Candidates []string
	// Input 交给被选 Agent 的输入模板
	Input string
}

// stepKind 实现 Step 接口
func (RouterStep) stepKind() {}

// supervisor 协议常量
const (
	superVisorWorker = "WORKER"
	superVisorDone   = "DONE"
	superVisorMaxDef = 8
)

// SupervisorStep 监督者步骤
//
// 监督 Agent 循环分解任务并委派工人 Agent，协议为纯文本：
// 首行 WORKER <name> 时委派，余下为任务说明；
// 首行 DONE 时收敛，余下为最终答案
type SupervisorStep struct {
	// Name 步骤名
	Name string
	// Supervisor 监督 Agent 名
	Supervisor string
	// Workers 可委派的工人 Agent 名单
	Workers []string
	// Input 初始任务模板
	Input string
	// MaxRounds 最大轮数，0 取默认 8
	MaxRounds int
}

// stepKind 实现 Step 接口
func (SupervisorStep) stepKind() {}

// runRouterStep 执行路由
// returns: 被选 Agent 的输出写入 run 后返回 nil
func (o *Orchestrator) runRouterStep(ctx context.Context, run *RunState, s RouterStep) error {
	o.mu.RLock()
	router, ok := o.agents[s.Router]
	o.mu.RUnlock()
	if !ok {
		return fmt.Errorf("router agent not registered: %s", s.Router)
	}

	candidates := strings.Join(s.Candidates, "、")
	choice, _, err := router.Run(ctx, sessionID(run.ID, agentStepOf(s.Name, s.Router)),
		routerPromptPrefix+candidates+"\n输入："+resolveInput(s.Input, run))
	if err != nil {
		return fmt.Errorf("router %q: %w", s.Router, err)
	}

	picked := strings.TrimSpace(choice.Content)
	if !contains(s.Candidates, picked) {
		return fmt.Errorf("router picked %q which is not in candidates %v", picked, s.Candidates)
	}

	target := AgentStep{Name: s.Name, Agent: picked, Input: s.Input}
	if err := o.runAgentStep(ctx, run, target); err != nil {
		return err
	}
	// 路由决策也落输出，便于排查
	run.Outputs[routerKey(stepAlias(s.Name, s.Router))] = picked
	o.save(run)
	return nil
}

// runSupervisorStep 执行监督循环
func (o *Orchestrator) runSupervisorStep(ctx context.Context, run *RunState, s SupervisorStep) error {
	o.mu.RLock()
	sup, ok := o.agents[s.Supervisor]
	o.mu.RUnlock()
	if !ok {
		return fmt.Errorf("supervisor agent not registered: %s", s.Supervisor)
	}
	for _, w := range s.Workers {
		o.mu.RLock()
		_, ok := o.agents[w]
		o.mu.RUnlock()
		if !ok {
			return fmt.Errorf("worker agent not registered: %s", w)
		}
	}

	maxRounds := s.MaxRounds
	if maxRounds <= 0 {
		maxRounds = superVisorMaxDef
	}

	task := resolveInput(s.Input, run)
	var transcript strings.Builder
	transcript.WriteString("任务：" + task + "\n")

	for round := 1; round <= maxRounds; round++ {
		prompt := supervisorPrompt(s.Workers, transcript.String())
		msg, _, err := sup.Run(ctx, sessionID(run.ID, agentStepOf(s.Name, s.Supervisor)), prompt)
		if err != nil {
			return fmt.Errorf("supervisor %q round %d: %w", s.Supervisor, round, err)
		}
		head, rest, found := strings.Cut(msg.Content, "\n")
		head = strings.TrimSpace(head)
		switch {
		case !found || head == "":
			return fmt.Errorf("supervisor output missing protocol line: %q", msg.Content)
		case head == superVisorDone:
			run.Outputs[stepAlias(s.Name, s.Supervisor)] = strings.TrimSpace(rest)
			run.Prev = strings.TrimSpace(rest)
			run.StepIdx++
			o.save(run)
			return nil
		case strings.HasPrefix(head, superVisorWorker+" "):
			worker := strings.TrimSpace(strings.TrimPrefix(head, superVisorWorker+" "))
			if !contains(s.Workers, worker) {
				return fmt.Errorf("supervisor delegated to unknown worker %q", worker)
			}
			o.mu.RLock()
			loop := o.agents[worker]
			o.mu.RUnlock()
			result, _, err := loop.Run(ctx, sessionID(run.ID, agentStepOf(s.Name, worker)), strings.TrimSpace(rest))
			if err != nil {
				return fmt.Errorf("worker %q: %w", worker, err)
			}
			transcript.WriteString(fmt.Sprintf("[委派 %s] %s\n[%s 结果] %s\n", worker, rest, worker, result.Content))
		default:
			return fmt.Errorf("supervisor protocol violation: %q", head)
		}
	}
	return fmt.Errorf("supervisor exceeded %d rounds", maxRounds)
}

// supervisorPrompt 组装监督 Agent 的输入
// returns: 含工人名单与当前进展记录的提示
func supervisorPrompt(workers []string, transcript string) string {
	var b strings.Builder
	b.WriteString("你是任务监督者。每轮回复首行必须是以下两种之一：\n")
	b.WriteString("WORKER <name> —— 委派该工人执行接下来的任务说明\n")
	b.WriteString("DONE —— 任务完成，接下来是最终答案\n\n")
	b.WriteString("可用工人：" + strings.Join(workers, "、") + "\n\n")
	b.WriteString(transcript)
	return b.String()
}

// agentStepOf 用步骤名与 Agent 名合成一个 AgentStep 以复用 sessionID
// returns: 合成步骤
func agentStepOf(name, agentName string) AgentStep {
	return AgentStep{Name: name, Agent: agentName}
}

// routerKey 路由决策的输出键
// returns: 步骤名加后缀
func routerKey(name string) string { return name + ":route" }

// stepAlias 步骤输出键，Name 为空时退回 agent 名
// returns: 输出键
func stepAlias(name, fallback string) string {
	if name != "" {
		return name
	}
	return fallback
}

// contains 线性包含判断
// returns: true 表示存在
func contains(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}
