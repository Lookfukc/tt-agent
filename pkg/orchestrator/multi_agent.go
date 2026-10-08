package orchestrator

import (
	"context"
	"fmt"
	"strings"
)

// routerPromptPrefix is the routing instruction prefix, constraining the
// classifier agent to output only a candidate name.
const routerPromptPrefix = "从以下候选中选择最合适处理该输入的一个，只输出名字本身，不要任何其他内容。候选："

// RouterStep is a routing step.
//
// A classifier agent first picks one executing agent from the candidates,
// then hands the input to it; if the classifier's output is not among the
// candidates the step fails — no fuzzy matching.
type RouterStep struct {
	// Name is the step name.
	Name string
	// Router is the classifier agent name; its output must be one of Candidates.
	Router string
	// Candidates is the list of candidate executing agent names.
	Candidates []string
	// Input is the input template handed to the picked agent.
	Input string
}

// stepKind implements the Step interface.
func (RouterStep) stepKind() {}

// supervisor protocol constants
const (
	superVisorWorker = "WORKER"
	superVisorDone   = "DONE"
	superVisorMaxDef = 8
)

// SupervisorStep is a supervisor step.
//
// A supervisor agent decomposes tasks in a loop and delegates to worker
// agents, using a plain-text protocol: a first line of WORKER <name>
// delegates, with the remainder as the task description; a first line of
// DONE converges, with the remainder as the final answer.
type SupervisorStep struct {
	// Name is the step name.
	Name string
	// Supervisor is the supervisor agent name.
	Supervisor string
	// Workers is the list of worker agents that may be delegated to.
	Workers []string
	// Input is the initial task template.
	Input string
	// MaxRounds is the round limit; 0 takes the default of 8.
	MaxRounds int
}

// stepKind implements the Step interface.
func (SupervisorStep) stepKind() {}

// runRouterStep executes the routing.
// returns: nil after the picked agent's output is written into the run
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
	// The routing decision is also recorded as an output, for easier
	// troubleshooting
	run.Outputs[routerKey(stepAlias(s.Name, s.Router))] = picked
	o.save(run)
	return nil
}

// runSupervisorStep executes the supervision loop.
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

// supervisorPrompt assembles the supervisor agent's input.
// returns: the prompt containing the worker list and the current progress record
func supervisorPrompt(workers []string, transcript string) string {
	var b strings.Builder
	b.WriteString("你是任务监督者。每轮回复首行必须是以下两种之一：\n")
	b.WriteString("WORKER <name> —— 委派该工人执行接下来的任务说明\n")
	b.WriteString("DONE —— 任务完成，接下来是最终答案\n\n")
	b.WriteString("可用工人：" + strings.Join(workers, "、") + "\n\n")
	b.WriteString(transcript)
	return b.String()
}

// agentStepOf synthesizes an AgentStep from a step name and an agent name
// to reuse sessionID.
// returns: the synthesized step
func agentStepOf(name, agentName string) AgentStep {
	return AgentStep{Name: name, Agent: agentName}
}

// routerKey is the output key for a routing decision.
// returns: the step name plus a suffix
func routerKey(name string) string { return name + ":route" }

// stepAlias is the step output key, falling back to the agent name when
// Name is empty.
// returns: the output key
func stepAlias(name, fallback string) string {
	if name != "" {
		return name
	}
	return fallback
}

// contains is a linear containment check.
// returns: true if present
func contains(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}
