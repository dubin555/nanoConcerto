package concerto

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Concert 定义流程编排的核心结构
type Concerto struct {
	taskBlueprints map[string]*taskBlueprint
	isBuilt bool
	buildErr error
	taskReportHandler TaskReportHandler // 任务报告处理器
	reportContextKeys []string // 报告上下文键
}

type TaskState struct {
	R interface{} // 任务返回值
	E error // 任务错误
}

type TaskReport struct {
	StartTime *time.Time // 任务开始时间
	StartTimeForTaskFn *time.Time // 任务函数开始时间
	EndTime *time.Time // 任务结束时间
	Name string // 任务名称
	Error error // 任务错误
	ContextTags map[string]interface{} // 任务上下文提取的标签信息
}

// TaskReportHandler 任务报告处理器
type TaskReportHandler func(report *TaskReport)

// taskFunc 任务函数
type taskFunc func(ctx context.Context, res map[string]*TaskState) (interface{}, error)

// taskBlueprint 任务蓝图
type taskBlueprint struct {
	Name string // 任务名称
	Fn taskFunc // 任务函数
	DependsOn []string // 依赖的任务名称
	ConsumerCnt int // 消费者数量
}

func New() *Concerto {
	return &Concerto{
		taskBlueprints: make(map[string]*taskBlueprint),
		isBuilt: false,
	}
}

func (c *Concerto) Add(name string, dep []string, fn taskFunc) *Concerto {
	if c.buildErr != nil {
		return c
	}
	if _, ok := c.taskBlueprints[name]; ok {
		c.buildErr = fmt.Errorf("task %s already exists", name)
		c.isBuilt = false
		return c
	}
	c.taskBlueprints[name] = &taskBlueprint{
		Name: name,
		Fn: fn,
		DependsOn: dep,
	}
	c.isBuilt = false
	return c
}

func (c *Concerto) SetTaskReportHandler(handler TaskReportHandler) *Concerto {
	c.taskReportHandler = handler
	return c
}

func (c *Concerto) SetReportContextKeys(keys ...string) *Concerto {
	c.reportContextKeys = keys
	return c
}

// dfs 检测循环依赖
func dfs(node string, visited map[string]int, c *Concerto, path []string) (bool, []string) {
	if visited[node] == 1 {
		return true, append(path, node) // 1 表示当前节点在当前路径中
	}
	if visited[node] == 2 {
		return false, path // 2 表示当前节点已经被访问过，无需重复访问
	}
	visited[node] = 1
	currentPath := append(path, node)

	taskBp, _ := c.taskBlueprints[node]
	for _, dep := range taskBp.DependsOn {
		if _, depExists := c.taskBlueprints[dep]; !depExists {
			return false, append(currentPath, dep)
		}
		if cyclic, cyclePath := dfs(dep, visited, c, currentPath); cyclic {
			return true, cyclePath
		}
	}

	visited[node] = 2
	return false, currentPath
}

// detectCycle 检测循环依赖
func (c *Concerto) detectCycle() (bool, []string) {
	visited := make(map[string]int)
	for node := range c.taskBlueprints {
		if cyclic, cyclePath := dfs(node, visited, c, nil); cyclic {
			return true, cyclePath
		}
	}
	return false, nil
}

func (c *Concerto) Build() error {
	c.isBuilt = false // 重置构建状态
	if c.buildErr != nil {
		return c.buildErr
	}
	c.buildErr = nil
	if len(c.taskBlueprints) == 0 {
		c.isBuilt = true
		return nil
	}

	// 初始化 ConsumerCnt
	for name, bp := range c.taskBlueprints {
		if bp.ConsumerCnt == 0 {
			bp.ConsumerCnt = 1
		}

		for _, depName := range bp.DependsOn {
			if depName == name {
				c.buildErr = fmt.Errorf("task %s depends on itself", name)
				return c.buildErr
			}
			if _, ok := c.taskBlueprints[depName]; !ok {
				c.buildErr = fmt.Errorf("task %s depends on non-existent task %s", name, depName)
				return c.buildErr
			}
		}
	}

	// 计算每个任务被多少其他任务依赖
	for _, bp := range c.taskBlueprints {
		for _, depName := range bp.DependsOn {
			c.taskBlueprints[depName].ConsumerCnt++
		}
	}

	// 检测循环依赖
	if cyclic, cyclePath := c.detectCycle(); cyclic {
		c.buildErr = fmt.Errorf("cycle detected: %s", strings.Join(cyclePath, " -> "))
		return c.buildErr
	}

	c.isBuilt = true
	return nil
}

type task struct {
	TaskBlueprint *taskBlueprint // 任务蓝图
	C chan *TaskState // 任务结果通道
	once sync.Once // 确保任务只执行一次
	Name string // 任务名称
}

// 将任务结果发送到任务结果通道
func (t *task) done(r interface{}, e error) {
	taskState := &TaskState{
		R: r,
		E: e,
	}
	for i := 0; i < t.TaskBlueprint.ConsumerCnt; i++ {
		t.C <- taskState
	}
}

// 关闭任务结果通道
func (t *task) close() {
	t.once.Do(func() {
		close(t.C)
	})
}

// 等待所有依赖任务完成
func awaitDependencies(
	ctx context.Context, 
	currTaskName string,
	dependencies []string,
	allTasks map[string]*task) (map[string]*TaskState, error) {
		dependenciesRes := make(map[string]*TaskState, len(dependencies))
		for _, dep := range dependencies {
			depTask := allTasks[dep]
			var depState *TaskState
			select {
			case depState = <-depTask.C:
				if depState.E != nil {
					return nil, depState.E
				}
			case <-ctx.Done():
				return nil, fmt.Errorf("task %s cancelled while waiting for dep '%s'", currTaskName, depTask.Name)
			}
			dependenciesRes[depTask.Name] = depState
		}
	return dependenciesRes, nil
}

func (c *Concerto) Run(ctx context.Context, timeoutInMs int64) (map[string]*TaskState, error) {
	if !c.isBuilt {
		return nil, fmt.Errorf("concerto not built")
	}
	if c.buildErr!= nil {
		return nil, c.buildErr
	}
	if len(c.taskBlueprints) == 0 {
		return nil, nil
	}

	ctx, cancelFunc := context.WithTimeout(ctx, time.Duration(timeoutInMs)*time.Millisecond)
	defer cancelFunc()

	runTasks := make(map[string]*task, len(c.taskBlueprints))
	for name, bp := range c.taskBlueprints {
		runTasks[name] = &task{
			TaskBlueprint: bp,
			C: make(chan *TaskState, bp.ConsumerCnt),
			Name: name,
		}
	}

	var wg sync.WaitGroup
	for _, t := range runTasks {
		wg.Add(1)
		go func(t *task) {
			defer wg.Done()
			defer t.close()

			// 等待依赖任务完成
			startTime := time.Now()
			dependentRes, err := awaitDependencies(ctx, t.Name, t.TaskBlueprint.DependsOn, runTasks)
			if err!= nil {
				if c.taskReportHandler!= nil {
					c.taskReportHandler(&TaskReport{
						StartTime: &startTime,
						Name: t.Name,
						Error: err,
					})
				}
				t.done(nil, err)
				return
			}

			// 执行任务
			taskStartTime := time.Now()
			r, e := t.TaskBlueprint.Fn(ctx, dependentRes)
			if c.taskReportHandler!= nil {
				taskEndTime := time.Now()
				contextTags := make(map[string]interface{})
				if c.reportContextKeys!= nil {
					for _, key := range c.reportContextKeys {
						if v, ok := ctx.Value(key).(interface{}); ok {
							contextTags[key] = v
						}
					}
				}
				c.taskReportHandler(&TaskReport{
					StartTime: &startTime,
					StartTimeForTaskFn: &taskStartTime,
					EndTime: &taskEndTime,
					Name: t.Name,
					Error: e,
					ContextTags: contextTags,
				})
			}
			t.done(r, e)

		}(t)
	}

	finalResults := make(map[string]*TaskState, len(c.taskBlueprints))
	doneCh := make(chan error, 1)

	go func ()  {
		wg.Wait()
		var firstError error
		for name, t := range runTasks {
			select {
			case taskRes, ok := <- t.C:
				if !ok {
					finalResults[name] = &TaskState{
						R: nil,
						E: fmt.Errorf("task %s closed channel", name),
					}
				} else if taskRes == nil {
					finalResults[name] = &TaskState{
						R: nil,
						E: fmt.Errorf("task %s returned nil", name),
					}
				} else {
					finalResults[name] = taskRes
				}
			case <- ctx.Done():
				finalResults[name] = &TaskState{
					R: nil,
					E: ctx.Err(),
				}
			}
			if res, exists := finalResults[name]; exists && res.E!= nil && firstError == nil {
				if firstError == nil {
					firstError = res.E
				}
			}
		}
		doneCh <- firstError
		close(doneCh)
		
	}()

	// 等待所有任务完成
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-doneCh:
		return finalResults, err
	}
}

