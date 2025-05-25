# Concerto (协奏曲) - Go语言任务编排框架

## 🎼 项目简介

Concerto（协奏曲）是一个轻量级的Go语言任务编排框架，用于管理具有依赖关系的多个任务的执行流程。它通过DAG（有向无环图）来组织任务，自动处理任务间的依赖关系，并支持并行执行。
**整个代码仓库用于教学目的**
**AI时代来临了，代码99%都是AI生成的。当然需要有人工干预的部分，比如要解决的问题，设计思路，变量名，等等**

## 🚀 核心特性

- **任务依赖管理**：声明式定义任务依赖关系
- **并行执行**：自动并行执行无依赖关系的任务
- **超时控制**：支持全局和单个任务的超时设置
- **执行监控**：提供任务执行时间统计和报告
- **上下文传递**：支持在任务间传递上下文信息

## 📚 使用场景

1. **特征计算, 多数据源聚合**：如BMI指数计算（需要先获取身高和体重）如商家特征+行业特征的组合
3. **复杂业务流程**：需要协调多个微服务调用的场景
4. **ETL流程**：数据抽取、转换、加载的管道处理

## 🛠️ 快速开始

```go
package main

import (
    "context"
    "dubin555.github.com/concerto/concerto"
)

func main() {
    c := concerto.New()
    
    // 添加任务（名称，依赖，处理函数）
    c.Add("getWeight", []string{}, getWeight)
    c.Add("getHeight", []string{}, getHeight)
    c.Add("getBMI", []string{"getWeight", "getHeight"}, getBMI)
    
    // 构建并执行
    _ = c.Build()
    ctx := context.Background()
    _, _ = c.Run(ctx, 1000) // 1000ms超时
}
```


## 📈 性能监控
框架提供任务执行时间统计：


```plainText
任务 getHeight 用户 user1 总耗时: 62.13ms, 任务执行耗时: 62.13ms
任务 getWeight 用户 user2 总耗时: 62.25ms, 任务执行耗时: 62.25ms
```