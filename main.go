package main

import (
	"context"
	"fmt"
	"time"
	"math/rand"
	"sync"

	"dubin555.github.com/concerto/concerto"
)

const userIDContextKey = "userID"

func getWeight(ctx context.Context, res map[string]*concerto.TaskState) (interface{}, error) {
	userID := ctx.Value(userIDContextKey).(string)
	fmt.Printf("获取用户体重: %s\n", userID)
	time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
	weight := 70 + rand.Intn(10)
	fmt.Printf("用户 %s 的体重为: %d\n", userID, weight)
	return weight, nil
}

func getHeight(ctx context.Context, res map[string]*concerto.TaskState) (interface{}, error) {
	userID := ctx.Value(userIDContextKey).(string)
	fmt.Printf("获取用户身高: %s\n", userID)
	time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
	height := 170 + rand.Intn(10)
	fmt.Printf("用户 %s 的身高为: %d\n", userID, height)
	return height, nil
}

func getBMI(ctx context.Context, res map[string]*concerto.TaskState) (interface{}, error) {
	userID := ctx.Value(userIDContextKey).(string)
	fmt.Printf("计算用户BMI: %s\n", userID)
	weight := float64(res["getWeight"].R.(int))
	height := float64(res["getHeight"].R.(int))
	bmi := weight / (height * height / 10000)
	fmt.Printf("用户 %s 的BMI为: %.2f\n", userID, bmi)
	return bmi, nil
}

func main() {
	c := concerto.New()
	c.Add("getWeight", []string{}, getWeight)
	c.Add("getHeight", []string{}, getHeight)
	c.Add("getBMI", []string{"getWeight", "getHeight"}, getBMI)
	c.SetTaskReportHandler(func(report *concerto.TaskReport) {
		fmt.Printf("任务 %s 用户 %s 总耗时: %v, 任务执行耗时: %v \n", report.Name, report.ContextTags[userIDContextKey].(string) ,report.EndTime.Sub(*report.StartTime), report.EndTime.Sub(*report.StartTimeForTaskFn))
	})
	c.SetReportContextKeys(userIDContextKey)
	fmt.Println("开始构建")
	_ = c.Build()
	fmt.Println("开始执行")
	userIds := []string{"user1", "user2"}
	var wg sync.WaitGroup
	for _, userID := range userIds {
		wg.Add(1)
		go func(userID string) {
			defer wg.Done()
			ctx := context.WithValue(context.Background(), userIDContextKey, userID)
			_, _ = c.Run(ctx, 1000)
		}(userID)
	}
	wg.Wait()
	fmt.Printf("所有用户的BMI计算完成")
}