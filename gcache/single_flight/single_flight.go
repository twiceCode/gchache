package singleflight

import "sync"

// 代表当前正在进行或者已经进行结束的请求
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()         // 发现有相同的请求正在执行，停止等待
		return c.val, c.err // 所有相同请求执行完成，返回结果
	}

	c := new(call) // 有新请求进来了，创建一个call结构体指针
	c.wg.Add(1)    // 当前正在执行的加一
	g.m[key] = c   // 添加到map中
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key) // map是线程不安全的
	g.mu.Unlock()

	return c.val, c.err
}
