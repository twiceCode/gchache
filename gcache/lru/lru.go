package lru

import "container/list"

//疑问：为什么值类型为Value,不能为interface{}吗？
type Value interface {
	Len() int64 //返回值占用的内存大小
}

type Cache struct {
	maxBytes int64 // 最大内存
	nowBytes int64 // 已使用内存

	dList *list.List
	cache map[string]*list.Element // 缓存，使用双向队列加dic来实现LRU算法

	OnEvicted func(key string, value Value) // 某条记录被移除时的回调函数，可以为 nil
}

type entry struct {
	key   string // 方便移除map中key对应的value
	value Value
}

//初始化一个Cache
func New(maxBytes int64, onEvicted func(key string, value Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		dList:     list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

//移除缓存中最久未被使用的元素
func (c *Cache) RemoveOdlest() {
	el := c.dList.Back()
	if el != nil {
		c.dList.Remove(el)
		kv := el.Value.(*entry)
		//根据key删除
		delete(c.cache, kv.key)
		c.nowBytes -= (int64(len(kv.key)) + kv.value.Len())
		//调用回调函数
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

//插入一个数据
func (c *Cache) Set(key string, val Value) {
	if el, ok := c.cache[key]; ok {
		//键存在,就更新
		c.dList.MoveToFront(el)
		kv := el.Value.(*entry)
		c.nowBytes += (val.Len() - kv.value.Len())
		//kv的类型是*entry，直接赋值可以修改
		kv.value = val
	} else {
		//插入一个节点
		el := c.dList.PushFront(&entry{key: key, value: val})
		c.cache[key] = el
		c.nowBytes += (int64(len(key)) + val.Len())
	}
	//判断当前占用内存有没有超过最大内存，有的话就移除队头节点
	for c.nowBytes > c.maxBytes && c.maxBytes != 0 {
		c.RemoveOdlest()
	}
}

//查找指定key的value
func (c *Cache) Get(key string) (val Value, ok bool) {
	if el, ok := c.cache[key]; ok {
		//将该节点移动到队列末尾
		c.dList.MoveToFront(el)
		//类型断言
		kv := el.Value.(*entry)
		return kv.value, true
	}
	return
}

//获取插入了多少条数据
func (c *Cache) Len() int {
	return c.dList.Len()
}
