package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// 如何选择从那个节点获取缓存
// 如何解决缓存的存放问题（也就是缓存存放到哪个节点）
// 使用hash的话，当节点的数量发生变化怎么办

// 定义了函数类型 Hash，采取依赖注入的方式，
// 允许用于替换成自定义的Hash函数，也方便测试时替换，默认为crc32.ChecksumIEEE算法
type Hash func([]byte) uint32

type Map struct {
	hash     Hash           //hash函数
	replicas int            // 虚拟节点倍数
	keys     []int          // hash环
	hashMap  map[int]string // 虚拟节点和真实节点的映射
}

func New(replicas int, f Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     f,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		// 对每一个真实节点 key，对应创建 m.replicas 个虚拟节点，
		// 虚拟节点的名称是：strconv.Itoa(i) + key，即通过添加编号的方式区分不同虚拟节点
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			// 虚拟节点指向真实节点
			m.hashMap[hash] = key
		}
	}
	// 环上的哈希值排序
	sort.Ints(m.keys)
}

func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(key)))
	index := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	// m.keys是一个环状结构，所以用取余数的方式来处理
	return m.hashMap[m.keys[index%len(m.keys)]]
}
