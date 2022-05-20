package gcache

import (
	"fmt"
	singleflight "hmj/gcache/single_flight"
	"sync"

	"github.com/sirupsen/logrus"
)

type Getter interface {
	Get(key string) ([]byte, error)
}

//接口型函数使用的意义是什么？
type GetterFunc func(string) ([]byte, error)

// GetterFunc类型实现Getter接口
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

//类似于缓存的命名空间
type Group struct {
	name      string // 命名空间的id
	getter    Getter // 缓存未命中时获取源数据的回调(callback)。
	mainCache cache  // 实现的并发缓存
	peers     PeerPicker
	loader    *singleflight.Group
}

// 缓存的获取过程
// Get 方法实现了上述所说的流程 ⑴ 和 ⑶。
// 流程 ⑴ ：从 mainCache 中查找缓存，如果存在则返回缓存值。
// 流程 ⑶ ：缓存不存在，则调用 load 方法，load 调用 getLocally（分布式场景下会调用 getFromPeer 从其他节点获取），
// getLocally 调用用户回调函数 g.getter.Get() 获取源数据，并且将源数据添加到缓存 mainCache 中（通过 populateCache 方法）

var (
	rw     sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	rw.Lock()
	defer rw.Unlock()
	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
		loader:    &singleflight.Group{},
	}
	groups[name] = g
	return g
}

func GetGroup(name string) *Group {
	rw.RLock()
	g := groups[name]
	rw.RUnlock()
	return g
}

func (g *Group) load(key string) (value ByteView, err error) {
	// g.loader.Do可以使对于同一个key的请求只会请求一次
	view, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			// 选择远程节点
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				logrus.Println("[gcache] Failed to get from peer", err)
			}
		}
		return g.getLocally(key)
	})

	if err == nil {
		return view.(ByteView), nil
	}

	return
}

// 由于远程节点没有缓存值，通过该方法设置到本地缓存
func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

// 远程节点没有获取缓存，通过回调函数从DB中获取
func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err

	}
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}

// 注册peers（也就是注册分布式节点）
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

// 从远程节点获取缓存值
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	bytes, err := peer.Get(g.name, key)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: bytes}, nil
}

// 获取缓存值
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	// 优先从本地获取
	if v, ok := g.mainCache.get(key); ok {
		logrus.Debug("[gcache] hit")
		return v, nil
	}
	return g.load(key)
}
