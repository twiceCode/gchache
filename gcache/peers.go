package gcache

type PeerPicker interface {
	// PeerPicker的PickPeer()方法用于根据传入的key选择相应节点PeerGetter
	PickPeer(key string) (PeerGetter, bool)
}

type PeerGetter interface {
	// 接口PeerGetter的Get()方法用于从对应group查找缓存值PeerGetterHTTP客户端
	Get(group string, key string) ([]byte, error)
}
