package gcache

import (
	"fmt"
	"hmj/gcache/consistenthash"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

const (
	defaultBasePath = "/gcache/"
	defaultReplicas = 50
)

type HTTPPool struct {
	self        string // 记录自己的地址，格式为  主机/IP-端口
	basePath    string // 请求地址的默认前缀
	mu          sync.Mutex
	peers       *consistenthash.Map    // 根据具体的key选择节点
	httpGetters map[string]*httpGetter // 映射远程节点与对应的 httpGetter
}

func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:        self,
		basePath:    defaultBasePath,
		httpGetters: make(map[string]*httpGetter),
	}
}

//对不同服务进行区分
func (p *HTTPPool) Log(format string, v ...interface{}) {
	logrus.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

//实现Handler接口
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//检查有没有指定前缀
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s %s", r.Method, r.URL.Path)
	//约定请求格式为： /<basepath>/<groupname>/<key>
	parts := strings.Split(r.URL.Path[len(p.basePath):], "/")
	//如果切分的片段长度不止为2，那么说明不符合要求
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName, key := parts[0], parts[1]

	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(view.ByteSlice())
}

// 实例化了一致性哈希算法并且添加了传入的节点
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers == nil {
		p.peers = consistenthash.New(defaultReplicas, nil)
	}
	p.peers.Add(peers...)
	// 每一个节点创建了一个HTTP客户端 httpGetter
	for _, peer := range peers {
		// 这个地方的URL可能有问题
		p.httpGetters[peer] = &httpGetter{baseURL: peer + p.basePath}
	}
}

// 实现PeerPick接口
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// PickerPeer()包装了一致性哈希算法的Get()方法，
	// 根据具体的key,选择节点,返回节点对应的HTTP客户端。
	if peer := p.peers.Get(key); peer != "" && p.self != peer {
		p.Log("Pick peer %s", peer)
		return p.httpGetters[peer], true
	}
	return nil, false
}

type httpGetter struct {
	baseURL string
}

// 实现PeerGetter接口
func (h *httpGetter) Get(group string, key string) ([]byte, error) {
	url := fmt.Sprintf("%v%v/%v",
		h.baseURL,
		url.QueryEscape(group),
		url.QueryEscape(key),
	)
	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %v", err)
	}

	return bytes, nil
}
