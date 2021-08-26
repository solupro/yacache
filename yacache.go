package yacache

import (
	"fmt"
	"log"
	"sync"
	"yacache/singleflight"
	pb "yacache/yacachepb"
)

type Getter interface {
	Get(key string) ([]byte, error)
}

type GetterFunc func(key string) ([]byte, error)

func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

type Group struct {
	name      string
	getter    Getter
	mainCache cache
	peers     PeerPicker
	loader    *singleflight.Group
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if nil == getter {
		panic("nil Getter")
	}

	mu.Lock()
	defer mu.Unlock()
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
	mu.RLock()
	g := groups[name]
	mu.RUnlock()

	return g
}

func (g *Group) Get(key string) (ByteView, error) {
	if "" == key {
		return ByteView{}, fmt.Errorf("key is requeired")
	}

	if v, ok := g.mainCache.get(key); ok {
		log.Println("[YaCache] hit")
		return v, nil
	}

	return g.load(key)
}

func (g *Group) load(key string) (value ByteView, err error) {
	view, err := g.loader.Do(key, func() (interface{}, error) {
		if nil != g.peers {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err = g.getFromPeer(key, peer); nil == err {
					return value, err
				}

				log.Println("[YaCache] Failed to get from peer", err)
			}
		}

		return g.getLocal(key)
	})

	if nil != err {
		return ByteView{}, err
	}

	return view.(ByteView), nil
}

func (g *Group) getFromPeer(key string, peer PeerGetter) (ByteView, error) {

	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}

	err := peer.Get(req, res)
	if nil != err {
		return ByteView{}, err
	}

	return ByteView{b: res.Value}, nil
}

func (g *Group) getLocal(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if nil != err {
		return ByteView{}, err
	}

	value := ByteView{cloneBytes(bytes)}
	g.populateCache(key, value)

	return value, nil
}

func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

func (g *Group) RegisterPeers(peers PeerPicker) {
	if nil != g.peers {
		panic("already exists peers")
	}

	g.peers = peers
}
