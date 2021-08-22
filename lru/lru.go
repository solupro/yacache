package lru

import "container/list"

type Cache struct {
	maxBytes int64
	// used
	nbytes int64
	ll     *list.List
	cache  map[string]*list.Element

	// optional and executed when an entry is purged.
	OnEvicted func(key string, value Value)
}

// Value use Len to count how many bytes it takes
type Value interface {
	Len() int
}

type entry struct {
	key   string
	value Value
}

func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

func (c *Cache) Get(key string) (value Value, ok bool) {
	if el, ok := c.cache[key]; ok {
		c.ll.MoveToFront(el)
		kv := el.Value.(*entry)
		return kv.value, true
	}

	return nil, false
}

func (c *Cache) RemoveOldest() {
	el := c.ll.Back()
	if nil != el {
		c.ll.Remove(el)
		kv := el.Value.(*entry)
		delete(c.cache, kv.key)
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())

		if nil != c.OnEvicted {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

func (c *Cache) Add(key string, value Value) {
	// 已存在更新值
	if el, ok := c.cache[key]; ok {
		c.ll.MoveToFront(el)
		kv := el.Value.(*entry)
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
	} else {
		el := c.ll.PushFront(&entry{key, value})
		c.cache[key] = el
		c.nbytes += int64(len(key)) + int64(value.Len())
	}
	for 0 != c.maxBytes && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

func (c *Cache) Len() int {
	return c.ll.Len()
}
