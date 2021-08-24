package yacache

import (
	"fmt"
	"log"
	"net/http"
	"strings"
)

const DEFUALT_BASE_PATH = "/_yacache/"

type HTTPPool struct {
	self     string
	basePath string
}

func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{self: self, basePath: DEFUALT_BASE_PATH}
}

func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s %s", r.Method, r.URL.Path)

	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if 2 != len(parts) {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	groupName, key := parts[0], parts[1]
	group := GetGroup(groupName)
	if nil == group {
		http.Error(w, "No such group: "+groupName, http.StatusNotFound)
		return
	}

	view, err := group.Get(key)
	if nil != err {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(view.ByteSlice())
}
