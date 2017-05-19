package consistenthash

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"sync"
)

type Uint32s []uint32

func (a Uint32s) Len() int           { return len(a) }
func (a Uint32s) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Uint32s) Less(i, j int) bool { return a[i] < a[j] }

func fnvHash(s string) uint32 {
	var _fnv = fnv.New32a()
	_fnv.Write([]byte(s))
	return _fnv.Sum32()
}

type HashFunc func(string) uint32

type ConsistentHash struct {
	replica   int
	hash2Node map[uint32]string
	hashfunc  HashFunc

	sortedHash []uint32

	rwm sync.RWMutex
}

func New(replica int, f HashFunc) *ConsistentHash {
	c := &ConsistentHash{replica: replica, hash2Node: make(map[uint32]string)}
	if f == nil {
		f = fnvHash
	}
	c.hashfunc = f
	return c
}

func (c *ConsistentHash) getVirtualNode(id string, n int) string {
	return fmt.Sprintf("%s#%d", id, n)
}

func (c *ConsistentHash) AddNode(nodes ...string) {
	newNodes := make(map[uint32]string)
	for _, node := range nodes {
		for i := 0; i < c.replica; i++ {
			v := c.hashfunc(c.getVirtualNode(node, i))
			newNodes[v] = node
		}
	}

	c.rwm.Lock()
	for v, node := range newNodes {
		// NOTE:use new node if hash value is equal.
		c.hash2Node[v] = node
	}
	c.updateSortedHash()
	c.rwm.Unlock()
}

func (c *ConsistentHash) updateSortedHash() {
	c.sortedHash = c.sortedHash[:0]
	for v := range c.hash2Node {
		c.sortedHash = append(c.sortedHash, v)
	}
	sort.Sort(Uint32s(c.sortedHash))
}

func (c *ConsistentHash) RemoveNode(nodes ...string) {
	deleted := make(map[uint32]string)
	for _, node := range nodes {
		for i := 0; i < c.replica; i++ {
			v := c.hashfunc(c.getVirtualNode(node, i))
			deleted[v] = node
		}
	}

	c.rwm.Lock()
	for v, node := range deleted {
		if id, ok := c.hash2Node[v]; ok && id == node {
			delete(c.hash2Node, v)
		}
	}
	c.updateSortedHash()
	c.rwm.Unlock()
}

func (c *ConsistentHash) Size() int {
	c.rwm.RLock()
	defer c.rwm.RUnlock()

	return len(c.hash2Node)
}

var ErrEmptyNode = errors.New("no nodes")
var ErrInvalidKey = errors.New("invalid key")

func (c *ConsistentHash) GetNode(key string) (string, error) {
	if key == "" {
		return "", ErrInvalidKey
	}

	keyHash := c.hashfunc(key)

	c.rwm.RLock()
	defer c.rwm.RUnlock()

	n := len(c.sortedHash)
	if n == 0 {
		return "", ErrEmptyNode
	}

	found := sort.Search(n, func(i int) bool { return c.sortedHash[i] >= keyHash })
	if found == n {
		found = 0
	}

	return c.hash2Node[c.sortedHash[found]], nil
}

func (c *ConsistentHash) GetStatistics() map[string]float64 {
	sum := make(map[string]uint32)
	c.rwm.RLock()
	for i := range c.sortedHash {
		d := uint32(0)
		if i == 0 {
			d = c.sortedHash[i] + math.MaxUint32 - c.sortedHash[len(c.sortedHash)-1]
		} else {
			d = c.sortedHash[i] - c.sortedHash[i-1]
		}
		sum[c.hash2Node[c.sortedHash[i]]] += d
	}
	c.rwm.RUnlock()

	ret := make(map[string]float64, len(sum))
	for node, d := range sum {
		ret[node] = float64(d) / float64(math.MaxUint32)
	}
	return ret
}
