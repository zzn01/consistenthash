# consistenthash
a thread-safe golang consistent hash implementation.

usage:

```golang
package main

import (
	"fmt"
	"hash/crc32"

	"github.com/zzn01/consistenthash"
)

func h(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

func main() {
	c := consistenthash.New(1024, h)
	c.AddNode("127.0.0.1:1110", "127.0.0.1:1111", "127.0.0.1:1112", "127.0.0.1:1113")
	node, err := c.GetNode("key1")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(node)
}
```
