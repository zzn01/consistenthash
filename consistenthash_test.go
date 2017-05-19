package consistenthash

import (
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"testing"
)

func reverse(s []string) []string {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}

func TestBase(t *testing.T) {
	c := New(10, func(s string) uint32 {
		l := strings.Split(s, "#")
		s = strings.Join(reverse(l), "")

		n, err := strconv.Atoi(s)
		if err != nil {
			panic(err)
		}

		return uint32(n)
	})

	//1: 1 11 21 31 41 51
	//5: 5 15 25 35 45 55
	cases := []struct {
		node string
		key  string
	}{
		{"1", "1"},
		{"1", "39"},
		{"5", "23"},
		{"5", "34"},
	}

	_, err := c.GetNode("")
	if err != ErrInvalidKey {
		t.Errorf("should return ErrInvalidKey")
		return
	}
	_, err = c.GetNode("1")
	if err != ErrEmptyNode {
		t.Errorf("should return ErrEmptyNode")
		return
	}

	c.AddNode("1", "5")
	for _, v := range cases {
		n, _ := c.GetNode(v.key)
		if v.node != n {
			t.Errorf("getNode(%s) should return %s but not %s", v.key, v.node, n)
		}
	}
	c.RemoveNode("1")
	for _, v := range cases {
		n, _ := c.GetNode(v.key)
		if "5" != n {
			t.Errorf("getNode(%s) should return %s but not %s", v.key, "100", n)
		}
	}
}

func h(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}

func TestBase2(_ *testing.T) {
	c := New(1024, h)
	c.AddNode("127.0.0.1:1110", "127.0.0.1:1111", "127.0.0.1:1112", "127.0.0.1:1113")
	fmt.Println(c.GetStatistics())

	c = New(1024, nil)
	c.AddNode("127.0.0.1:1110", "127.0.0.1:1111", "127.0.0.1:1112", "127.0.0.1:1113")
	fmt.Println(c.GetStatistics())
}
