package rcf

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	var seed int64
	binary.Read(crand.Reader, binary.LittleEndian, &seed)
	rand.Seed(seed)
	os.Exit(m.Run())
}

func TestBasics(t *testing.T) {
	type Foo struct {
		Foo  int
		Bar  string
		Baz  bool
		Qux  []int
		Quux map[string]string
	}

	path := filepath.Join(os.TempDir(), fmt.Sprintf("rcf-test-%d", rand.Int63()))
	var f *File
	var err error

	open := func() {
		f, err = New(path, [][]string{
			{"Foo"},
			{"Bar", "Baz"},
			{"Qux", "Quux"},
		}, func(i int) (ret interface{}) {
			switch i {
			case 0:
				ret = &struct {
					Foo []int
				}{}
			case 1:
				ret = &struct {
					Bar []string
					Baz []bool
				}{}
			case 2:
				ret = &struct {
					Qux  [][]int
					Quux []map[string]string
				}{}
			}
			return
		})

		if err != nil {
			t.Fatalf("new: %v", err)
		}
	}
	open()

	foos := []Foo{
		{1, "A", true, []int{1, 2, 3}, map[string]string{"A": "a"}},
		{2, "B", true, []int{2, 3, 4}, map[string]string{"B": "b"}},
		{3, "C", true, []int{3, 4, 5}, map[string]string{"C": "c"}},
		{4, "D", true, []int{4, 5, 6}, map[string]string{"D": "d"}},
	}
	metas := []string{
		"first",
		"second",
		"third",
	}
	for _, meta := range metas {
		err = f.Append(foos, meta)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	f.Sync()

	n := 0
	err = f.IterMetas(func(meta interface{}) bool {
		if meta, ok := meta.(string); !ok {
			t.Fatal("meta decode error")
		} else {
			if meta != metas[n] {
				t.Fatal("meta no match")
			}
		}
		n++
		return true
	})
	if err != nil {
		t.Fatalf("itermeta: %v", err)
	}
	if n != len(metas) {
		t.Fatalf("itermeta")
	}

	n = 0
	err = f.IterRows([]string{"Foo"}, func(i int) bool {
		n++
		return true
	})
	if n != len(foos)*len(metas) {
		t.Fatal("iterrows")
	}

	f.Close()

	open() // reopen
	n = 0
	err = f.IterRows([]string{"Foo"}, func(i int) bool {
		n++
		return true
	})
	if n != len(foos)*len(metas) {
		t.Fatal("iterrows")
	}

	for _, meta := range metas {
		err = f.Append(foos, meta)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	n = 0
	err = f.IterRows([]string{"Foo"}, func(i int) bool {
		n++
		return true
	})
	if n != len(foos)*len(metas)*2 {
		t.Fatal("iterrows")
	}
}
