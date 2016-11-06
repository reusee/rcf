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
		f, err = New(path, func(i int) (ret interface{}) {
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

	t.Run("append", func(t *testing.T) {
		for _, meta := range metas {
			err = f.Append(foos, meta)
			if err != nil {
				t.Fatalf("append: %v", err)
			}
		}
	})

	f.Sync()

	t.Run("iter metas", func(t *testing.T) {
		n := 0
		err = f.IterMetas(func(meta string) bool {
			if meta != metas[n] {
				t.Fatal("meta no match")
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
	})

	t.Run("iter rows", func(t *testing.T) {
		n := 0
		err = f.Iter([]string{"Foo"}, func(cols ...interface{}) bool {
			foos := cols[0].([]int)
			if foos[0] != 1 || foos[1] != 2 || foos[2] != 3 || foos[3] != 4 {
				t.Fatal("foo value not match")
			}
			n++
			return true
		})
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		if n != len(metas) {
			t.Fatalf("iterrows")
		}
	})

	t.Run("reopen", func(t *testing.T) {
		f.Close()
		open() // reopen
		n := 0
		err = f.Iter([]string{"Foo"}, func(cols ...interface{}) bool {
			foos := cols[0].([]int)
			if foos[0] != 1 || foos[1] != 2 || foos[2] != 3 || foos[3] != 4 {
				t.Fatal("foo value not match")
			}
			n++
			return true
		})
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		if n != len(metas) {
			t.Fatal("iterrows")
		}
	})

	t.Run("append then iter", func(t *testing.T) {
		for _, meta := range metas {
			err = f.Append(foos, meta)
			if err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		n := 0
		err = f.Iter([]string{"Foo"}, func(cols ...interface{}) bool {
			foos := cols[0].([]int)
			if foos[0] != 1 || foos[1] != 2 || foos[2] != 3 || foos[3] != 4 {
				t.Fatal("foo value not match")
			}
			n++
			return true
		})
		if err != nil {
			t.Fatalf("iter: %v", err)
		}
		if n != len(metas)*2 {
			t.Fatal("iterrows")
		}
	})
}
