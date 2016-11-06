package rcf

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkAppend(b *testing.B) {
	type Foo struct {
		Foo int
		Bar int
		Baz int
	}
	f, err := New(filepath.Join(os.TempDir(), fmt.Sprintf("rcf-test-%d", rand.Int63())), func(i int) interface{} {
		if i == 0 {
			return &struct {
				Foo []int
				Bar []int
				Baz []int
			}{}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = f.Append([]Foo{
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
		}, true)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkIter(b *testing.B) {
	type Foo struct {
		Foo int
		Bar int
		Baz int
	}
	f, err := New(filepath.Join(os.TempDir(), fmt.Sprintf("rcf-test-%d", rand.Int63())), func(i int) interface{} {
		if i == 0 {
			return &struct {
				Foo []int
				Bar []int
				Baz []int
			}{}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	for i := 0; i < 512; i++ {
		err = f.Append([]Foo{
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
			{1, 2, 3},
		}, true)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Iter([]string{"Foo", "Bar"}, func(cols ...interface{}) bool {
			return true
		})
	}
}
