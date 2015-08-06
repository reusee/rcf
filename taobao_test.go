package rcf

import (
	"fmt"
	"math/big"
	"testing"
	"time"
)

func TestTaobaoData(t *testing.T) {
	file, err := New("data.rcf",
		func(i int) interface{} {
			switch i {
			case 0:
				return &struct {
					Nid []int
				}{}
			case 1:
				return &struct {
					Category []int
					Price    []*big.Rat
					Sales    []int
					Seller   []int
				}{}
			case 2:
				return &struct {
					Title    []string
					Location []string
				}{}
			case 3:
				return &struct {
					Comments          []int
					SellerEncryptedId []string
					SellerName        []string
					SellerLevels      [][]uint8
					SellerIsTmall     []bool
					SellerCredit      []int
				}{}
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	t0 := time.Now()
	for i := 0; i < 1; i++ {
		file.IterRows([]string{"Nid", "Category", "Title"}, func(nid int, cat int, title string) bool {
			return true
		})
	}
	fmt.Printf("%v\n", time.Now().Sub(t0))
}
