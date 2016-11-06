package rcf

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/golang/snappy"
	"io"
	"os"
	"reflect"
	"runtime"
	"sync"
)

type File struct {
	sync.Mutex
	file         *os.File
	path         string
	colSets      [][]string
	colSetsFn    func(int) interface{}
	validateOnce sync.Once
}

func (f *File) Sync() error {
	f.Lock()
	defer f.Unlock()
	return f.file.Sync()
}

func (f *File) Close() error {
	return f.file.Close()
}

func New(path string, colSetsFn func(int) interface{}) (*File, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, makeErr(err, "open file")
	}
	n := 0
	colSets := [][]string{}
	for {
		v := colSetsFn(n)
		if v == nil {
			break
		}
		//gob.Register(v)
		t := reflect.TypeOf(v).Elem()
		set := []string{}
		for i, max := 0, t.NumField(); i < max; i++ {
			set = append(set, t.Field(i).Name)
		}
		colSets = append(colSets, set)
		n++
	}
	ret := &File{
		file:      file,
		path:      path,
		colSets:   colSets,
		colSetsFn: colSetsFn,
	}
	return ret, nil
}

func (f *File) validate() (err error) {
	f.validateOnce.Do(func() {
	read:
		// read number of sets
		var numSets uint8
		err = binary.Read(f.file, binary.LittleEndian, &numSets)
		if err == io.EOF { // no more
			return
		}
		if err != nil {
			err = makeErr(err, "read number of column sets")
			return
		}
		// read meta length
		var sum, l uint32
		err = binary.Read(f.file, binary.LittleEndian, &l)
		if err != nil {
			err = makeErr(err, "read meta length")
			return
		}
		sum += l
		// read sets length
		for i, max := 0, int(numSets); i < max; i++ {
			err = binary.Read(f.file, binary.LittleEndian, &l)
			if err != nil {
				err = makeErr(err, "read column set length")
				return
			}
			sum += l
		}
		_, err = f.file.Seek(int64(sum), os.SEEK_CUR)
		if err != nil {
			err = makeErr(err, "validate seek")
			return
		}
		goto read
		return
	})
	return
}

func encode(o interface{}) (bs []byte, err error) {
	buf := new(bytes.Buffer)
	w := snappy.NewWriter(buf)
	err = json.NewEncoder(w).Encode(o)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode(bs []byte, target interface{}) (err error) {
	r := snappy.NewReader(bytes.NewReader(bs))
	return json.NewDecoder(r).Decode(target)
}

func (f *File) Append(rows, meta interface{}) error {
	f.validate()
	// encode meta
	metaBin, err := encode(meta)
	if err != nil {
		return makeErr(err, "encode meta")
	}
	// column slices
	rowsValue := reflect.ValueOf(rows)
	if rowsValue.Type().Kind() != reflect.Slice {
		return makeErr(nil, "rows is not slice")
	}
	columns := make(map[string]reflect.Value)
	for i, l := 0, rowsValue.Len(); i < l; i++ {
		row := rowsValue.Index(i)
		// make colums slices
		if i == 0 {
			for _, set := range f.colSets {
				for _, col := range set {
					if _, ok := columns[col]; ok {
						continue
					}
					columns[col] = reflect.MakeSlice(reflect.SliceOf(row.FieldByName(col).Type()), 0, 0)
				}
			}
		}
		// append colum values
		for name, col := range columns {
			columns[name] = reflect.Append(col, row.FieldByName(name))
		}
	}
	// column sets
	var bins [][]byte
	for n, set := range f.colSets {
		var v interface{} = f.colSetsFn(n)
		s := reflect.ValueOf(v)
		if s.Kind() == reflect.Ptr {
			s = s.Elem()
		}
		for _, col := range set {
			field := s.FieldByName(col)
			if !field.IsValid() {
				return makeErr(nil, fmt.Sprintf("no %s field in colun set %d", col, n))
			}
			column := columns[col]
			if column.IsValid() { // if len(rows) == 0, this would be a nil slice
				field.Set(column)
			}
		}
		//t0 := time.Now()
		bin, err := encode(&v)
		if err != nil {
			return makeErr(err, "encode column set")
		}
		//pt("%d_%v ", len(bin), time.Now().Sub(t0))
		bins = append(bins, bin)
	}
	//pt("\n")
	// write header
	if len(bins) > 255 {
		return makeErr(nil, "more than 255 column sets")
	}
	f.Lock()
	defer f.Unlock()
	err = binary.Write(f.file, binary.LittleEndian, uint8(len(bins)))
	if err != nil {
		return makeErr(err, "write length length")
	}
	err = binary.Write(f.file, binary.LittleEndian, uint32(len(metaBin)))
	if err != nil {
		return makeErr(err, "write meta length")
	}
	for _, bin := range bins {
		err = binary.Write(f.file, binary.LittleEndian, uint32(len(bin)))
		if err != nil {
			return makeErr(err, "write column set length")
		}
	}
	// write encoded
	_, err = f.file.Write(metaBin)
	if err != nil {
		return makeErr(err, "write meta")
	}
	for _, bin := range bins {
		_, err = f.file.Write(bin)
		if err != nil {
			return makeErr(err, "write column set")
		}
	}
	return nil
}

func (f *File) IterMetas(fn interface{}) error {
	f.Sync()
	file, err := os.Open(f.path)
	if err != nil {
		return makeErr(err, "open file")
	}
	defer file.Close()
	fnValue := reflect.ValueOf(fn)
	fnType := fnValue.Type()
	metaType := fnType.In(0)
	decodeValue := reflect.ValueOf(decode)
	meta := reflect.New(metaType)
read:
	// read number of sets
	var numSets uint8
	err = binary.Read(file, binary.LittleEndian, &numSets)
	if err == io.EOF { // no more
		return nil
	}
	if err != nil {
		return makeErr(err, "read number of column sets")
	}
	// read meta length
	var metaLength uint32
	err = binary.Read(file, binary.LittleEndian, &metaLength)
	if err != nil {
		return makeErr(err, "read meta length")
	}
	// read sets length
	var sum, l uint32
	for i, max := 0, int(numSets); i < max; i++ {
		err = binary.Read(file, binary.LittleEndian, &l)
		if err != nil {
			return makeErr(err, "read column set length")
		}
		sum += l
	}
	// read meta
	bs := make([]byte, metaLength)
	_, err = io.ReadFull(file, bs)
	if err != nil {
		return makeErr(err, "read meta")
	}
	// decode meta
	ret := decodeValue.Call([]reflect.Value{
		reflect.ValueOf(bs),
		meta,
	})
	if e := ret[0].Interface(); e != nil {
		return makeErr(e.(error), "decode meta")
	}
	if !fnValue.Call([]reflect.Value{meta.Elem()})[0].Bool() {
		return nil
	}
	// skip sets
	_, err = file.Seek(int64(sum), os.SEEK_CUR)
	if err != nil {
		return makeErr(err, "skip column sets")
	}
	goto read
	return nil
}

func (f *File) Iter(cols []string, cb func(columns ...interface{}) bool) error {
	f.Sync()
	file, err := os.Open(f.path)
	if err != nil {
		return makeErr(err, "open file")
	}
	defer file.Close()

	// determine which set to decode and which column to collect
	toCollect := make([][]bool, 0)
	for _, set := range f.colSets {
		c := []bool{}
		for _, col := range set {
			in := false
			for _, column := range cols {
				if column == col {
					in = true
					break
				}
			}
			c = append(c, in)
		}
		toCollect = append(toCollect, c)
	}
	toDecode := make([]bool, len(toCollect))
	for n, c := range toCollect {
	loop:
		for _, b := range c {
			if b {
				toDecode[n] = true
				break loop
			}
		}
	}

	done := make(chan struct{})
	setErr := func(e error) {
		err = e
	}

	// read bytes
	bins := make(chan [][]byte)
	go func() {
	loop:
		for {
			// read number of sets
			var numSets uint8
			err := binary.Read(file, binary.LittleEndian, &numSets)
			if err == io.EOF { // no more
				break loop
			}
			if err != nil {
				setErr(makeErr(err, "read number of column sets"))
				break loop
			}
			// read meta length
			var metaLength uint32
			err = binary.Read(file, binary.LittleEndian, &metaLength)
			if err != nil {
				setErr(makeErr(err, "read meta length"))
				break loop
			}
			// read sets length
			var lens []uint32
			var l uint32
			for i, max := 0, int(numSets); i < max; i++ {
				err = binary.Read(file, binary.LittleEndian, &l)
				if err != nil {
					setErr(makeErr(err, "read column set length"))
					break loop
				}
				lens = append(lens, l)
			}
			// skip meta
			_, err = file.Seek(int64(metaLength), os.SEEK_CUR)
			if err != nil {
				setErr(makeErr(err, "skip meta"))
				break loop
			}
			// read bytes
			var bss [][]byte
			for n, l := range lens {
				if toDecode[n] { // decode
					bs := make([]byte, l)
					_, err = io.ReadFull(file, bs)
					if err != nil {
						setErr(makeErr(err, "read column set"))
						break loop
					}
					bss = append(bss, bs)
				} else { // skip
					_, err = file.Seek(int64(l), os.SEEK_CUR)
					if err != nil {
						setErr(makeErr(err, "skip column set"))
						break loop
					}
					bss = append(bss, nil)
				}
			}
			select {
			case bins <- bss:
			case <-done:
				break loop
			}
		}
		close(bins)
	}()

	columnsChan := make(chan []interface{})
	ncpu := runtime.NumCPU()
	wg := new(sync.WaitGroup)
	wg.Add(ncpu)
	for i := 0; i < ncpu; i++ {
		go func() {
			defer wg.Done()
		loop:
			for bss := range bins {
				var columns []interface{}
				for n, bs := range bss {
					if bs == nil {
						continue
					}
					s := f.colSetsFn(n)
					err := decode(bs, &s)
					if err != nil {
						setErr(makeErr(err, "decode column set"))
						break loop
					}
					sValue := reflect.ValueOf(s).Elem()
					for nfield, b := range toCollect[n] {
						if b {
							columns = append(columns, sValue.Field(nfield).Interface())
						}
					}
				}
				select {
				case columnsChan <- columns:
				case <-done:
					break loop
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(columnsChan)
	}()

	// call
	for columns := range columnsChan {
		if !cb(columns...) {
			close(done)
			return err
		}
	}

	return err
}
