package rcf

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/golang/snappy"
	"github.com/reusee/pipeline"
	"io"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
)

const (
	_COMPRESS_NONE = iota
	_COMPRESS_SNAPPY
)

type File struct {
	sync.Mutex
	file           *os.File
	path           string
	colSets        [][]string
	colSetsFn      func(int) interface{}
	validateOnce   sync.Once
	compressMethod int
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
		gob.Register(v)
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
	parts := strings.Split(path, ".")
	for _, part := range parts {
		if part == "snappy" {
			ret.compressMethod = _COMPRESS_SNAPPY
		}
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

func (f *File) encode(o interface{}) (bs []byte, err error) {
	buf := new(bytes.Buffer)
	if f.compressMethod == _COMPRESS_SNAPPY {
		w := snappy.NewWriter(buf)
		err = gob.NewEncoder(w).Encode(o)
		if err != nil {
			return nil, err
		}
		err = w.Close()
		if err != nil {
			return nil, err
		}
	} else {
		err = gob.NewEncoder(buf).Encode(o)
		if err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (f *File) decode(bs []byte, target interface{}) (err error) {
	var r io.Reader
	if f.compressMethod == _COMPRESS_SNAPPY {
		r = snappy.NewReader(bytes.NewReader(bs))
	} else {
		r = bytes.NewReader(bs)
	}
	return gob.NewDecoder(r).Decode(target)
}

func (f *File) Append(rows, meta interface{}) error {
	f.validate()
	// encode meta
	metaBin, err := f.encode(meta)
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
		bin, err := f.encode(&v)
		if err != nil {
			return makeErr(err, "encode column set")
		}
		bins = append(bins, bin)
	}
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

	line := pipeline.NewPipeline()
	p1 := line.NewPipe(100000)
	p2 := line.NewPipe(2048)

	go func() {
		for {
			meta := reflect.New(metaType)

			// read number of sets
			var numSets uint8
			err = binary.Read(file, binary.LittleEndian, &numSets)
			if err == io.EOF { // no more
				break
			}
			if err != nil {
				line.Error(makeErr(err, "read number of column sets"))
				return
			}

			// read meta length
			var metaLength uint32
			err = binary.Read(file, binary.LittleEndian, &metaLength)
			if err != nil {
				line.Error(makeErr(err, "read meta length"))
				return
			}

			// read sets length
			var sum, l uint32
			for i, max := 0, int(numSets); i < max; i++ {
				err = binary.Read(file, binary.LittleEndian, &l)
				if err != nil {
					line.Error(makeErr(err, "read column set length"))
					return
				}
				sum += l
			}

			// read meta
			bs := make([]byte, metaLength)
			_, err = io.ReadFull(file, bs)
			if err != nil {
				line.Error(makeErr(err, "read meta"))
				return
			}
			line.Add()

			if !p1.Do(func() {
				// decode meta
				err = f.decode(bs, meta.Interface())
				if err != nil {
					line.Error(makeErr(err, "decode meta"))
					return
				}
				// callback
				if !p2.Do(func() {
					if !fnValue.Call([]reflect.Value{meta.Elem()})[0].Bool() {
						line.Close()
						return
					}
					line.Done()
				}) {
					return
				}
			}) {
				return
			}

			// skip sets
			_, err = file.Seek(int64(sum), os.SEEK_CUR)
			if err != nil {
				line.Error(makeErr(err, "skip column sets"))
				return
			}

		}
		line.Wait()
		line.Close()
	}()

	go p1.ParallelProcess(runtime.NumCPU())
	p2.Process()

	return line.Err
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
		for _, b := range c {
			if b {
				toDecode[n] = true
				break
			}
		}
	}

	line := pipeline.NewPipeline()
	p1 := line.NewPipe(30000)
	p2 := line.NewPipe(2048)

	// read bytes
	go func() {
		for {
			// read number of sets
			var numSets uint8
			err := binary.Read(file, binary.LittleEndian, &numSets)
			if err == io.EOF { // no more
				break
			}
			if err != nil {
				line.Error(makeErr(err, "read number of column sets"))
				return
			}
			// read meta length
			var metaLength uint32
			err = binary.Read(file, binary.LittleEndian, &metaLength)
			if err != nil {
				line.Error(makeErr(err, "read meta length"))
				return
			}
			// read sets length
			var lens []uint32
			var l uint32
			for i, max := 0, int(numSets); i < max; i++ {
				err = binary.Read(file, binary.LittleEndian, &l)
				if err != nil {
					line.Error(makeErr(err, "read column set length"))
					return
				}
				lens = append(lens, l)
			}
			// skip meta
			_, err = file.Seek(int64(metaLength), os.SEEK_CUR)
			if err != nil {
				line.Error(makeErr(err, "skip meta"))
				return
			}
			// read bytes
			var bss [][]byte
			for n, l := range lens {
				if toDecode[n] { // decode
					bs := make([]byte, l)
					_, err = io.ReadFull(file, bs)
					if err != nil {
						line.Error(makeErr(err, "read column set"))
						return
					}
					bss = append(bss, bs)
				} else { // skip
					_, err = file.Seek(int64(l), os.SEEK_CUR)
					if err != nil {
						line.Error(makeErr(err, "skip column set"))
						return
					}
					bss = append(bss, nil)
				}
			}

			line.Add()
			if !p1.Do(func() {
				var columns []interface{}
				for n, bs := range bss {
					if bs == nil {
						continue
					}
					s := f.colSetsFn(n)
					err := f.decode(bs, &s)
					if err != nil {
						line.Error(makeErr(err, "decode column set"))
						return
					}
					sValue := reflect.ValueOf(s).Elem()
					for nfield, b := range toCollect[n] {
						if b {
							columns = append(columns, sValue.Field(nfield).Interface())
						}
					}
				}

				if !p2.Do(func() {
					if !cb(columns...) {
						line.Close()
						return
					}
					line.Done()
				}) {
					return
				}

			}) {
				return
			}

		}
		line.Wait()
		line.Close()
	}()

	go p1.ParallelProcess(runtime.NumCPU())
	p2.Process()

	return line.Err
}

func (f *File) IterAll(metaTarget interface{}, columnsTarget interface{}, cb func() bool) error {
	f.Sync()
	file, err := os.Open(f.path)
	if err != nil {
		return makeErr(err, "open file")
	}
	defer file.Close()

	columnsToCollect := make(map[string]bool)
	t := reflect.TypeOf(columnsTarget).Elem()
	for i, l := 0, t.NumField(); i < l; i++ {
		columnsToCollect[t.Field(i).Name] = true
	}

	toDecode := make([]bool, len(f.colSets))
	for i, set := range f.colSets {
		for _, col := range set {
			if columnsToCollect[col] {
				toDecode[i] = true
			}
		}
	}

	line := pipeline.NewPipeline()
	p1 := line.NewPipe(30000)
	p2 := line.NewPipe(2048)

	columnsTargetValue := reflect.ValueOf(columnsTarget).Elem()

	go func() {
		for {

			// read number of sets
			var numSets uint8
			err := binary.Read(file, binary.LittleEndian, &numSets)
			if err == io.EOF { // no more
				break
			}
			if err != nil {
				line.Error(makeErr(err, "read number of column sets"))
				return
			}

			// read meta length
			var metaLength uint32
			err = binary.Read(file, binary.LittleEndian, &metaLength)
			if err != nil {
				line.Error(makeErr(err, "read meta length"))
				return
			}

			// read sets length
			var lens []uint32
			var l uint32
			for i, max := 0, int(numSets); i < max; i++ {
				err = binary.Read(file, binary.LittleEndian, &l)
				if err != nil {
					line.Error(makeErr(err, "read column set length"))
					return
				}
				lens = append(lens, l)
			}

			// read meta
			metaBytes := make([]byte, metaLength)
			_, err = io.ReadFull(file, metaBytes)
			if err != nil {
				line.Error(makeErr(err, "read meta"))
				return
			}

			// read bytes
			var columnBytesSlice [][]byte
			for n, l := range lens {
				if toDecode[n] { // decode
					bs := make([]byte, l)
					_, err = io.ReadFull(file, bs)
					if err != nil {
						line.Error(makeErr(err, "read column set"))
						return
					}
					columnBytesSlice = append(columnBytesSlice, bs)
				} else { // skip
					_, err = file.Seek(int64(l), os.SEEK_CUR)
					if err != nil {
						line.Error(makeErr(err, "skip column set"))
						return
					}
					columnBytesSlice = append(columnBytesSlice, nil)
				}
			}

			line.Add()
			if !p1.Do(func() {
				// decode meta
				meta := reflect.New(reflect.TypeOf(metaTarget).Elem())
				err = f.decode(metaBytes, meta.Interface())
				if err != nil {
					line.Error(makeErr(err, "decode meta"))
					return
				}

				// decode columns
				toSet := make(map[string]reflect.Value)
				for n, bs := range columnBytesSlice {
					if bs == nil {
						continue
					}
					columnSet := f.colSetsFn(n)
					err := f.decode(bs, &columnSet)
					if err != nil {
						line.Error(makeErr(err, "decode column set"))
						return
					}
					columnSetType := reflect.TypeOf(columnSet).Elem()
					columnSetValue := reflect.ValueOf(columnSet).Elem()
					for i, l := 0, columnSetType.NumField(); i < l; i++ {
						name := columnSetType.Field(i).Name
						if columnsToCollect[name] {
							toSet[name] = columnSetValue.FieldByName(name)
						}
					}
				}

				if !p2.Do(func() {
					// assign
					reflect.ValueOf(metaTarget).Elem().Set(meta.Elem())
					for name, value := range toSet {
						columnsTargetValue.FieldByName(name).Set(value)
					}
					// callback
					if !cb() {
						line.Close()
						return
					}
					//pt("%d %d\n", p1.Len(), p2.Len())
					line.Done()
				}) {
					return
				}

			}) {
				return
			}

		}
		line.Wait()
		line.Close()
	}()

	go p1.ParallelProcess(runtime.NumCPU())
	p2.Process()

	return line.Err
}
