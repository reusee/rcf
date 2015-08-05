package rcf

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"io"
	"os"
	"reflect"
	"sync"

	"github.com/ugorji/go/codec"
)

type File struct {
	sync.Mutex
	file         *os.File
	path         string
	colSets      [][]string
	colSetsFn    func(int) interface{}
	validateOnce sync.Once
}

var codecHandle = new(codec.CborHandle)

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
	w := gzip.NewWriter(buf)
	err = codec.NewEncoder(w, codecHandle).Encode(o)
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
	r, err := gzip.NewReader(bytes.NewReader(bs))
	if err != nil {
		return err
	}
	return codec.NewDecoder(r, codecHandle).Decode(target)
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
		s := reflect.ValueOf(f.colSetsFn(n))
		for _, col := range set {
			s.Elem().FieldByName(col).Set(columns[col])
		}
		bin, err := encode(s)
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

func (f *File) IterMetas(fn func(meta interface{}) bool) error {
	f.Sync()
	file, err := os.Open(f.path)
	if err != nil {
		return makeErr(err, "open file")
	}
	defer file.Close()
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
	var meta interface{}
	err = decode(bs, &meta)
	if err != nil {
		return makeErr(err, "decode meta")
	}
	if !fn(meta) {
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

func (f *File) IterRows(cols []string, cb interface{}) error {
	f.Sync()
	file, err := os.Open(f.path)
	if err != nil {
		return makeErr(err, "open file")
	}
	defer file.Close()
	cbValue := reflect.ValueOf(cb)
	// determine which set to decode
	toDecode := make([]bool, len(f.colSets))
	for n, set := range f.colSets {
	loop:
		for _, col := range set {
			for _, c := range cols {
				if c == col {
					toDecode[n] = true
					break loop
				}
			}
		}
	}
	// read number of sets
read:
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
	var lens []uint32
	var l uint32
	for i, max := 0, int(numSets); i < max; i++ {
		err = binary.Read(file, binary.LittleEndian, &l)
		if err != nil {
			return makeErr(err, "read column set length")
		}
		lens = append(lens, l)
	}
	// skip meta
	_, err = file.Seek(int64(metaLength), os.SEEK_CUR)
	if err != nil {
		return makeErr(err, "skip meta")
	}
	// decode sets
	columns := make(map[string]reflect.Value)
	for n, l := range lens {
		if toDecode[n] { // decode
			bs := make([]byte, l)
			_, err = io.ReadFull(file, bs)
			if err != nil {
				return makeErr(err, "read column set")
			}
			s := f.colSetsFn(n)
			err = decode(bs, &s)
			if err != nil {
				return makeErr(err, "decode column set")
			}
			sValue := reflect.ValueOf(s).Elem()
			sType := sValue.Type()
			for i, max := 0, sValue.NumField(); i < max; i++ {
				columns[sType.Field(i).Name] = sValue.Field(i)
			}
		} else { // skip
			_, err = file.Seek(int64(l), os.SEEK_CUR)
			if err != nil {
				return makeErr(err, "skip column set")
			}
		}
	}
	// call
	for i, max := 0, columns[cols[0]].Len(); i < max; i++ {
		var args []reflect.Value
		for _, col := range cols {
			args = append(args, columns[col].Index(i))
		}
		rets := cbValue.Call(args)
		if !rets[0].Bool() {
			return nil
		}
	}
	goto read
	return nil
}
