package rcf

import "fmt"

type Err struct {
	Pkg  string
	Info string
	Err  error
}

func (e *Err) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("%s: %s", e.Pkg, e.Info)
	}
	return fmt.Sprintf("%s: %s\n%v", e.Pkg, e.Info, e.Err)
}

func makeErr(err error, info string) *Err {
	return &Err{
		Pkg:  `rcf`,
		Info: info,
		Err:  err,
	}
}
