package fake

import (
	"time"
)

type Executor struct{}

func (nsexec *Executor) Execute([]string, string, []string, time.Duration) (string, error) {
	return "output", nil
}

func (nsexec *Executor) ExecuteWithTimeout([]string, string, []string, time.Duration) (string, error) {
	return "output", nil
}

func (nsexec *Executor) ExecuteWithoutTimeout([]string, string, []string, time.Duration) (string, error) {
	return "output", nil
}

func (nsexec *Executor) ExecuteWithStdin(string, []string, string, time.Duration) (string, error) {
	return "output", nil
}

func (nsexec *Executor) ExecuteWithStdinPipe(string, []string, string, time.Duration) (string, error) {
	return "output", nil
}

type Joiner struct {
	MockDelay  time.Duration
	MockResult interface{}
	MockError  error
}

func (nsjoin *Joiner) Run(fn func() (interface{}, error)) (interface{}, error) {
	time.Sleep(nsjoin.MockDelay)
	return nsjoin.MockResult, nsjoin.MockError
}

func (nsjoin *Joiner) Revert() error {
	return nil
}
