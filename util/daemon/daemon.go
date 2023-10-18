package daemon

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/sirupsen/logrus"
)

func WaitForExit() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	s := <-c
	logrus.Infof("Received signal '%s'", s)
	return nil
}
