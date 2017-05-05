package daemon

import (
	"github.com/Sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

func WaitForExit() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)
	s := <-c
	logrus.Infof("Received signal '%s'", s)
	return nil
}
