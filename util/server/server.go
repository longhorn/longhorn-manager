package server

import (
	"github.com/docker/go-connections/sockets"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"net/http"
	"os"
	"path/filepath"
)

type UnixServer struct {
	sockFile string
}

func NewUnixServer(sockFile string) *UnixServer {
	return &UnixServer{sockFile}
}

func (s *UnixServer) Serve(handler http.Handler) {
	if err := os.MkdirAll(filepath.Dir(s.sockFile), 0755); err != nil {
		logrus.Fatalf("%+v", errors.Wrapf(err, "error creating parent dir for '%s'", s.sockFile))
	}
	server := http.Server{
		Addr:    s.sockFile,
		Handler: handler,
	}
	listener, err := sockets.NewUnixSocket(s.sockFile, 0)
	if err != nil {
		logrus.Fatalf("Failed opening unix socket '%s'", s.sockFile)
	}
	logrus.Infof("Unix socket server listening at %v", s.sockFile)
	err = server.Serve(listener)
	logrus.Fatalf("server.Serve returned error: %+v", errors.Wrap(err, "http server error"))
}

type TCPServer struct {
	addr string
}

func NewTCPServer(addrPort string) *TCPServer {
	return &TCPServer{addrPort}
}

func (s *TCPServer) Serve(handler http.Handler) {
	logrus.Infof("TCP server listening at %v", s.addr)
	err := http.ListenAndServe(s.addr, handler)
	logrus.Fatalf("http.ListenAndServe returned error: %+v", errors.Wrap(err, "http server error"))
}
