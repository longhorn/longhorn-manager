/*
This file is copied then modified from
    https://github.com/kubernetes-csi/csi-driver-host-path/blob/master/pkg/hostpath/server.go
*/

/*
Copyright 2019 The Kubernetes Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package csi

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/protosanitizer"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func NewNonBlockingGRPCServer() *NonBlockingGRPCServer {
	return &NonBlockingGRPCServer{}
}

type NonBlockingGRPCServer struct {
	wg     sync.WaitGroup
	server *grpc.Server
}

func (s *NonBlockingGRPCServer) Start(endpoint string, ids csi.IdentityServer, cs csi.ControllerServer, ns csi.NodeServer) {

	s.wg.Add(1)

	go s.serve(endpoint, ids, cs, ns)

	return
}

func (s *NonBlockingGRPCServer) Wait() {
	s.wg.Wait()
}

func (s *NonBlockingGRPCServer) Stop() {
	s.server.GracefulStop()
}

func (s *NonBlockingGRPCServer) ForceStop() {
	s.server.Stop()
}

func (s *NonBlockingGRPCServer) serve(endpoint string, ids csi.IdentityServer, cs csi.ControllerServer, ns csi.NodeServer) {

	proto, addr, err := parseEndpoint(endpoint)
	if err != nil {
		logrus.Fatal(err.Error())
	}

	if proto == "unix" {
		addr = "/" + addr
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			logrus.Fatalf("Failed to remove %s, error: %s", addr, err.Error())
		}
	}

	listener, err := net.Listen(proto, addr)
	if err != nil {
		logrus.Fatalf("Failed to listen: %v", err)
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(logGRPC),
	}
	server := grpc.NewServer(opts...)
	s.server = server

	if ids != nil {
		csi.RegisterIdentityServer(server, ids)
	}
	if cs != nil {
		csi.RegisterControllerServer(server, cs)
	}
	if ns != nil {
		csi.RegisterNodeServer(server, ns)
	}

	logrus.Infof("Listening for connections on address: %#v", listener.Addr())

	server.Serve(listener)

}

func parseEndpoint(ep string) (string, string, error) {
	if strings.HasPrefix(strings.ToLower(ep), "unix://") || strings.HasPrefix(strings.ToLower(ep), "tcp://") {
		s := strings.SplitN(ep, "://", 2)
		if s[1] != "" {
			return s[0], s[1], nil
		}
	}
	return "", "", fmt.Errorf("Invalid endpoint: %v", ep)
}

func logGRPC(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	log := logrus.StandardLogger()
	logLevel := logrus.InfoLevel
	switch info.FullMethod {
	case "/csi.v1.Node/NodeGetCapabilities", "/csi.v1.Node/NodeGetVolumeStats":
		logLevel = logrus.TraceLevel
	default:
		logLevel = logrus.InfoLevel
	}

	log.Logf(logLevel, "GRPC call: %s request: %+v", info.FullMethod, protosanitizer.StripSecrets(req))
	resp, err := handler(ctx, req)
	if err != nil {
		if logLevel == logrus.TraceLevel {
			log.Errorf("GRPC call: %s request: %+v failed with error: %v", info.FullMethod, protosanitizer.StripSecrets(req), err)
		} else {
			log.Errorf("GRPC error: %v", err)
		}
	} else {
		log.Logf(logLevel, "GRPC response: %+v", protosanitizer.StripSecrets(resp))
	}
	return resp, err
}
