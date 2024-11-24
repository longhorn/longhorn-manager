// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.3.0
// - protoc             v4.24.3
// source: smrpc/smrpc.proto

package smrpc

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

const (
	ShareManagerService_FilesystemResize_FullMethodName = "/ShareManagerService/FilesystemResize"
	ShareManagerService_FilesystemTrim_FullMethodName   = "/ShareManagerService/FilesystemTrim"
	ShareManagerService_Unmount_FullMethodName          = "/ShareManagerService/Unmount"
	ShareManagerService_Mount_FullMethodName            = "/ShareManagerService/Mount"
)

// ShareManagerServiceClient is the client API for ShareManagerService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ShareManagerServiceClient interface {
	FilesystemResize(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error)
	FilesystemTrim(ctx context.Context, in *FilesystemTrimRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	Unmount(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error)
	Mount(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error)
}

type shareManagerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewShareManagerServiceClient(cc grpc.ClientConnInterface) ShareManagerServiceClient {
	return &shareManagerServiceClient{cc}
}

func (c *shareManagerServiceClient) FilesystemResize(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, ShareManagerService_FilesystemResize_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *shareManagerServiceClient) FilesystemTrim(ctx context.Context, in *FilesystemTrimRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, ShareManagerService_FilesystemTrim_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *shareManagerServiceClient) Unmount(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, ShareManagerService_Unmount_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *shareManagerServiceClient) Mount(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, ShareManagerService_Mount_FullMethodName, in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ShareManagerServiceServer is the server API for ShareManagerService service.
// All implementations must embed UnimplementedShareManagerServiceServer
// for forward compatibility
type ShareManagerServiceServer interface {
	FilesystemResize(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
	FilesystemTrim(context.Context, *FilesystemTrimRequest) (*emptypb.Empty, error)
	Unmount(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
	Mount(context.Context, *emptypb.Empty) (*emptypb.Empty, error)
	mustEmbedUnimplementedShareManagerServiceServer()
}

// UnimplementedShareManagerServiceServer must be embedded to have forward compatible implementations.
type UnimplementedShareManagerServiceServer struct {
}

func (UnimplementedShareManagerServiceServer) FilesystemResize(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FilesystemResize not implemented")
}
func (UnimplementedShareManagerServiceServer) FilesystemTrim(context.Context, *FilesystemTrimRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FilesystemTrim not implemented")
}
func (UnimplementedShareManagerServiceServer) Unmount(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Unmount not implemented")
}
func (UnimplementedShareManagerServiceServer) Mount(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Mount not implemented")
}
func (UnimplementedShareManagerServiceServer) mustEmbedUnimplementedShareManagerServiceServer() {}

// UnsafeShareManagerServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ShareManagerServiceServer will
// result in compilation errors.
type UnsafeShareManagerServiceServer interface {
	mustEmbedUnimplementedShareManagerServiceServer()
}

func RegisterShareManagerServiceServer(s grpc.ServiceRegistrar, srv ShareManagerServiceServer) {
	s.RegisterService(&ShareManagerService_ServiceDesc, srv)
}

func _ShareManagerService_FilesystemResize_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShareManagerServiceServer).FilesystemResize(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ShareManagerService_FilesystemResize_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShareManagerServiceServer).FilesystemResize(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _ShareManagerService_FilesystemTrim_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FilesystemTrimRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShareManagerServiceServer).FilesystemTrim(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ShareManagerService_FilesystemTrim_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShareManagerServiceServer).FilesystemTrim(ctx, req.(*FilesystemTrimRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ShareManagerService_Unmount_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShareManagerServiceServer).Unmount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ShareManagerService_Unmount_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShareManagerServiceServer).Unmount(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _ShareManagerService_Mount_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ShareManagerServiceServer).Mount(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ShareManagerService_Mount_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ShareManagerServiceServer).Mount(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

// ShareManagerService_ServiceDesc is the grpc.ServiceDesc for ShareManagerService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ShareManagerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "ShareManagerService",
	HandlerType: (*ShareManagerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "FilesystemResize",
			Handler:    _ShareManagerService_FilesystemResize_Handler,
		},
		{
			MethodName: "FilesystemTrim",
			Handler:    _ShareManagerService_FilesystemTrim_Handler,
		},
		{
			MethodName: "Unmount",
			Handler:    _ShareManagerService_Unmount_Handler,
		},
		{
			MethodName: "Mount",
			Handler:    _ShareManagerService_Mount_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "smrpc/smrpc.proto",
}
