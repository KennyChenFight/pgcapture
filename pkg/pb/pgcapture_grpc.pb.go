// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// DBLogClient is the client API for DBLog service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type DBLogClient interface {
	Capture(ctx context.Context, opts ...grpc.CallOption) (DBLog_CaptureClient, error)
}

type dBLogClient struct {
	cc grpc.ClientConnInterface
}

func NewDBLogClient(cc grpc.ClientConnInterface) DBLogClient {
	return &dBLogClient{cc}
}

func (c *dBLogClient) Capture(ctx context.Context, opts ...grpc.CallOption) (DBLog_CaptureClient, error) {
	stream, err := c.cc.NewStream(ctx, &DBLog_ServiceDesc.Streams[0], "/pgcapture.DBLog/Capture", opts...)
	if err != nil {
		return nil, err
	}
	x := &dBLogCaptureClient{stream}
	return x, nil
}

type DBLog_CaptureClient interface {
	Send(*CaptureRequest) error
	Recv() (*CaptureMessage, error)
	grpc.ClientStream
}

type dBLogCaptureClient struct {
	grpc.ClientStream
}

func (x *dBLogCaptureClient) Send(m *CaptureRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *dBLogCaptureClient) Recv() (*CaptureMessage, error) {
	m := new(CaptureMessage)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// DBLogServer is the server API for DBLog service.
// All implementations must embed UnimplementedDBLogServer
// for forward compatibility
type DBLogServer interface {
	Capture(DBLog_CaptureServer) error
	mustEmbedUnimplementedDBLogServer()
}

// UnimplementedDBLogServer must be embedded to have forward compatible implementations.
type UnimplementedDBLogServer struct {
}

func (UnimplementedDBLogServer) Capture(DBLog_CaptureServer) error {
	return status.Errorf(codes.Unimplemented, "method Capture not implemented")
}
func (UnimplementedDBLogServer) mustEmbedUnimplementedDBLogServer() {}

// UnsafeDBLogServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to DBLogServer will
// result in compilation errors.
type UnsafeDBLogServer interface {
	mustEmbedUnimplementedDBLogServer()
}

func RegisterDBLogServer(s grpc.ServiceRegistrar, srv DBLogServer) {
	s.RegisterService(&DBLog_ServiceDesc, srv)
}

func _DBLog_Capture_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(DBLogServer).Capture(&dBLogCaptureServer{stream})
}

type DBLog_CaptureServer interface {
	Send(*CaptureMessage) error
	Recv() (*CaptureRequest, error)
	grpc.ServerStream
}

type dBLogCaptureServer struct {
	grpc.ServerStream
}

func (x *dBLogCaptureServer) Send(m *CaptureMessage) error {
	return x.ServerStream.SendMsg(m)
}

func (x *dBLogCaptureServer) Recv() (*CaptureRequest, error) {
	m := new(CaptureRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// DBLog_ServiceDesc is the grpc.ServiceDesc for DBLog service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var DBLog_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "pgcapture.DBLog",
	HandlerType: (*DBLogServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Capture",
			Handler:       _DBLog_Capture_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "pb/pgcapture.proto",
}
