package testutils

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/greenplum-db/gpupgrade/hub/services"

	pb "github.com/greenplum-db/gpupgrade/idl"

	"google.golang.org/grpc"
)

type MockAgentServer struct {
	addr       net.Addr
	grpcServer *grpc.Server
	numCalls   int
	mu         sync.Mutex

	StatusConversionRequest              *pb.CheckConversionStatusRequest
	StatusConversionResponse             *pb.CheckConversionStatusReply
	UpgradeConvertPrimarySegmentsRequest *pb.UpgradeConvertPrimarySegmentsRequest
	CreateSegmentDataDirRequest          *pb.CreateSegmentDataDirRequest

	Err chan error
}

// NewMockAgentServer starts a locally running Agent server and returns a struct
// that facilitates unit testing. It also returns a services.Dialer that will
// redirect any outgoing connections to this mock server, as well as the port
// that the server is listening on.
func NewMockAgentServer() (*MockAgentServer, services.Dialer, int) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}

	mockServer := &MockAgentServer{
		addr:       lis.Addr(),
		grpcServer: grpc.NewServer(),
		Err:        make(chan error, 10000),
	}

	pb.RegisterAgentServer(mockServer.grpcServer, mockServer)

	go func() {
		mockServer.grpcServer.Serve(lis)
	}()

	// Target this running server during dial.
	port := lis.Addr().(*net.TCPAddr).Port
	dialer := func(ctx context.Context, _ string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		authority := fmt.Sprintf("127.0.0.1:%d", port)
		return grpc.DialContext(ctx, authority, opts...)
	}

	return mockServer, dialer, port
}

func (m *MockAgentServer) CheckUpgradeStatus(context.Context, *pb.CheckUpgradeStatusRequest) (*pb.CheckUpgradeStatusReply, error) {
	m.increaseCalls()

	return &pb.CheckUpgradeStatusReply{}, nil
}

func (m *MockAgentServer) CheckConversionStatus(ctx context.Context, in *pb.CheckConversionStatusRequest) (*pb.CheckConversionStatusReply, error) {
	m.increaseCalls()

	m.StatusConversionRequest = in

	var err error
	if len(m.Err) != 0 {
		err = <-m.Err
	}

	return m.StatusConversionResponse, err
}

func (m *MockAgentServer) CheckDiskSpaceOnAgents(context.Context, *pb.CheckDiskSpaceRequestToAgent) (*pb.CheckDiskSpaceReplyFromAgent, error) {
	m.increaseCalls()

	return &pb.CheckDiskSpaceReplyFromAgent{}, nil
}

func (m *MockAgentServer) PingAgents(context.Context, *pb.PingAgentsRequest) (*pb.PingAgentsReply, error) {
	m.increaseCalls()

	return &pb.PingAgentsReply{}, nil
}

func (m *MockAgentServer) UpgradeConvertPrimarySegments(ctx context.Context, in *pb.UpgradeConvertPrimarySegmentsRequest) (*pb.UpgradeConvertPrimarySegmentsReply, error) {
	m.increaseCalls()

	m.mu.Lock()
	defer m.mu.Unlock()
	m.UpgradeConvertPrimarySegmentsRequest = in

	var err error
	if len(m.Err) != 0 {
		err = <-m.Err
	}

	return &pb.UpgradeConvertPrimarySegmentsReply{}, err
}

func (m *MockAgentServer) CreateSegmentDataDirectories(ctx context.Context, in *pb.CreateSegmentDataDirRequest) (*pb.CreateSegmentDataDirReply, error) {
	m.increaseCalls()

	m.mu.Lock()
	defer m.mu.Unlock()
	m.CreateSegmentDataDirRequest = in

	var err error
	if len(m.Err) != 0 {
		err = <-m.Err
	}

	return &pb.CreateSegmentDataDirReply{}, err
}

func (m *MockAgentServer) Stop() {
	m.grpcServer.Stop()
}

func (m *MockAgentServer) increaseCalls() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.numCalls++
}

func (m *MockAgentServer) NumberOfCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.numCalls
}
