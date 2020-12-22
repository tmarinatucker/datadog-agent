package tagger

import (
	"context"
	"fmt"
	"io"

	"google.golang.org/grpc"

	"github.com/DataDog/datadog-agent/cmd/agent/api/pb"
	"github.com/DataDog/datadog-agent/cmd/agent/api/response"
	"github.com/DataDog/datadog-agent/pkg/api/util"
	"github.com/DataDog/datadog-agent/pkg/config"
	"github.com/DataDog/datadog-agent/pkg/status/health"
	"github.com/DataDog/datadog-agent/pkg/tagger/collectors"
)

type remoteTagger struct {
	conn   *grpc.ClientConn
	client pb.AgentSecureClient
	stream pb.AgentSecure_TaggerStreamEntitiesClient

	ctx    context.Context
	cancel context.CancelFunc

	health *health.Handle
}

func newRemoteTagger() *remoteTagger {
	return &remoteTagger{}
}

func (t *remoteTagger) Init() error {
	t.health = health.RegisterLiveness("tagger")

	t.ctx, t.cancel = context.WithCancel(context.Background())

	token := util.GetAuthToken()

	conn, err := grpc.DialContext(
		t.ctx,
		fmt.Sprintf(":%v", config.Datadog.GetInt("cmd_port")),

		// NOTE: we're using grpc.WithInsecure because the gRPC server only
		// persists its TLS certs in memory, and we currently have no
		// infrastructure to make them available to clients.
		grpc.WithInsecure(),

		grpc.WithAuthority(fmt.Sprintf("Bearer %s", token)),
	)
	if err != nil {
		return err
	}

	t.client = pb.NewAgentSecureClient(conn)

	go t.run()

	return nil
}

func (t *remoteTagger) Stop() error {
	t.cancel()

	err := t.conn.Close()
	if err != nil {
		return err
	}

	return nil
}

func (t *remoteTagger) Tag(entity string, cardinality collectors.TagCardinality) ([]string, error) {
	panic("not implemented") // TODO: Implement
}

func (t *remoteTagger) Standard(entity string) ([]string, error) {
	panic("not implemented") // TODO: Implement
}

func (t *remoteTagger) List(cardinality collectors.TagCardinality) response.TaggerListResponse {
	panic("not implemented") // TODO: Implement
}

func (t *remoteTagger) Subscribe(cardinality collectors.TagCardinality) chan []EntityEvent {
	panic("not implemented") // TODO: Implement
}

func (t *remoteTagger) Unsubscribe(ch chan []EntityEvent) {
	panic("not implemented") // TODO: Implement
}

func (t *remoteTagger) run() {
	err := t.startTaggerStream()
	if err != nil {
		panic(err)
	}

	for {
		select {
		case <-t.health.C:
		case <-t.ctx.Done():
			return
		default:
		}

		response, err := t.stream.Recv()
		if err != nil {
			// Recv() returns io.EOF only when the connection has
			// been intentionally closed by the server. When that
			// happens, we need to establish a new stream. With any
			// other error, including network issues, the stream
			// will remain funcional, as the ClientConn will manage
			// retries accordingly.
			// TODO(juliogreff): verify if that's actually the case :D
			if err == io.EOF {
				err = t.startTaggerStream()
			}

			// TODO(juliogreff): log instead of panic
			panic("whoops")
			continue
		}

		fmt.Printf("EVENT: %s", response.Type)
	}
}

// TODO(juliogreff): should we block until the stream is started, with some
// exponential backoff perhaps?
func (t *remoteTagger) startTaggerStream() error {
	select {
	case <-t.ctx.Done():
		return nil
	default:
	}

	var err error
	t.stream, err = t.client.TaggerStreamEntities(t.ctx, nil)

	return err
}
