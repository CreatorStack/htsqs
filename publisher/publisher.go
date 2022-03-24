package publisher

import (
	"context"
)

// Publisher is the interface clients can use to publish messages
type Publisher interface {
	Publish(ctx context.Context, msg interface{}) error
}
