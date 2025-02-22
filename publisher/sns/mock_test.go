package sns

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
)

type snsPublisherMock struct {
	queue chan<- *string
}

func (p *snsPublisherMock) PublishWithContext(ctx context.Context, input *sns.PublishInput, o ...request.Option) (*sns.PublishOutput, error) {
	p.queue <- input.Message
	return &sns.PublishOutput{}, nil
}

func (p *snsPublisherMock) PublishBatchWithContext(ctx context.Context, input *sns.PublishBatchInput, o ...request.Option) (*sns.PublishBatchOutput, error) {
	for _, entry := range input.PublishBatchRequestEntries {
		p.queue <- entry.Message
	}
	return &sns.PublishBatchOutput{}, nil
}
