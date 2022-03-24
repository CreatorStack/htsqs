package sns

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

// sender is the interface to sns.SNS. Its sole purpose is to make
// Publisher.service and interface that we can mock for testing.
type sender interface {
	PublishWithContext(ctx context.Context, input *sns.PublishInput, o ...request.Option) (*sns.PublishOutput, error)
}

// Config holds the info required to work with AWS SNS
type Config struct {

	// AWS session
	AWSSession *session.Session

	// Topic ARN where the messages are going to be sent
	TopicArn string
}

// Publisher is the AWS SNS message publisher
type Publisher struct {
	sns sender
	cfg Config
}

// Publish allows SNS Publisher to implement the publisher.Publisher interface
// and publish messages to an AWS SNS backend
func (p *Publisher) Publish(ctx context.Context, msg interface{}) error {
	b, err := json.Marshal(msg)

	defaultMessageGroupID := "default"

	if err != nil {
		return err
	}

	input := &sns.PublishInput{
		Message:  aws.String(string(b)),
		TopicArn: &p.cfg.TopicArn,
	}
	// if the topic is a fifo topic, we need to set the message group id
	if strings.Contains(strings.ToLower(*input.TopicArn), "fifo") {
		input.MessageGroupId = &defaultMessageGroupID
	}

	_, err = p.sns.PublishWithContext(ctx, input)

	return err
}

func defaultPublisherConfig(cfg *Config) {
	if cfg.AWSSession == nil {
		cfg.AWSSession = session.Must(session.NewSession())
	}
}

// New creates a new AWS SNS publisher
func New(cfg Config) *Publisher {
	defaultPublisherConfig(&cfg)
	return &Publisher{cfg: cfg, sns: sns.New(cfg.AWSSession)}
}
