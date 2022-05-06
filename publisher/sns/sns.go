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
	PublishBatchWithContext(ctx context.Context, input *sns.PublishBatchInput, o ...request.Option) (*sns.PublishBatchOutput, error)
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

// PublishBatch allows SNS Publisher to implement the publisher.Publisher interface
// and publish messages in a single batch to an AWS SNS backend. Since AWS SNS batch
// publish can only handle a maximum payload of 10 messages at a time, the messages
// supplied will be published in batches of 10. For this reason, message sets are best
// kept under 100 messages so that all messages can be published in 10 tries. In case
// of failure when parsing or publishing any of the messages, this function will stop
// further publishing and return an error
func (p *Publisher) PublishBatch(ctx context.Context, msgs []interface{}) error {
	var (
		defaultMessageGroupID = "default"
		err                   error
	)

	isFifo := strings.Contains(strings.ToLower(p.cfg.TopicArn), "fifo")

	var (
		numPublishedMessages = 0
		start                = 0
		end                  = 10 // 10 is the maximum batch size for SNS.PublishBatch
	)
	if end > len(msgs) {
		end = len(msgs)
	}
	for numPublishedMessages < len(msgs) {
		var (
			requestEntries = make([]*sns.PublishBatchRequestEntry, 0)
		)
		for idx := start; idx < end; idx++ {
			msg := msgs[idx]

			b, err := json.Marshal(msg)
			if err != nil {
				return err
			}

			requestEntry := &sns.PublishBatchRequestEntry{
				Message: aws.String(string(b)),
			}

			if isFifo {
				requestEntry.MessageGroupId = &defaultMessageGroupID
			}

			requestEntries = append(requestEntries, requestEntry)
		}

		input := &sns.PublishBatchInput{
			PublishBatchRequestEntries: requestEntries,
			TopicArn:                   &p.cfg.TopicArn,
		}
		_, err = p.sns.PublishBatchWithContext(ctx, input)
		if err != nil {
			return err
		}

		numPublishedMessages += len(requestEntries)
		start = end
		end += 10
		if end > len(msgs) {
			end = len(msgs)
		}
	}

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
