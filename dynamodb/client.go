package dynamodb

import (
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

const dateFormat = "20060102"

type Client struct {
	logger    log.Logger
	client    *dynamodb.DynamoDB
	tableName string
}

type Config struct {
	TableName string
}

type Record struct {
	PartitionId string            `json:"partitionId"`
	Metric      string            `json:"metric"`
	Timestamp   time.Time         `json:"timestamp"`
	Labels      map[string]string `json:"labels"`
	Value       string            `json:"value"`
}

// extractLabels get all labels from a given metric
func extractLabels(metric model.Metric) map[string]string {
	labels := make(map[string]string, len(metric)-1)
	for l, v := range metric {
		if l != model.MetricNameLabel {
			labels[string(l)] = string(v)
		}
	}
	return labels
}

func NewClient(logger log.Logger, tableName string) *Client {
	sess := session.Must(session.NewSession())
	svc := dynamodb.New(sess)

	if logger == nil {
		logger = log.NewNopLogger()
	}

	return &Client{
		logger:    logger,
		client:    svc,
		tableName: tableName,
	}
}

func (c *Client) putSample(sample *model.Sample) error {
	ts := sample.Timestamp
	metric := string(sample.Metric[model.MetricNameLabel])
	r := Record{
		PartitionId: fmt.Sprintf("%s-%s", metric, ts.Time().Format(dateFormat)),
		Metric:      metric,
		Timestamp:   ts.Time(),
		Labels:      extractLabels(sample.Metric),
		Value:       fmt.Sprintf("%s", sample.Value),
	}
	mr, err := dynamodbattribute.MarshalMap(r)
	if err != nil {
		level.Error(c.logger).Log("msg", "couldn't marshall record into dynamodb attribute", "err", err)
		return err
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(c.tableName),
		Item:      mr,
	}
	_, err = c.client.PutItem(input)
	if err != nil {
		level.Error(c.logger).Log("err", "error inserting new record", "err", err)
	}
	return err
}

func (c *Client) Write(samples model.Samples) error {
	writeErr := *new(error)
	putErr := *new(error)
	for _, s := range samples {
		putErr = c.putSample(s)
		if putErr != nil {
			writeErr = putErr
			level.Error(c.logger).Log("err", "error inserting new record", "err", putErr)
		}

	}
	return writeErr
}

func (c *Client) Name() string {
	return "DynamoDb"
}

func (c *Client) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	return nil, nil
}

func (c *Client) HealthCheck() error {
	return nil
}
