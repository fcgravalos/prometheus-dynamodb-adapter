package dynamodb

import (
	"fmt"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/cenkalti/backoff"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

const (
	dateFormat       = "20060102"
	dynamodbPutLimit = 25
)

type Client struct {
	logger    log.Logger
	client    *dynamodb.DynamoDB
	tableName string
}

type Config struct {
	/*Region             string
	AwsAccessKeyId     string
	AwsSecretAccessKey string*/
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

func (c *Client) sendBatch(writeReqs []*dynamodb.WriteRequest) error {
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			fmt.Sprintf("%s", c.tableName): writeReqs,
		},
	}
	fmt.Printf("LEN OF WRITEREQS: %v\n", len(writeReqs))
	output, err := c.client.BatchWriteItem(input)

	if err != nil {
		level.Error(c.logger).Log("msg", "couldn't insert new record into dynamodb table", "err", err)
		return err
	}

	if len(output.UnprocessedItems) > 0 {
		go func(writeReqs map[string][]*dynamodb.WriteRequest) error {
			b := backoff.NewExponentialBackOff()
			b.MaxElapsedTime = 3 * time.Minute
			operation := func() error {
				_, err := c.client.BatchWriteItem(input)
				return err
			}
			err = backoff.Retry(operation, b)
			if err != nil {
				level.Error(c.logger).Log("msg", "couldn't insert items after retrying", "err", err)
			}
			return err
		}(output.UnprocessedItems)
	}
	return err
}

// Write implements the Writer interface and writes metric samples to DynamoDb table
func (c *Client) Write(samples model.Samples) error {
	err := *new(error)
	writeReqs := make([]*dynamodb.WriteRequest, dynamodbPutLimit)
	for _, sample := range samples {
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
			continue
		}
		writeReq := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: mr,
			},
		}

		if len(writeReqs)%dynamodbPutLimit == 0 {
			err = c.sendBatch(writeReqs)
			writeReqs = nil
		}
		writeReqs = append(writeReqs, writeReq)
	}

	if len(writeReqs) > 0 {
		fmt.Printf("AAAAQUIIIIIIII: %v\n", len(writeReqs))
		err = c.sendBatch(writeReqs)
	}

	return err
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
