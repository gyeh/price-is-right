package cloud

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

const logGroupName = "/ecs/npi-rates"

// LogStreamer streams CloudWatch logs from Fargate tasks.
type LogStreamer struct {
	client *cloudwatchlogs.Client
}

// NewLogStreamer creates a new CloudWatch log streamer.
func NewLogStreamer(ctx context.Context, region string) (*LogStreamer, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}
	return &LogStreamer{client: cloudwatchlogs.NewFromConfig(cfg)}, nil
}

// TaskIDFromARN extracts the task ID from an ECS task ARN.
// ARN format: arn:aws:ecs:region:account:task/cluster/task-id
func TaskIDFromARN(arn string) string {
	parts := strings.Split(arn, "/")
	if len(parts) >= 3 {
		return parts[len(parts)-1]
	}
	return arn
}

// StreamLogs streams CloudWatch logs for a Fargate task, calling onLog for each new line.
// Blocks until the context is cancelled. The log stream name follows the ECS awslogs format:
// {prefix}/{container-name}/{task-id}
func (s *LogStreamer) StreamLogs(ctx context.Context, taskARN string, onLog func(line string)) {
	taskID := TaskIDFromARN(taskARN)
	streamName := fmt.Sprintf("ecs/%s/%s", containerName, taskID)

	var nextToken *string

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(3 * time.Second):
		}

		input := &cloudwatchlogs.GetLogEventsInput{
			LogGroupName:  aws.String(logGroupName),
			LogStreamName: aws.String(streamName),
			StartFromHead: aws.Bool(true),
		}
		if nextToken != nil {
			input.NextToken = nextToken
		}

		resp, err := s.client.GetLogEvents(ctx, input)
		if err != nil {
			// Log stream may not exist yet while task is starting up
			continue
		}

		for _, event := range resp.Events {
			if event.Message != nil {
				onLog(strings.TrimRight(*event.Message, "\n"))
			}
		}

		if resp.NextForwardToken != nil {
			nextToken = resp.NextForwardToken
		}
	}
}
