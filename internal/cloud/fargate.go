package cloud

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

const (
	clusterName   = "npi-rates"
	taskFamily    = "npi-rates-worker"
	containerName = "npi-rates"
)

// FargateOrchestrator manages ECS Fargate tasks for distributed processing.
type FargateOrchestrator struct {
	ecsClient *ecs.Client
	region    string
	bucket    string
	subnets   []string
}

// NewFargateOrchestrator creates a new Fargate orchestrator.
func NewFargateOrchestrator(ctx context.Context, region, bucket string, subnets []string) (*FargateOrchestrator, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	return &FargateOrchestrator{
		ecsClient: ecs.NewFromConfig(cfg),
		region:    region,
		bucket:    bucket,
		subnets:   subnets,
	}, nil
}

// TaskInput defines the parameters for a single Fargate task.
type TaskInput struct {
	URLsS3Key string  // S3 key containing the URL list file
	NPIs      []int64
	TaskIndex  int
	OutputKey  string // S3 key for results
}

// LaunchTask starts a Fargate task with the given parameters.
func (f *FargateOrchestrator) LaunchTask(ctx context.Context, input TaskInput) (string, error) {
	// Build NPI string
	npiStrs := make([]string, len(input.NPIs))
	for i, n := range input.NPIs {
		npiStrs[i] = fmt.Sprintf("%d", n)
	}

	// Build command — task downloads URLs from S3 and uploads results to S3
	cmd := []string{
		"/npi-rates", "search",
		"--urls-s3", fmt.Sprintf("s3://%s/%s", f.bucket, input.URLsS3Key),
		"--npi", strings.Join(npiStrs, ","),
		"--output-s3", fmt.Sprintf("s3://%s/%s", f.bucket, input.OutputKey),
		"--cloud-region", f.region,
		"--no-progress",
		"--workers", "2",
	}

	result, err := f.ecsClient.RunTask(ctx, &ecs.RunTaskInput{
		Cluster:        aws.String(clusterName),
		TaskDefinition: aws.String(taskFamily),
		LaunchType:     ecstypes.LaunchTypeFargate,
		Count:          aws.Int32(1),
		NetworkConfiguration: &ecstypes.NetworkConfiguration{
			AwsvpcConfiguration: &ecstypes.AwsVpcConfiguration{
				Subnets:        f.subnets,
				AssignPublicIp: ecstypes.AssignPublicIpEnabled,
			},
		},
		Overrides: &ecstypes.TaskOverride{
			ContainerOverrides: []ecstypes.ContainerOverride{
				{
					Name:    aws.String(containerName),
					Command: cmd,
				},
			},
		},
		CapacityProviderStrategy: []ecstypes.CapacityProviderStrategyItem{
			{
				CapacityProvider: aws.String("FARGATE_SPOT"),
				Weight:          1,
			},
		},
	})
	if err != nil {
		return "", fmt.Errorf("launching Fargate task: %w", err)
	}

	if len(result.Tasks) == 0 {
		return "", fmt.Errorf("no tasks launched")
	}

	return aws.ToString(result.Tasks[0].TaskArn), nil
}

// TaskResult holds the completion status of a Fargate task.
type TaskResult struct {
	TaskArn  string
	Success  bool
	ExitCode int32
	Reason   string
}

// WaitForTasks polls until all tasks complete. Returns per-task results.
// The onStatus callback is invoked on each poll with running/pending/stopped counts.
func (f *FargateOrchestrator) WaitForTasks(ctx context.Context, taskArns []string, onStatus func(running, pending, stopped int)) ([]TaskResult, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(15 * time.Second):
		}

		resp, err := f.ecsClient.DescribeTasks(ctx, &ecs.DescribeTasksInput{
			Cluster: aws.String(clusterName),
			Tasks:   taskArns,
		})
		if err != nil {
			return nil, fmt.Errorf("describing tasks: %w", err)
		}

		running, pending, stopped := 0, 0, 0
		allDone := true
		for _, task := range resp.Tasks {
			switch aws.ToString(task.LastStatus) {
			case "RUNNING":
				running++
				allDone = false
			case "PENDING", "PROVISIONING":
				pending++
				allDone = false
			case "STOPPED":
				stopped++
			default:
				allDone = false
			}
		}

		if onStatus != nil {
			onStatus(running, pending, stopped)
		}

		if allDone {
			results := make([]TaskResult, len(resp.Tasks))
			for i, task := range resp.Tasks {
				results[i] = TaskResult{
					TaskArn: aws.ToString(task.TaskArn),
					Success: true,
				}
				for _, container := range task.Containers {
					if container.ExitCode != nil && *container.ExitCode != 0 {
						results[i].Success = false
						results[i].ExitCode = *container.ExitCode
						results[i].Reason = aws.ToString(container.Reason)
					}
				}
			}
			return results, nil
		}
	}
}

// DescribeTasks returns the current status of the given tasks.
func (f *FargateOrchestrator) DescribeTasks(ctx context.Context, taskArns []string) ([]ecstypes.Task, error) {
	resp, err := f.ecsClient.DescribeTasks(ctx, &ecs.DescribeTasksInput{
		Cluster: aws.String(clusterName),
		Tasks:   taskArns,
	})
	if err != nil {
		return nil, err
	}
	return resp.Tasks, nil
}

// StopAllTasks stops all the given Fargate tasks. Returns any errors encountered.
func (f *FargateOrchestrator) StopAllTasks(ctx context.Context, taskArns []string) []error {
	var errs []error
	for _, arn := range taskArns {
		_, err := f.ecsClient.StopTask(ctx, &ecs.StopTaskInput{
			Cluster: aws.String(clusterName),
			Task:    aws.String(arn),
			Reason:  aws.String("Orchestrator shutdown"),
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("stopping %s: %w", TaskIDFromARN(arn), err))
		}
	}
	return errs
}
