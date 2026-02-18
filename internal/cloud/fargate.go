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
	clusterName    = "npi-rates"
	taskFamily     = "npi-rates-worker"
	containerName  = "npi-rates"
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
	URLs       []string
	NPIs       []int64
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

	// Build command
	cmd := []string{
		"/npi-rates", "search",
		"--npi", strings.Join(npiStrs, ","),
		"--output", fmt.Sprintf("s3://%s/%s", f.bucket, input.OutputKey),
		"--no-progress",
		"--workers", "2",
	}

	// Add URLs as inline args
	for _, u := range input.URLs {
		cmd = append(cmd, "--url", u)
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

// WaitForTasks polls until all tasks complete. Returns an error if any task fails.
func (f *FargateOrchestrator) WaitForTasks(ctx context.Context, taskArns []string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(15 * time.Second):
		}

		resp, err := f.ecsClient.DescribeTasks(ctx, &ecs.DescribeTasksInput{
			Cluster: aws.String(clusterName),
			Tasks:   taskArns,
		})
		if err != nil {
			return fmt.Errorf("describing tasks: %w", err)
		}

		allDone := true
		for _, task := range resp.Tasks {
			status := aws.ToString(task.LastStatus)
			if status != "STOPPED" {
				allDone = false
				break
			}
		}

		if allDone {
			// Check for failures
			for _, task := range resp.Tasks {
				if task.StopCode == ecstypes.TaskStopCodeEssentialContainerExited {
					for _, container := range task.Containers {
						if container.ExitCode != nil && *container.ExitCode != 0 {
							return fmt.Errorf("task %s container %s exited with code %d: %s",
								aws.ToString(task.TaskArn),
								aws.ToString(container.Name),
								*container.ExitCode,
								aws.ToString(container.Reason),
							)
						}
					}
				}
			}
			return nil
		}

		// Log status
		running, pending, stopped := 0, 0, 0
		for _, task := range resp.Tasks {
			switch aws.ToString(task.LastStatus) {
			case "RUNNING":
				running++
			case "PENDING", "PROVISIONING":
				pending++
			case "STOPPED":
				stopped++
			}
		}
		fmt.Printf("  Tasks: %d running, %d pending, %d stopped (of %d total)\n",
			running, pending, stopped, len(taskArns))
	}
}
