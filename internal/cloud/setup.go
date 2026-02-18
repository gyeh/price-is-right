package cloud

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// SetupConfig holds configuration for cloud infrastructure provisioning.
type SetupConfig struct {
	Region   string
	S3Bucket string
}

// Setup provisions the AWS infrastructure needed for Fargate-based processing.
func Setup(ctx context.Context, cfg SetupConfig) error {
	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}

	// 1. Create S3 bucket
	fmt.Printf("Creating S3 bucket %s...\n", cfg.S3Bucket)
	if err := createS3Bucket(ctx, awsCfg, cfg.S3Bucket, cfg.Region); err != nil {
		fmt.Printf("  Bucket may already exist: %v\n", err)
	} else {
		fmt.Println("  Created.")
	}

	// 2. Create ECR repository
	fmt.Println("Creating ECR repository npi-rates...")
	if err := createECRRepo(ctx, awsCfg); err != nil {
		fmt.Printf("  Repository may already exist: %v\n", err)
	} else {
		fmt.Println("  Created.")
	}

	// 3. Create ECS cluster
	fmt.Println("Creating ECS cluster npi-rates...")
	if err := createECSCluster(ctx, awsCfg); err != nil {
		fmt.Printf("  Cluster may already exist: %v\n", err)
	} else {
		fmt.Println("  Created.")
	}

	// 4. Create IAM task role
	fmt.Println("Creating IAM task execution role...")
	roleArn, err := createTaskRole(ctx, awsCfg, cfg.S3Bucket)
	if err != nil {
		fmt.Printf("  Role may already exist: %v\n", err)
	} else {
		fmt.Printf("  Created: %s\n", roleArn)
	}

	// 5. Register task definition
	fmt.Println("Registering ECS task definition...")
	if err := registerTaskDefinition(ctx, awsCfg, cfg.Region); err != nil {
		return fmt.Errorf("registering task definition: %w", err)
	}
	fmt.Println("  Registered.")

	fmt.Println("\nCloud setup complete. Next steps:")
	fmt.Println("  1. Build and push Docker image:")
	fmt.Println("     docker build -t npi-rates .")
	fmt.Printf("     aws ecr get-login-password --region %s | docker login --username AWS --password-stdin <account-id>.dkr.ecr.%s.amazonaws.com\n", cfg.Region, cfg.Region)
	fmt.Println("     docker tag npi-rates:latest <account-id>.dkr.ecr.<region>.amazonaws.com/npi-rates:latest")
	fmt.Println("     docker push <account-id>.dkr.ecr.<region>.amazonaws.com/npi-rates:latest")
	fmt.Println("  2. Run with --cloud flag")

	return nil
}

func createS3Bucket(ctx context.Context, cfg aws.Config, bucket, region string) error {
	client := s3svc.NewFromConfig(cfg)

	input := &s3svc.CreateBucketInput{
		Bucket: aws.String(bucket),
	}
	if region != "us-east-1" {
		input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(region),
		}
	}

	_, err := client.CreateBucket(ctx, input)
	return err
}

func createECRRepo(ctx context.Context, cfg aws.Config) error {
	client := ecr.NewFromConfig(cfg)
	_, err := client.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: aws.String("npi-rates"),
	})
	return err
}

func createECSCluster(ctx context.Context, cfg aws.Config) error {
	client := ecs.NewFromConfig(cfg)
	_, err := client.CreateCluster(ctx, &ecs.CreateClusterInput{
		ClusterName: aws.String(clusterName),
		CapacityProviders: []string{"FARGATE", "FARGATE_SPOT"},
	})
	return err
}

func createTaskRole(ctx context.Context, cfg aws.Config, bucket string) (string, error) {
	client := iam.NewFromConfig(cfg)

	assumeRolePolicy := `{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Principal": {"Service": "ecs-tasks.amazonaws.com"},
			"Action": "sts:AssumeRole"
		}]
	}`

	roleResult, err := client.CreateRole(ctx, &iam.CreateRoleInput{
		RoleName:                 aws.String("npi-rates-task-role"),
		AssumeRolePolicyDocument: aws.String(assumeRolePolicy),
	})
	if err != nil {
		return "", err
	}

	// Attach S3 write policy
	s3Policy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Effect": "Allow",
			"Action": ["s3:PutObject", "s3:GetObject"],
			"Resource": "arn:aws:s3:::%s/*"
		}]
	}`, bucket)

	_, err = client.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
		RoleName:       aws.String("npi-rates-task-role"),
		PolicyName:     aws.String("npi-rates-s3-access"),
		PolicyDocument: aws.String(s3Policy),
	})
	if err != nil {
		return "", fmt.Errorf("attaching S3 policy: %w", err)
	}

	return aws.ToString(roleResult.Role.Arn), nil
}

func registerTaskDefinition(ctx context.Context, cfg aws.Config, region string) error {
	client := ecs.NewFromConfig(cfg)

	_, err := client.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{
		Family:                  aws.String(taskFamily),
		RequiresCompatibilities: []ecstypes.Compatibility{ecstypes.CompatibilityFargate},
		NetworkMode:             ecstypes.NetworkModeAwsvpc,
		Cpu:                     aws.String("8192"),  // 8 vCPU — speeds up pgzip decompression
		Memory:                  aws.String("16384"), // 16 GB (minimum for 8 vCPU)
		EphemeralStorage: &ecstypes.EphemeralStorage{
			SizeInGiB: 200, // max Fargate ephemeral — supports decompressed files up to ~190GB
		},
		TaskRoleArn:      aws.String("npi-rates-task-role"),
		ExecutionRoleArn: aws.String("ecsTaskExecutionRole"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{
				Name:      aws.String(containerName),
				Image:     aws.String(fmt.Sprintf("npi-rates:latest")),
				Essential: aws.Bool(true),
				LogConfiguration: &ecstypes.LogConfiguration{
					LogDriver: ecstypes.LogDriverAwslogs,
					Options: map[string]string{
						"awslogs-group":         "/ecs/npi-rates",
						"awslogs-region":        region,
						"awslogs-stream-prefix": "ecs",
					},
				},
			},
		},
	})

	return err
}
