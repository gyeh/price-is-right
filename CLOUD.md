# Cloud / Fargate Orchestration Setup

This guide walks through distributing NPI rate searches across AWS Fargate tasks for processing large MRF file sets (e.g., 278 files, ~437GB compressed).

## Why Fargate?

- **No time limit** — Lambda has a 15-minute timeout; a single 40GB MRF file takes longer to download + decompress + parse
- **Up to 200GB ephemeral storage** — Lambda caps at 10GB `/tmp`
- **~30s cold start** — serverless containers, no EC2 instances to manage
- **Spot pricing** — FARGATE_SPOT gives 70-90% discount over on-demand
- **x86-64 architecture** — required for simdjson SIMD acceleration (AVX2+CLMUL)

## Prerequisites

- AWS CLI v2 configured with credentials (`aws configure`)
- Docker installed and running
- Go 1.22+ (to build the binary)
- Sufficient IAM permissions: ECS, ECR, S3, IAM, CloudWatch Logs

### Required IAM Permissions for Setup

The user/role running `cloud-setup` needs:

```
ecr:CreateRepository
ecs:CreateCluster
ecs:RegisterTaskDefinition
iam:CreateRole
iam:PutRolePolicy
s3:CreateBucket
logs:CreateLogGroup (for CloudWatch, created automatically by ECS)
```

## Step 1: Provision Infrastructure

The `cloud-setup` command creates all required AWS resources:

```bash
npi-rates cloud-setup --s3-bucket my-npi-results --region us-east-1
```

This creates:

| Resource | Name | Purpose |
|----------|------|---------|
| S3 Bucket | `my-npi-results` | Stores partial results from each Fargate task |
| ECR Repository | `npi-rates` | Hosts the Docker image |
| ECS Cluster | `npi-rates` | Fargate-only cluster with FARGATE_SPOT enabled |
| IAM Role | `npi-rates-task-role` | Assumed by ECS tasks; grants S3 read/write to the results bucket |
| ECS Task Definition | `npi-rates-worker` | 4 vCPU, 8GB RAM, 100GB ephemeral storage |

The command is **idempotent** — re-running it skips resources that already exist.

### ECS Task Execution Role

The task definition references `ecsTaskExecutionRole` as the execution role (used by ECS itself to pull images and write logs). If you don't already have this role, create it:

```bash
aws iam create-role \
  --role-name ecsTaskExecutionRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "ecs-tasks.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

aws iam attach-role-policy \
  --role-name ecsTaskExecutionRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
```

### CloudWatch Log Group

ECS tasks log to `/ecs/npi-rates`. Create the log group if it doesn't exist:

```bash
aws logs create-log-group --log-group-name /ecs/npi-rates --region us-east-1
```

## Step 2: Build and Push Docker Image

Get your AWS account ID:

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1
```

Build the image (must be amd64 for simdjson SIMD support):

```bash
docker build --platform linux/amd64 -t npi-rates .
```

Authenticate to ECR, tag, and push:

```bash
aws ecr get-login-password --region $REGION \
  | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

docker tag npi-rates:latest $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/npi-rates:latest

docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/npi-rates:latest
```

### Update Task Definition Image

After pushing, update the task definition to use the full ECR image URI. The `cloud-setup` command registers the task definition with `npi-rates:latest` as a placeholder — you need to update it to point to your ECR image:

```bash
# Get the current task definition
aws ecs describe-task-definition --task-definition npi-rates-worker \
  --query 'taskDefinition' > /tmp/taskdef.json

# Edit the image field in /tmp/taskdef.json to:
#   "<account-id>.dkr.ecr.<region>.amazonaws.com/npi-rates:latest"
# Then re-register (or just re-run cloud-setup after updating the code)
```

Alternatively, edit `internal/cloud/setup.go` line 173 to include your full ECR URI before running `cloud-setup`, and it will register the correct image automatically.

## Step 3: Find Your Subnet IDs

Fargate tasks need a VPC subnet with internet access (to download MRF files). Use your default VPC's public subnets:

```bash
# List default VPC subnets
aws ec2 describe-subnets \
  --filters "Name=default-for-az,Values=true" \
  --query 'Subnets[].SubnetId' \
  --output text
```

Note these subnet IDs — you'll need them when launching tasks.

## Step 4: Launch Fargate Tasks

### Splitting the Work

Each Fargate task processes a subset of MRF URLs. Split your URL file into chunks:

```bash
# Split 278 URLs into chunks of 5 URLs each (56 tasks)
split -l 5 urls.txt /tmp/url_chunk_
```

### Launching Tasks via AWS CLI

For each chunk, launch a Fargate task:

```bash
CLUSTER=npi-rates
TASK_DEF=npi-rates-worker
SUBNET=subnet-abc123  # from step 3
BUCKET=my-npi-results
NPI=1234567890

# For each chunk file
for i in $(seq -w 1 56); do
  URLS=$(cat /tmp/url_chunk_$i | tr '\n' ',' | sed 's/,$//')

  aws ecs run-task \
    --cluster $CLUSTER \
    --task-definition $TASK_DEF \
    --launch-type FARGATE \
    --capacity-provider-strategy capacityProvider=FARGATE_SPOT,weight=1 \
    --network-configuration "awsvpcConfiguration={subnets=[$SUBNET],assignPublicIp=ENABLED}" \
    --overrides "{
      \"containerOverrides\": [{
        \"name\": \"npi-rates\",
        \"command\": [\"/npi-rates\", \"search\",
          \"--urls-file\", \"/dev/stdin\",
          \"--npi\", \"$NPI\",
          \"--output\", \"s3://$BUCKET/results/task-$i.json\",
          \"--no-progress\",
          \"--workers\", \"2\"
        ]
      }]
    }" \
    --query 'tasks[0].taskArn' \
    --output text
done
```

> **Note**: The container command above uses `--urls-file /dev/stdin` as a placeholder. In practice, you'd either:
> 1. Bake the URL list into the container, or
> 2. Upload URL chunks to S3 and have the container download them, or
> 3. Pass URLs directly via environment variables

The simplest approach is to upload URL chunk files to S3:

```bash
# Upload URL chunks
for f in /tmp/url_chunk_*; do
  aws s3 cp $f s3://$BUCKET/urls/$(basename $f).txt
done
```

Then modify the container command to download the URL file from S3 before running the search. This requires adding a wrapper script to the Dockerfile (see [Customizing the Container](#customizing-the-container) below).

### Launching Tasks Programmatically

The Go `FargateOrchestrator` in `internal/cloud/fargate.go` provides a programmatic API:

```go
orch, err := cloud.NewFargateOrchestrator(ctx, "us-east-1", "my-npi-results", []string{"subnet-abc123"})

// Launch tasks
var taskArns []string
for i, chunk := range urlChunks {
    arn, err := orch.LaunchTask(ctx, cloud.TaskInput{
        URLs:      chunk,
        NPIs:      []int64{1234567890},
        TaskIndex: i,
        OutputKey: fmt.Sprintf("results/task-%d.json", i),
    })
    taskArns = append(taskArns, arn)
}

// Wait for all tasks to complete
err = orch.WaitForTasks(ctx, taskArns)

// Download and merge results
s3Client, _ := cloud.NewS3Client(ctx, "my-npi-results", "us-east-1")
var allResults []mrf.RateResult
for i := range urlChunks {
    results, _ := s3Client.DownloadResults(ctx, fmt.Sprintf("results/task-%d.json", i))
    allResults = append(allResults, results...)
}
```

## Step 5: Monitor Tasks

### Check task status

```bash
# List running tasks
aws ecs list-tasks --cluster npi-rates --desired-status RUNNING

# Describe specific tasks
aws ecs describe-tasks --cluster npi-rates \
  --tasks arn:aws:ecs:us-east-1:123456789:task/npi-rates/abc123
```

### View logs

```bash
# Stream logs from a task
aws logs tail /ecs/npi-rates --follow

# Get logs for a specific task
aws logs get-log-events \
  --log-group-name /ecs/npi-rates \
  --log-stream-name "ecs/npi-rates/<task-id>"
```

### Stop all tasks

```bash
for arn in $(aws ecs list-tasks --cluster npi-rates --query 'taskArns[]' --output text); do
  aws ecs stop-task --cluster npi-rates --task $arn
done
```

## Step 6: Collect Results

Download partial results from S3 and merge:

```bash
# Download all result files
aws s3 sync s3://my-npi-results/results/ /tmp/results/

# Merge with jq
jq -s '{
  search_params: {
    npis: .[0].search_params.npis,
    searched_files: ([.[].search_params.searched_files] | add),
    matched_files: ([.[].search_params.matched_files] | add),
    duration_seconds: ([.[].search_params.duration_seconds] | max)
  },
  results: [.[].results[]]
}' /tmp/results/*.json > final_results.json
```

## Customizing the Container

To have the container fetch URL files from S3 at runtime, replace the Dockerfile `ENTRYPOINT` with a wrapper script:

```dockerfile
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /npi-rates ./cmd/npi-rates

FROM alpine:3.21
RUN apk add --no-cache ca-certificates aws-cli
COPY --from=builder /npi-rates /npi-rates
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
```

Create `entrypoint.sh`:

```bash
#!/bin/sh
set -e

# If URLS_S3_PATH is set, download the URL file from S3
if [ -n "$URLS_S3_PATH" ]; then
  aws s3 cp "$URLS_S3_PATH" /tmp/urls.txt
  exec /npi-rates search --urls-file /tmp/urls.txt "$@"
else
  exec /npi-rates "$@"
fi
```

Then launch tasks with the environment variable:

```bash
aws ecs run-task \
  --cluster npi-rates \
  --task-definition npi-rates-worker \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-abc123],assignPublicIp=ENABLED}" \
  --overrides '{
    "containerOverrides": [{
      "name": "npi-rates",
      "environment": [
        {"name": "URLS_S3_PATH", "value": "s3://my-npi-results/urls/url_chunk_01.txt"}
      ],
      "command": ["--npi", "1234567890", "--output", "s3://my-npi-results/results/task-01.json", "--no-progress"]
    }]
  }'
```

## Cost Estimates

Rough pricing for processing 278 files (~437GB compressed) with FARGATE_SPOT in us-east-1:

| Resource | Spec | Est. Cost |
|----------|------|-----------|
| Fargate Spot (per task) | 4 vCPU, 8GB RAM, ~20 min | ~$0.03/task |
| 56 tasks total | 5 URLs each | ~$1.70 |
| S3 storage | Results (small) | < $0.01 |
| Data transfer | 437GB download from CDN | $0 (free from internet) |
| **Total** | | **~$2** |

Actual cost depends on file sizes and processing time. The largest files (40GB) may take 30+ minutes per task.

## Tuning

### Task Definition Resources

Adjust in `internal/cloud/setup.go` or via AWS console:

| Setting | Default | Notes |
|---------|---------|-------|
| CPU | 4096 (4 vCPU) | More CPUs speed up pgzip decompression and jsplit |
| Memory | 8192 (8 GB) | Sufficient for most files; increase for 40GB+ files |
| Ephemeral Storage | 100 GB | jsplit NDJSON output + temp files; increase for largest files |

### Workers per Task

Each Fargate task runs `--workers 2` by default (2 files concurrently within a task). Increase if the task has spare CPU/memory, decrease if you see disk pressure from concurrent decompression.

### URLs per Task

- **1 URL/task**: Maximum parallelism, most Fargate tasks, highest overhead
- **5 URLs/task**: Good balance (recommended)
- **10+ URLs/task**: Fewer tasks but longer runtime per task; risk of Spot interruption

## Cleanup

Remove all provisioned resources:

```bash
# Delete task definition (all revisions)
for arn in $(aws ecs list-task-definitions --family-prefix npi-rates-worker --query 'taskDefinitionArns[]' --output text); do
  aws ecs deregister-task-definition --task-definition $arn
done

# Delete ECS cluster
aws ecs delete-cluster --cluster npi-rates

# Delete ECR repository (and all images)
aws ecr delete-repository --repository-name npi-rates --force

# Delete IAM role and policy
aws iam delete-role-policy --role-name npi-rates-task-role --policy-name npi-rates-s3-access
aws iam delete-role --role-name npi-rates-task-role

# Delete S3 bucket (must be empty first)
aws s3 rm s3://my-npi-results --recursive
aws s3 rb s3://my-npi-results

# Delete log group
aws logs delete-log-group --log-group-name /ecs/npi-rates
```

## Architecture Diagram

```
                         ┌─────────────────────────────────────────────┐
                         │              Your Machine                   │
                         │                                             │
                         │  npi-rates cloud-setup                      │
                         │    └─ Creates: S3, ECR, ECS, IAM, TaskDef  │
                         │                                             │
                         │  docker build + push → ECR                  │
                         │                                             │
                         │  Launch N Fargate tasks (via CLI or Go API) │
                         └──────────────┬──────────────────────────────┘
                                        │
                    ┌───────────────────┬┴──────────────────┐
                    ▼                   ▼                    ▼
          ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
          │  Fargate Task 1 │ │  Fargate Task 2 │ │  Fargate Task N │
          │  (FARGATE_SPOT) │ │  (FARGATE_SPOT) │ │  (FARGATE_SPOT) │
          │                 │ │                 │ │                 │
          │ Download 5 URLs │ │ Download 5 URLs │ │ Download 5 URLs │
          │ pgzip decompress│ │ pgzip decompress│ │ pgzip decompress│
          │ jsplit → NDJSON │ │ jsplit → NDJSON │ │ jsplit → NDJSON │
          │ simdjson parse  │ │ simdjson parse  │ │ simdjson parse  │
          │ NPI matching    │ │ NPI matching    │ │ NPI matching    │
          └────────┬────────┘ └────────┬────────┘ └────────┬────────┘
                   │                   │                    │
                   ▼                   ▼                    ▼
          ┌──────────────────────────────────────────────────────────┐
          │                      S3 Bucket                           │
          │  results/task-1.json  task-2.json  ...  task-N.json     │
          └──────────────────────────────┬───────────────────────────┘
                                         │
                                         ▼
                         ┌───────────────────────────────┐
                         │       Your Machine             │
                         │  Download + merge results      │
                         │  → final_results.json          │
                         └───────────────────────────────┘
```
