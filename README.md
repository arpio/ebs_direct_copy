# ebs_direct_copy

A Python CLI tool for copying EBS snapshots, AMIs, and EC2 instances across AWS regions using the [EBS Direct APIs](https://docs.aws.amazon.com/ebs/latest/APIReference/Welcome.html). All items are processed in parallel with real-time progress bars.

## Background

This tool was created in response to ongoing impairment of the AWS **Middle East (UAE)** (`me-central-1`) and **Middle East (Bahrain)** (`me-south-1`) regions. When a region is degraded, the standard `ec2:CopySnapshot` API — which relies on AWS's internal control plane — may be unavailable or unreliable. This tool bypasses that dependency entirely by reading snapshot data block-by-block from the source region and writing it directly to the destination, using only the EBS Direct data-plane APIs.

This makes it suitable for data evacuation scenarios where normal cross-region copy mechanisms have failed.

## How it works

Rather than delegating the copy to AWS (as `ec2:CopySnapshot` does), this tool:

1. Lists all non-empty blocks in the source snapshot via `ListSnapshotBlocks`
2. Reads each block via `GetSnapshotBlock`
3. Writes each block to a new destination snapshot via `PutSnapshotBlock`
4. Finalizes the destination snapshot via `CompleteSnapshot`

Block transfers run in parallel across a shared thread pool. AMI copies transfer all underlying snapshots concurrently, then re-register the AMI in the destination region. EC2 instance copies first create an AMI (without rebooting), then copy that AMI.

## Requirements

- Python 3.10+
- boto3

```bash
pip install boto3
```

## IAM permissions

| Side        | Permissions required |
|-------------|----------------------|
| Source      | `ebs:ListSnapshotBlocks`, `ebs:GetSnapshotBlock`, `ec2:DescribeSnapshots`, `ec2:DescribeImages`, `ec2:DescribeInstances`, `ec2:CreateImage` |
| Destination | `ebs:StartSnapshot`, `ebs:PutSnapshotBlock`, `ebs:CompleteSnapshot`, `ec2:DescribeSnapshots`, `ec2:DeleteSnapshot`, `ec2:RegisterImage` |

## Usage

```
python ebs_direct_copy.py [OPTIONS] ID [ID ...]
```

Each `ID` can be a snapshot (`snap-...`), an AMI (`ami-...`), or an EC2 instance (`i-...`). Multiple IDs are processed in parallel.

### Options

| Option | Description |
|--------|-------------|
| `--from-region` | **(Required)** Source AWS region, e.g. `me-south-1` |
| `--to-region` | Destination AWS region. Defaults to `AWS_DEFAULT_REGION` if set |
| `--description` | Description applied to copied snapshots/AMIs |
| `--name` | Name applied to copied AMI(s) |
| `--src-profile` | AWS CLI profile to use for the source region |
| `--dst-profile` | AWS CLI profile to use for the destination region |
| `--kms-key-id` | KMS key ARN to encrypt snapshots in the destination region |
| `--workers` | Number of concurrent block-copy threads (default: 32) |

### Examples

**Copy a snapshot out of Bahrain to eu-west-1:**
```bash
python ebs_direct_copy.py \
  --from-region me-south-1 \
  --to-region eu-west-1 \
  snap-0abc1234def567890
```

**Copy multiple AMIs and snapshots in parallel:**
```bash
python ebs_direct_copy.py \
  --from-region me-south-1 \
  --to-region eu-west-1 \
  ami-0abc1234def567890 \
  ami-0def1234abc567890 \
  snap-0123456789abcdef0
```

**Copy an EC2 instance out of UAE (creates an AMI first, then copies it):**
```bash
python ebs_direct_copy.py \
  --from-region me-central-1 \
  --to-region eu-west-1 \
  i-0abc1234def567890
```

**Copy with destination-side KMS encryption using separate AWS profiles:**
```bash
python ebs_direct_copy.py \
  --from-region me-south-1 \
  --to-region us-east-1 \
  --src-profile bahrain-prod \
  --dst-profile us-prod \
  --kms-key-id arn:aws:kms:us-east-1:123456789012:key/mrk-abc123 \
  ami-0abc1234def567890
```

**Increase parallelism for large snapshots:**
```bash
python ebs_direct_copy.py \
  --from-region me-south-1 \
  --to-region eu-west-1 \
  --workers 64 \
  snap-0abc1234def567890
```

## Output

While running, the tool displays a live progress bar per item:

```
  snap-0abc1234def567890   [████████████░░░░░░░░░░░░]  50.2%  Copying 1373/2739 blocks  → snap-0def1234abc567890
```

On completion, a summary table is printed:

```
──────────────────────────────────────────────────────────────────────────
  SOURCE                      DESTINATION                 RESULT
──────────────────────────────────────────────────────────────────────────
  snap-0abc1234def567890      snap-0def1234abc567890      ✓  (eu-west-1)
──────────────────────────────────────────────────────────────────────────
```

The exit code is `0` if all items succeeded, `1` if any failed.

## Cancellation

Press `Ctrl-C` at any time. In-progress destination snapshots will be deleted before the process exits.

## Notes

- Snapshots must be in `completed` state in the source region to be copied.
- EC2 instance copies use `--no-reboot` when creating the intermediate AMI. For crash-consistent copies this is fine; for application-consistent copies, stop the instance first and pass its AMI ID directly.
- The `--workers` thread count is shared across all concurrent copies. For large parallel workloads, increasing this value (e.g. `--workers 64` or higher) will improve throughput.
- If the source snapshot is encrypted, the destination snapshot will also be encrypted. Use `--kms-key-id` to specify a different key in the destination region, or omit it to use the destination account's default EBS KMS key.
