# Benchmark infrastructure

Terraform that provisions a single EC2 host and an S3 bucket in one
region for running the `taquba-bencher` benchmarks against real object
storage, which allows published numbers (recorded in `../RESULTS.md`) to
come from a reproducible environment.

What it creates:

- An EC2 instance (default `m7i.xlarge`, a non-burstable type so CPU
  credits cannot throttle a run) that on first boot installs Rust,
  clones taquba at `git_ref`, and builds `taquba-bencher --features aws`.
- An S3 bucket for bench data, private, with a lifecycle rule that
  expires `bench-` run prefixes after one day.
- A least-privilege IAM role granting the host access to that bucket
  only, plus Session Manager for a keyless, inbound-port-free shell.

The host and bucket are placed in the same region.

## Usage

Requires the Terraform CLI, AWS credentials in the environment, and the
AWS CLI with the Session Manager plugin for connecting.

```bash
cp terraform.tfvars.example terraform.tfvars   # set a unique bucket_name
terraform init
terraform apply

# Open a shell on the host (command is also printed as an output).
$(terraform output -raw ssm_connect)

# Become root. cloud-init builds under /opt/taquba as root, but SSM logs
# in as the unprivileged ssm-user, which cannot write there. If `cargo`
# is not found, export the toolchain paths explicitly:
sudo -i
export RUSTUP_HOME=/opt/rust/rustup CARGO_HOME=/opt/rust/cargo PATH=/opt/rust/cargo/bin:$PATH
cloud-init status --wait   # block until the first-boot build finishes

# On the host, run a bench and capture the CSV. terraform output
# bench_command_hint prints a ready-to-edit invocation.
cd /opt/taquba
STORE_URL=s3://<bucket> AWS_REGION=<region> \
  cargo bench -p taquba-bencher --features aws --bench steady_state > steady.csv

# Summarise the result into ../RESULTS.md, then tear everything down.
terraform destroy
```

Record the `git_ref` you built and the instance type in the
`RESULTS.md` entry so the numbers stay tied to their environment.

## Sampling storage during a run

To track storage growth over a long run (leak and drift checks),
`sample-storage.sh` appends `epoch,objects,bytes` rows to a CSV on a
fixed interval. The AWS CLI is preinstalled by `user_data`, so run it
from the cloned repo on the host:

```bash
# Sample the run's prefix every 5 minutes.
/opt/taquba/taquba-bencher/terraform/sample-storage.sh \
  s3://<bucket>/<store-prefix> storage.csv 300
```

On a missing CLI or an `aws s3 ls` error it appends an `ERR` row
(and an `*.aws-err.log`). It loops until killed.

## Running a long bench in the background

An SSM shell dies with your connection, killing any foreground bench.
For multi-hour runs, start the bench and the sampler as transient
`systemd-run` units; they survive disconnects and are queryable with
`systemctl` and `journalctl`:

```bash
# Bench. bash -c sets the toolchain env and redirects output to files.
systemd-run --unit taquba-bench --working-directory /opt/taquba \
  bash -c 'export RUSTUP_HOME=/opt/rust/rustup CARGO_HOME=/opt/rust/cargo PATH=/opt/rust/cargo/bin:$PATH; \
    STORE_URL=s3://<bucket> AWS_REGION=<region> \
    cargo bench -p taquba-bencher --features aws --bench steady_state \
    > /opt/taquba/bench.csv 2> /opt/taquba/bench.err'

# Sampler (writes its own CSV).
systemd-run --unit taquba-storage \
  /opt/taquba/taquba-bencher/terraform/sample-storage.sh \
    s3://<bucket>/<store-prefix> /opt/taquba/storage.csv 300

systemctl status taquba-bench    # progress; reads inactive when done
systemctl stop taquba-storage    # stop the sampler once the bench ends
```

## Retrieving results before destroy

`terraform destroy` deletes everything, including the instance and the
bucket (`force_destroy` removes the bucket even with run data still in
it). Be aware of where your data actually lives before you tear down:

- The **bucket** holds the system-under-test's data (the
  `bench-<unix-millis>` queue workload), not your results.
- The benches write their CSV to **stdout on the host**, so the numbers
  land on the instance's local disk. Destroy deletes that too.

So both places your data could be are wiped on destroy. Capture what you
need first. Since `../RESULTS.md` records summarised percentiles rather
than raw CSV, the minimum is to read the run's summary off the host (it
prints to stderr) and write the entry before destroying.

To keep the raw CSV as well, copy it off the host before destroy. If you
stage it through S3, use a `results/` prefix so the `bench-` lifecycle
rule does not expire it within a day:

```bash
# On the host, after the run.
aws s3 cp steady.csv s3://<bucket>/results/steady-<date>.csv

# On your machine, before terraform destroy.
aws s3 cp s3://<bucket>/results/steady-<date>.csv .
```

## State

State is local by default. For shared or long-lived use, configure
an S3 backend in `versions.tf`.
