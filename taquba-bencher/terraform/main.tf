data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd*/ubuntu-noble-24.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Bench data bucket. force_destroy lets terraform destroy remove the
# bucket even with run prefixes still in it; the lifecycle rule expires
# them anyway so leftover data does not accrue cost between runs.
resource "aws_s3_bucket" "bench" {
  bucket        = var.bucket_name
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "bench" {
  bucket = aws_s3_bucket.bench.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "bench" {
  bucket = aws_s3_bucket.bench.id

  # Expire run data one day after a run. This assumes run data is written
  # directly into the bucket (STORE_URL=s3://<bucket>), so every prefix is
  # bench-<unix-millis>.
  rule {
    id     = "expire-bench-runs"
    status = "Enabled"

    filter {
      prefix = "bench-"
    }

    expiration {
      days = 1
    }
  }
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "bench" {
  name               = "${var.bucket_name}-bench"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

# Least-privilege access to the bench bucket only.
data "aws_iam_policy_document" "bucket_access" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.bench.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.bench.arn}/*"]
  }
}

resource "aws_iam_role_policy" "bucket_access" {
  name   = "bucket-access"
  role   = aws_iam_role.bench.id
  policy = data.aws_iam_policy_document.bucket_access.json
}

# Session Manager provides the management channel, so the host needs no
# inbound port and no SSH key.
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.bench.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "bench" {
  name = "${var.bucket_name}-bench"
  role = aws_iam_role.bench.name
}

resource "aws_security_group" "bench" {
  name        = "${var.bucket_name}-bench"
  description = "Egress-only access for the taquba bench host; SSM carries management traffic."
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "All outbound traffic (object store, package mirrors, git)."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "bench" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance_type
  subnet_id              = element(tolist(data.aws_subnets.default.ids), 0)
  iam_instance_profile   = aws_iam_instance_profile.bench.name
  vpc_security_group_ids = [aws_security_group.bench.id]

  root_block_device {
    volume_size = var.root_volume_gb
    volume_type = "gp3"
  }

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    repo_url = var.taquba_repo_url
    git_ref  = var.git_ref
  })

  tags = {
    Name = "${var.bucket_name}-bench"
  }
}
