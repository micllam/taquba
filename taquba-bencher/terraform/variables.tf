variable "region" {
  description = "AWS region for the bench host and bucket. Keep both in the same region so the object-store round trip does not dominate every measurement."
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type for the bench host. Use a non-burstable family so CPU credits never throttle a run mid-flight."
  type        = string
  default     = "m7i.xlarge"

  validation {
    condition     = !startswith(var.instance_type, "t")
    error_message = "Use a non-burstable (non-T) instance type so CPU credit exhaustion cannot distort benchmark numbers."
  }
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name for benchmark data. Each run uses a fresh bench-<unix-millis> prefix inside it."
  type        = string
}

variable "taquba_repo_url" {
  description = "Git URL cloned and built on the bench host."
  type        = string
  default     = "https://github.com/micllam/taquba.git"
}

variable "git_ref" {
  description = "Git ref (branch, tag, or commit) checked out before building. Record this in the RESULTS.md entry for the run."
  type        = string
  default     = "master"
}

variable "root_volume_gb" {
  description = "Root volume size in GiB. The build plus a local bench store fit comfortably in the default."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags applied to all created resources."
  type        = map(string)
  default = {
    project = "taquba-bencher"
  }
}
