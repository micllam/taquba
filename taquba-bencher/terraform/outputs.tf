output "instance_id" {
  description = "EC2 instance ID of the bench host."
  value       = aws_instance.bench.id
}

output "bucket" {
  description = "S3 bucket holding benchmark data."
  value       = aws_s3_bucket.bench.id
}

output "ssm_connect" {
  description = "Command to open a shell on the bench host via SSM."
  value       = "aws ssm start-session --region ${var.region} --target ${aws_instance.bench.id}"
}

output "bench_command_hint" {
  description = "Example bench invocation, run as root (`sudo -i`). Writes run data directly into the bucket (no subfolder) so the `bench-` lifecycle rule expires it."
  value       = "cd /opt/taquba && STORE_URL=s3://${aws_s3_bucket.bench.id} AWS_REGION=${var.region} cargo bench -p taquba-bencher --features aws --bench steady_state > steady.csv"
}
