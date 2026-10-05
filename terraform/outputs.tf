output "bucket" {
  value = google_storage_bucket.data.name
}

output "run_service_account" {
  value = google_service_account.run.email
}

output "api_url" {
  value = var.create_api ? google_cloud_run_v2_service.api[0].uri : null
}

output "jobs" {
  value = [google_cloud_run_v2_job.ingest.name, google_cloud_run_v2_job.backfill.name]
}

output "workload_identity_provider" {
  value = google_iam_workload_identity_pool_provider.github.name
}

output "deployer_service_account" {
  value = google_service_account.deployer.email
}
