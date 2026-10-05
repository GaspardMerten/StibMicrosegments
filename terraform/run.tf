# ---------------------------------------------------------------- ingest jobs
# One image, three commands. A day takes ~10-15 s and ~1.2 GB RAM (two UTC-day bulk Parquet files
# in memory); 4 GiB leaves room for days with JSON fallback.

resource "google_cloud_run_v2_job" "ingest" {
  name                = "ms-ingest"
  location            = var.region
  deletion_protection = false

  template {
    task_count = 1
    template {
      service_account = google_service_account.run.email
      max_retries     = 2
      timeout         = "3600s"

      containers {
        image   = local.image
        command = ["python", "-m", "stibms.ingest"]
        args    = ["--date", "yesterday"]

        resources {
          limits = {
            cpu    = "2"
            memory = "4Gi"
          }
        }
        env {
          name  = "MS_BUCKET"
          value = "gs://${google_storage_bucket.data.name}"
        }
        env {
          name = local.token_env.name
          value_source {
            secret_key_ref {
              secret  = local.token_env.secret
              version = "latest"
            }
          }
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [client, client_version, template[0].template[0].containers[0].image]
  }
  depends_on = [google_secret_manager_secret_iam_member.run_token]
}

# Same command over a range, dates split round-robin across tasks (CLOUD_RUN_TASK_INDEX/COUNT).
# Run with: gcloud run jobs execute ms-backfill --region europe-west1 \
#   --args=-m,stibms.ingest,--date,2024-04-05,--to,2026-10-04,--shard-from-env --tasks 20
resource "google_cloud_run_v2_job" "backfill" {
  name                = "ms-backfill"
  location            = var.region
  deletion_protection = false

  template {
    task_count  = var.backfill_tasks
    parallelism = var.backfill_parallelism
    template {
      service_account = google_service_account.run.email
      max_retries     = 2
      timeout         = "3600s"

      containers {
        image   = local.image
        command = ["python", "-m", "stibms.ingest"]
        args    = ["--date", var.backfill_from, "--to", var.backfill_to, "--shard-from-env"]

        resources {
          limits = {
            cpu    = "2"
            memory = "4Gi"
          }
        }
        env {
          name  = "MS_BUCKET"
          value = "gs://${google_storage_bucket.data.name}"
        }
        env {
          name = local.token_env.name
          value_source {
            secret_key_ref {
              secret  = local.token_env.secret
              version = "latest"
            }
          }
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [client, client_version, template[0].template[0].containers[0].image]
  }
  depends_on = [google_secret_manager_secret_iam_member.run_token]
}

# Derive (placed / passages / coverage per line-month) over a range of months, in one task: months
# must run one after the other because they share the link-key registry. ~4-5 min per month for
# all lines, so a full history (~31 months) takes ~2.5 h, hence the long timeout. The nightly
# ingest re-derives the current month itself; this job is for backfills and ALGO_VERSION bumps:
#   gcloud run jobs execute ms-derive --region europe-west1 --args=--from,2024-04,--to,2026-10
resource "google_cloud_run_v2_job" "derive" {
  name                = "ms-derive"
  location            = var.region
  deletion_protection = false

  template {
    task_count = 1
    template {
      service_account = google_service_account.run.email
      max_retries     = 1
      timeout         = "21600s"

      containers {
        # Created on the image CI already pushed; CI keeps it updated like the other jobs.
        image   = "${var.region}-docker.pkg.dev/${var.project_id}/${var.image_repository}/stib-microsegments:latest"
        command = ["python", "-m", "stibms.derive"]
        args    = ["--month", "current"]

        resources {
          limits = {
            cpu    = "2"
            memory = "8Gi"
          }
        }
        env {
          name  = "MS_BUCKET"
          value = "gs://${google_storage_bucket.data.name}"
        }
      }
    }
  }

  lifecycle {
    ignore_changes = [client, client_version, template[0].template[0].containers[0].image]
  }
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_runs_ingest" {
  name     = google_cloud_run_v2_job.ingest.name
  location = google_cloud_run_v2_job.ingest.location
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

# Service day D closes at D+1 03:00 Brussels; the bulk hourly file of that hour lands ~30 min later.
resource "google_cloud_scheduler_job" "nightly" {
  name      = "ms-ingest-nightly"
  region    = var.region
  schedule  = "30 5 * * *"
  time_zone = "Europe/Brussels"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/${google_cloud_run_v2_job.ingest.name}:run"
    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
}

# ---------------------------------------------------------------- API (placeholder)
# Created here with a placeholder image; CI deploys the real one once the API exists.
# Scales to zero (min 0) and only gets CPU during requests.
resource "google_cloud_run_v2_service" "api" {
  count               = var.create_api ? 1 : 0
  name                = "ms-api"
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = false

  template {
    service_account                  = google_service_account.run.email
    max_instance_request_concurrency = 8
    timeout                          = "120s"

    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }

    containers {
      image = local.image
      resources {
        limits = {
          # 2 vCPU: one cache-missing analysis (one at a time, stibms.api.HEAVY) leaves a core for
          # the other requests. CPU only during requests (cpu_idle), min 0 instances.
          cpu    = "2"
          memory = "2Gi"
        }
        cpu_idle          = true
        startup_cpu_boost = true
      }
      env {
        name  = "MS_BUCKET"
        value = "gs://${google_storage_bucket.data.name}"
      }
    }
  }

  lifecycle {
    ignore_changes = [client, client_version, scaling, template[0].containers[0].image, template[0].containers[0].command, template[0].containers[0].args]
  }
}

resource "google_cloud_run_v2_service_iam_member" "public" {
  count    = var.create_api ? 1 : 0
  name     = google_cloud_run_v2_service.api[0].name
  location = google_cloud_run_v2_service.api[0].location
  role     = "roles/run.invoker"
  member   = "allUsers"
}
