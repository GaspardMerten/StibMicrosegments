terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
  # Local state, as for JourneyReality. Move to a GCS backend if more than one person applies.
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "this" {}

locals {
  # Jobs and the service need an existing image at creation; CI replaces it on every deploy and
  # Terraform ignores the image afterwards (lifecycle blocks below).
  image = coalesce(var.image, "us-docker.pkg.dev/cloudrun/container/hello")
  token_env = {
    name   = "MOBILITYTWIN_TOKEN"
    secret = google_secret_manager_secret.token.secret_id
  }
}

resource "google_project_service" "required" {
  for_each = toset([
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudscheduler.googleapis.com",
    "secretmanager.googleapis.com",
    "iamcredentials.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
  ])
  service            = each.key
  disable_on_destroy = false
}

# ---------------------------------------------------------------- data
# Raw days (~9 MB each), GTFS feeds once per content sha, derived tables and the result cache.
# No lifecycle rule: raw days are the archive the derived layers are rebuilt from.
resource "google_storage_bucket" "data" {
  name                        = var.bucket
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = false
  }
  depends_on = [google_project_service.required]
}

# The value is added out of band, never in Terraform state:
#   printf %s "$MOBILITYTWIN_TOKEN" | gcloud secrets versions add ms-mobilitytwin-token --data-file=- --project mobility-twin-norse
resource "google_secret_manager_secret" "token" {
  secret_id = "ms-mobilitytwin-token"
  replication {
    auto {}
  }
  depends_on = [google_project_service.required]
}

# ---------------------------------------------------------------- identities
resource "google_service_account" "run" {
  account_id   = "ms-run"
  display_name = "Microsegments API and ingest jobs"
  depends_on   = [google_project_service.required]
}

resource "google_storage_bucket_iam_member" "run_data" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.run.email}"
}

resource "google_secret_manager_secret_iam_member" "run_token" {
  secret_id = google_secret_manager_secret.token.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.run.email}"
}

resource "google_service_account" "scheduler" {
  account_id   = "ms-cron"
  display_name = "Triggers the microsegments nightly ingest"
  depends_on   = [google_project_service.required]
}

# CI: the project's shared pool `github` / provider `github-oidc` only admits
# GaspardMerten/MobilityLakeHouse (attribute condition), so this repo gets its own pool, provider
# and deploy account, as transit-map and sncb-map do.
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "ms-github"
  display_name              = "GitHub Actions for microsegments"
  depends_on                = [google_project_service.required]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github"
  display_name                       = "GitHub"
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.repository" = "assertion.repository"
  }
  attribute_condition = "assertion.repository == \"${var.github_repo}\""
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account" "deployer" {
  account_id   = "ms-deploy"
  display_name = "Builds and deploys microsegments from GitHub"
  depends_on   = [google_project_service.required]
}

resource "google_service_account_iam_member" "github_impersonates_deployer" {
  service_account_id = google_service_account.deployer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repo}"
}

resource "google_project_iam_member" "deployer_run" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.deployer.email}"
}

# Images go to the project's existing cloud-run-source-deploy repository (Cloud Run already pulls from it).
resource "google_artifact_registry_repository_iam_member" "deployer_push" {
  project    = var.project_id
  location   = var.region
  repository = var.image_repository
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.deployer.email}"
}

resource "google_service_account_iam_member" "deployer_acts_as_run" {
  service_account_id = google_service_account.run.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.deployer.email}"
}
