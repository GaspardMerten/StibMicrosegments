variable "project_id" {
  type    = string
  default = "mobility-twin-norse"
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "bucket" {
  type    = string
  default = "mobility-twin-norse-microsegments"
}

variable "image" {
  description = "Container image at creation. CI overwrites it on every deploy; null = Cloud Run hello placeholder."
  type        = string
  default     = null
}

variable "github_repo" {
  description = "owner/name of the repository allowed to deploy through WIF."
  type        = string
  default     = "GaspardMerten/StibMicrosegments"
}

variable "image_repository" {
  description = "Artifact Registry Docker repository (europe-west1) created for the image, with a keep-5 cleanup policy."
  type        = string
  default     = "microsegments"
}

variable "create_api" {
  description = "Create the ms-api Cloud Run service (placeholder image until the API ships)."
  type        = bool
  default     = true
}

variable "backfill_from" {
  type    = string
  default = "2024-04-05"
}

variable "backfill_to" {
  type    = string
  default = "2026-10-04"
}

variable "backfill_tasks" {
  type    = number
  default = 20
}

variable "backfill_parallelism" {
  type    = number
  default = 10
}
