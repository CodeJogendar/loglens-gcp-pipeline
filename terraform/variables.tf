variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type    = string
  default = "asia-south1"
}

variable "alert_email" {
  type        = string
  description = "Email that receives the 5xx alert"
}
