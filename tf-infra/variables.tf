variable "project_id" {
  description = "The GCP Project ID"
  type        = string
  default     = "jason-hsbc"
}

variable "region" {
  description = "The GCP Region"
  type        = string
  default     = "asia-east2" # 默认使用香港节点
}
