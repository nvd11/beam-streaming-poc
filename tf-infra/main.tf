# =========================================================
# 1. Service Account (服务账号)
# 为 Beam Streaming POC 提供专属身份
# =========================================================
resource "google_service_account" "beam_poc_sa" {
  account_id   = "beam-streaming-poc-sa"
  display_name = "Beam Streaming POC Service Account"
  description  = "专属 SA：用于 Apache Beam 实时风控 POC，具备 Pub/Sub 读写权限"
}

# =========================================================
# 2. IAM 权限绑定
# =========================================================
# 赋予 SA 往 Pub/Sub Topic 发送模拟数据的权限
resource "google_project_iam_member" "pubsub_publisher" {
  project = var.project_id
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:${google_service_account.beam_poc_sa.email}"
}

# 赋予 SA 从 Pub/Sub Subscription 读取流式数据的权限
resource "google_project_iam_member" "pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.beam_poc_sa.email}"
}

# =========================================================
# 3. Pub/Sub Topic (主题)
# =========================================================
resource "google_pubsub_topic" "trade_transactions" {
  name = "trade-transactions"

  message_retention_duration = "86400s" # 消息保留 1 天，方便测试
}

# =========================================================
# 4. Pub/Sub Subscription (订阅)
# =========================================================
resource "google_pubsub_subscription" "trade_transactions_sub" {
  name  = "trade-transactions-sub"
  topic = google_pubsub_topic.trade_transactions.name

  # POC 测试专属配置：保留已确认的消息，方便反复清空状态重试实验
  retain_acked_messages      = true
  message_retention_duration = "86400s" # 1 天

  # 关闭 TTL 自动过期
  expiration_policy {
    ttl = "" 
  }

  # 确保接收的顺序性（可选，对于带 event_time 的数据不是绝对必须）
  enable_message_ordering = false
}
