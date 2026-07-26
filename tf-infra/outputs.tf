output "service_account_email" {
  description = "Beam POC 专属 Service Account 邮箱"
  value       = google_service_account.beam_poc_sa.email
}

output "pubsub_topic_id" {
  description = "Pub/Sub 交易数据 Topic ID"
  value       = google_pubsub_topic.trade_transactions.id
}

output "pubsub_subscription_id" {
  description = "Pub/Sub 交易数据 Subscription ID"
  value       = google_pubsub_subscription.trade_transactions_sub.id
}
