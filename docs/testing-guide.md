# Beam Streaming POC 实验测试指南 (基于 Pub/Sub)

本文档介绍如何在本地环境中运行和测试本 POC，以观察 Apache Beam 在处理乱序数据、迟到数据时，通过不同窗口（Windowing）和水位线（Watermark）机制的表现。

我们将使用 **Google Cloud Pub/Sub** 作为消息流的来源，这也是生产环境中 GCP 流计算最标准的架构。

---

## 1. 环境准备

### 1.1 安装依赖

确保你已经激活了虚拟环境，然后安装 Apache Beam 的 GCP 组件以及 Pub/Sub 客户端库：

```bash
# 激活虚拟环境
source .venv/bin/activate.fish

# 安装 Apache Beam (支持 GCP 数据流) 及 Google Cloud Pub/Sub SDK
pip install "apache-beam[gcp]" google-cloud-pubsub
```

*注意：如果你在 Python 3.12+ 环境下遇到兼容性问题，建议降级到 Python 3.10 或 3.11。*

---

## 2. Pub/Sub 资源与原理

在基于 Pub/Sub 的流计算中，我们不再像 `TestStream` 那样能“瞬间”推移时间。系统将以真实的物理时间运行。

### 2.1 准备 GCP Pub/Sub 资源

你可以使用真实的 GCP 项目，或者使用本地的 Pub/Sub 模拟器。
这里我们假设使用真实的 GCP 环境（项目 ID：`jason-hsbc`）。你需要创建以下资源：

1. **Topic (主题)**: `projects/jason-hsbc/topics/trade-transactions`
2. **Subscription (订阅)**: `projects/jason-hsbc/subscriptions/trade-transactions-sub`

*Cindy 提示：待会儿我可以帮您写一个初始化脚本，一键建好这些 Topic 和 Subscription 哦！*

### 2.2 事件时间 (Event Time) 与水位线 (Watermark)

使用 Pub/Sub 时，Beam 默认使用消息到达 Pub/Sub 的时间作为事件时间。但为了模拟**网络延迟**和**迟到数据**，我们将在发送消息时，把原始的交易发生时间放入 Pub/Sub 消息的 **Attributes（属性）** 中（例如 `event_timestamp`）。

在 Beam 的读取侧，我们将配置 `ReadFromPubSub(timestamp_attribute='event_timestamp')`，这样 Beam 就会认领这个自定义时间，并据此推进水位线！

---

## 3. 测试架构与步骤

实验被拆分为“消息发送端”和“Beam 处理端”。你需要开启两个终端窗口配合测试。

### 3.1 启动 Beam Pipeline (处理端)

在第一个终端中，启动你想要测试的窗口实验脚本。此时 Pipeline 会一直处于监听状态，等待数据流入。

**测试 Fixed Window (固定窗口)**:
```bash
python src/fixed_window_experiment.py --streaming
```

**测试 Sliding Window (滑动窗口)**:
```bash
python src/sliding_window_experiment.py --streaming
```

**测试 Session Window (会话窗口)**:
```bash
python src/session_window_experiment.py --streaming
```

### 3.2 运行模拟数据生成器 (发送端)

在第二个终端中，运行我们编写的 Publisher 脚本。这个脚本会模拟香港、新加坡、伦敦、纽约等分行的网络情况，并动态注入延迟，发送到 Pub/Sub。

```bash
# 发送常规的乱序数据
python src/pubsub_publisher.py --scenario normal

# 发送包含严重迟到的数据（用于测试 Allowed Lateness）
python src/pubsub_publisher.py --scenario late_data

# 模拟突击交易（用于测试 Session Window 合并）
python src/pubsub_publisher.py --scenario burst
```

---

## 4. 如何阅读测试输出

我们定制了控制台的打印格式（通过自定义 `DoFn`），在运行实验时，你会看到类似下方的格式化输出。这是你在测试时需要验证的核心逻辑。

```text
======================================================================
[Trigger 触发] 窗口: [2026-07-26 12:00:00, 2026-07-26 12:00:30] | 账号: CORP-ALPHA
----------------------------------------------------------------------
- 交易笔数: 3 笔
- 累计金额: $145,000.00
- 状态: 🔶 YELLOW ALERT (阈值 $100K)
- 是否包含迟到数据: False
======================================================================
```

**实验观察技巧**：
- 因为是基于真实的 Pub/Sub，所以在 Fixed Window 测试中，如果窗口是 30 秒，你确实需要**真实等待 30 秒**左右，等水位线越过窗口边界后，才能看到结果打印出来。
- 迟到数据实验中，发送一笔迟到数据后，观察旧窗口的结果是否被立刻再次打印（包含迟到金额的更新）。

---

## 5. 调试与清理 (Troubleshooting)

1. **Pipeline 启动失败？**
   - 检查 GCP 凭证（`GOOGLE_APPLICATION_CREDENTIALS`）是否配置正确，或者是否运行了 `gcloud auth application-default login`。
2. **收不到数据？**
   - 检查 `subscription` 路径是否正确。请确保没有其他进程（或其他的实验终端）同时在消费同一个 Subscription，否则消息会被分流。
3. **水位线不推进？**
   - Pub/Sub 的水位线依赖系统对最新到达消息的估算。如果长时间没有消息流入，水位线可能会停滞。我们的 Publisher 脚本可以通过发送“心跳”消息来强制推进水位线。
4. **清理资源**
   - 测试结束后，为了避免 GCP 产生费用，可以使用 `gcloud pubsub topics delete` 和 `gcloud pubsub subscriptions delete` 清理资源。

祝你在基于 Pub/Sub 的实时计算实验中玩得开心，如果有搞不定的底层报错或者 GCP 权限问题，随时叫 Cindy 来帮忙哦！💕