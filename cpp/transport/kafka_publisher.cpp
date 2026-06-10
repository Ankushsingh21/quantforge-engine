// transport/kafka_publisher.cpp

#ifdef QUANTFORGE_KAFKA_ENABLED

#include "kafka_publisher.hpp"

#include <cstring>
#include <sstream>
namespace qf {

// ── Simple DR callback ─────────────────────────────────────────────

class DrCb : public RdKafka::DeliveryReportCb {
public:
  std::atomic<uint64_t> &errors;

  explicit DrCb(std::atomic<uint64_t> &err_ctr) : errors(err_ctr) {}

  void dr_cb(RdKafka::Message &msg) override {
    if (msg.err()) {
      errors.fetch_add(1, std::memory_order_relaxed);

      // Don't log every error on hot path
      // just count.
    }
  }
};

// ── Constructor ───────────────────────────────────────────────────

KafkaPublisher::KafkaPublisher(const std::string &brokers, TopicNames topics)
    : topics_(std::move(topics)) {
  std::string err;

  auto conf = std::unique_ptr<RdKafka::Conf>(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

  conf->set("bootstrap.servers", brokers, err);

  conf->set("queue.buffering.max.messages", "100000", err);

  conf->set("queue.buffering.max.ms", "5",
            err); // 5ms batching

  conf->set("batch.num.messages", "1000", err);

  conf->set("compression.codec", "lz4", err);

  conf->set("message.max.bytes", "10485760",
            err); // 10MB

  static DrCb dr_cb{errors_};

  conf->set("dr_cb", &dr_cb, err);

  producer_.reset(RdKafka::Producer::create(conf.get(), err));

  if (!producer_) {
    LOG_ERROR("[Kafka] Failed to create producer: {}", err);

    return;
  }

  // Create topic handles

  auto make_topic = [&](const std::string &name) {
    auto tc = std::unique_ptr<RdKafka::Conf>(
        RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

    std::string terr;

    return std::unique_ptr<RdKafka::Topic>(
        RdKafka::Topic::create(producer_.get(), name, tc.get(), terr));
  };

  topic_ticks_ = make_topic(topics_.ticks);

  topic_fills_ = make_topic(topics_.fills);

  topic_portfolio_ = make_topic(topics_.portfolio);

  topic_alerts_ = make_topic(topics_.alerts);

  healthy_.store(true, std::memory_order_release);

  LOG_INFO("[Kafka] Producer ready. Brokers={}", brokers);

  // Background poll thread

  stop_.store(false, std::memory_order_release);

  poll_thread_ = std::thread([this] { delivery_poll_loop(); });
}

KafkaPublisher::~KafkaPublisher() {

  stop_.store(true, std::memory_order_release);

  if (poll_thread_.joinable())
    poll_thread_.join();

  if (producer_) {
    producer_->flush(5000);
  }
}

// ── Publish methods ───────────────────────────────────────────────

void KafkaPublisher::publish_tick(const NormalizedTick &tick) {
  if (!healthy_.load(std::memory_order_acquire) || !topic_ticks_)
    return;

  std::string payload = serialize_tick(tick);

  // Key = instrument_token (4 bytes)
  // for partition routing

  uint32_t key_val = static_cast<uint32_t>(tick.instrument_token);

  producer_->produce(topic_ticks_.get(), RdKafka::Topic::PARTITION_UA,
                     RdKafka::Producer::RK_MSG_COPY,
                     const_cast<char *>(payload.data()), payload.size(),
                     &key_val, sizeof(key_val), nullptr);

  published_.fetch_add(1, std::memory_order_relaxed);
}

void KafkaPublisher::publish_fill(const Fill &fill) {
  if (!healthy_.load(std::memory_order_acquire) || !topic_fills_)
    return;

  std::string payload = serialize_fill(fill);

  producer_->produce(topic_fills_.get(), RdKafka::Topic::PARTITION_UA,
                     RdKafka::Producer::RK_MSG_COPY,
                     const_cast<char *>(payload.data()), payload.size(),
                     fill.strategy_id, std::strlen(fill.strategy_id), nullptr);

  published_.fetch_add(1, std::memory_order_relaxed);
}

void KafkaPublisher::publish_portfolio(const PortfolioSnapshot &snap) {
  if (!healthy_.load(std::memory_order_acquire) || !topic_portfolio_)
    return;

  std::string payload = serialize_portfolio(snap);

  producer_->produce(
      topic_portfolio_.get(),
      0, // single partition for portfolio updates (ordering matters)
      RdKafka::Producer::RK_MSG_COPY, const_cast<char *>(payload.data()),
      payload.size(), nullptr, 0, nullptr);

  published_.fetch_add(1, std::memory_order_relaxed);
}

void KafkaPublisher::publish_alert(const RiskAlert &alert) {
  if (!healthy_.load(std::memory_order_acquire) || !topic_alerts_)
    return;

  std::string payload = serialize_alert(alert);

  producer_->produce(topic_alerts_.get(), RdKafka::Topic::PARTITION_UA,
                     RdKafka::Producer::RK_MSG_COPY,
                     const_cast<char *>(payload.data()), payload.size(),
                     nullptr, 0, nullptr);
}

void KafkaPublisher::flush(int wait_ms) {
  if (producer_)
    producer_->flush(wait_ms);
}

// ── Delivery poll loop ─────────────────────────────────────────────

void KafkaPublisher::delivery_poll_loop() {
  while (!stop_.load(std::memory_order_acquire)) {
    if (producer_) {
      producer_->poll(10); // 10ms poll timeout
    }
  }
}

// ── Serializers (compact binary - replace with protobuf in production)
// ────────────────────────────────────────────────────────────────────

std::string KafkaPublisher::serialize_tick(const NormalizedTick &t) {

  // Simple flat binary:
  // token(8) + ltp(8) + volume(8) + ts(8) = 32 bytes

  std::string buf(32, '\0');

  std::memcpy(buf.data() + 0, &t.instrument_token, 8);
  std::memcpy(buf.data() + 8, &t.ltp, 8);
  std::memcpy(buf.data() + 16, &t.volume, 8);
  std::memcpy(buf.data() + 24, &t.recv_ts_ns, 8);

  return buf;
}

std::string KafkaPublisher::serialize_fill(const Fill &f) {

  std::string buf(32 + MAX_ORDER_ID_LEN + MAX_STRATEGY_LEN, '\0');

  size_t off = 0;

  std::memcpy(buf.data() + off, f.order_id, MAX_ORDER_ID_LEN);
  off += MAX_ORDER_ID_LEN;

  std::memcpy(buf.data() + off, f.strategy_id, MAX_STRATEGY_LEN);
  off += MAX_STRATEGY_LEN;

  std::memcpy(buf.data() + off, &f.instrument_token, 8);
  off += 8;

  std::memcpy(buf.data() + off, &f.price, 8);
  off += 8;

  std::memcpy(buf.data() + off, &f.quantity, 4);
  off += 4;

  uint8_t side = static_cast<uint8_t>(f.side);

  std::memcpy(buf.data() + off, &side, 1);

  return buf;
}

std::string KafkaPublisher::serialize_portfolio(const PortfolioSnapshot &s) {

  // JSON string for portfolio
  // (less frequent, OK to be larger)

  std::ostringstream oss;

  oss << "{\"available_capital\":" << s.available_capital
      << ",\"realized_pnl\":" << s.total_realized_pnl
      << ",\"unrealized_pnl\":" << s.total_unrealized_pnl
      << ",\"ts\":" << s.snapshot_ts_ns << "}";

  return oss.str();
}

std::string KafkaPublisher::serialize_alert(const RiskAlert &a) {

  std::ostringstream oss;

  oss << "{\"level\":" << static_cast<int>(a.level) << ",\"strategy\":\""
      << a.strategy_id << "\""
      << ",\"message\":\"" << a.message << "\""
      << ",\"ts\":" << a.ts_ns << "}";

  return oss.str();
}

} // namespace qf

#endif // QUANTFORGE_KAFKA_ENABLED