#include "core.hpp"

using namespace std::chrono_literals;

std::unique_ptr<NativeStream> NativeStream::udp(const std::string& host, uint16_t port, size_t batch_size, uint64_t linger_ms, size_t capacity) {
    return std::unique_ptr<NativeStream>(new NativeStream(host, port, batch_size, linger_ms, capacity, true));
}

std::unique_ptr<NativeStream> NativeStream::manual(size_t batch_size, uint64_t linger_ms, size_t capacity) {
    return std::unique_ptr<NativeStream>(new NativeStream("manual", 0, batch_size, linger_ms, capacity, false));
}

NativeStream::NativeStream(std::string host, uint16_t requested_port, size_t batch_size, uint64_t linger_ms, size_t capacity, bool udp)
    : address_(std::move(host)),
      port_(requested_port),
      batch_size_(std::max<size_t>(1, batch_size)),
      linger_ms_(linger_ms),
      capacity_(std::max<size_t>(1, capacity)),
      udp_(udp) {
    if (udp_) {
        bind_udp();
    }
    worker_ = std::thread([this] { worker_loop(); });
}

NativeStream::~NativeStream() {
    stop(true);
}

std::pair<std::string, uint16_t> NativeStream::address() const {
    return {address_, port_};
}

void NativeStream::emit_json(const py::dict& payload) {
    push_event(dict_to_row(payload));
}

void NativeStream::emit_columns(const py::dict& columns) {
    auto rows = rows_from_columns(columns);
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& row : rows) {
        stats_.ingested.fetch_add(1, std::memory_order_relaxed);
        if (queue_.size() >= capacity_) {
            stats_.dropped.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        queue_.push_back(std::move(row));
    }
    cv_.notify_all();
}

py::object NativeStream::next_batch(const py::dict& plan_dict, uint64_t timeout_ms) {
    PlanSpec plan = parse_plan(plan_dict);
    std::vector<Row> rows;
    bool closed = false;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (pending_chunks_.empty() && !closed_) {
            cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms));
        }
        while (!pending_chunks_.empty()) {
            auto chunk = std::move(pending_chunks_.front());
            pending_chunks_.pop_front();
            rows.insert(rows.end(), std::make_move_iterator(chunk.begin()), std::make_move_iterator(chunk.end()));
        }
        closed = closed_;
    }

    std::vector<Row> output_rows;
    {
        std::lock_guard<std::mutex> lock(processor_mutex_);
        output_rows = process_rows(stats_, processor_, std::move(rows), plan);
        if (output_rows.empty() && closed) {
            output_rows = flush_all_windows(processor_, plan);
        }
    }

    output_rows = project_rows(std::move(output_rows), plan.select);
    if (output_rows.empty()) {
        return py::none();
    }
    auto batch = rows_to_columns(output_rows);
    stats_.output_batches.fetch_add(1, std::memory_order_relaxed);
    stats_.output_rows.fetch_add(output_rows.size(), std::memory_order_relaxed);
    return batch;
}

py::dict NativeStream::aggregate_columns(const py::dict& columns, const py::dict& plan_dict) {
    PlanSpec plan = parse_plan(plan_dict);
    auto rows = rows_from_columns(columns);
    std::vector<Row> output_rows;
    {
        std::lock_guard<std::mutex> lock(processor_mutex_);
        output_rows = process_rows(stats_, processor_, std::move(rows), plan);
        auto flushed = flush_all_windows(processor_, plan);
        output_rows.insert(output_rows.end(), std::make_move_iterator(flushed.begin()), std::make_move_iterator(flushed.end()));
    }
    output_rows = project_rows(std::move(output_rows), plan.select);
    stats_.output_batches.fetch_add(1, std::memory_order_relaxed);
    stats_.output_rows.fetch_add(output_rows.size(), std::memory_order_relaxed);
    return rows_to_columns(output_rows);
}

py::dict NativeStream::aggregate_trade_numeric(
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> timestamps,
    py::array_t<uint8_t, py::array::c_style | py::array::forcecast> is_trade,
    py::array_t<int64_t, py::array::c_style | py::array::forcecast> symbol_id,
    py::array_t<double, py::array::c_style | py::array::forcecast> values,
    int64_t window_ms) {
    if (timestamps.ndim() != 1 || is_trade.ndim() != 1 || symbol_id.ndim() != 1 || values.ndim() != 1) {
        throw py::value_error("numeric columns must be one-dimensional");
    }
    const auto count = timestamps.shape(0);
    if (is_trade.shape(0) != count || symbol_id.shape(0) != count || values.shape(0) != count) {
        throw py::value_error("numeric columns must have equal length");
    }
    if (window_ms <= 0) {
        throw py::value_error("window_ms must be positive");
    }

    const int64_t* ts = timestamps.data();
    const uint8_t* trade = is_trade.data();
    const int64_t* sym = symbol_id.data();
    const double* val = values.data();
    std::unordered_map<uint64_t, NumericAggregate> aggregates;
    aggregates.reserve(static_cast<size_t>(count / 256 + 16));

    {
        py::gil_scoped_release release;
        for (py::ssize_t index = 0; index < count; ++index) {
            if (!trade[index]) {
                continue;
            }
            const int64_t bucket = ts[index] - (ts[index] % window_ms);
            const uint64_t key = (static_cast<uint64_t>(bucket) << 20) ^ static_cast<uint64_t>(sym[index] & 0xFFFFF);
            auto& aggregate = aggregates[key];
            aggregate.sum += val[index];
            aggregate.count += 1;
        }
    }

    std::vector<std::pair<uint64_t, NumericAggregate>> ordered;
    ordered.reserve(aggregates.size());
    for (auto& item : aggregates) {
        ordered.push_back(item);
    }
    std::sort(ordered.begin(), ordered.end(), [](const auto& left, const auto& right) {
        return left.first < right.first;
    });

    py::list window_start_out;
    py::list window_end_out;
    py::list symbol_id_out;
    py::list avg_value_out;
    py::list count_out;
    for (const auto& [key, aggregate] : ordered) {
        const int64_t bucket = static_cast<int64_t>(key >> 20);
        const int64_t group = static_cast<int64_t>(key & 0xFFFFF);
        window_start_out.append(bucket);
        window_end_out.append(bucket + window_ms);
        symbol_id_out.append(group);
        avg_value_out.append(aggregate.sum / static_cast<double>(aggregate.count));
        count_out.append(aggregate.count);
    }

    stats_.ingested.fetch_add(static_cast<uint64_t>(count), std::memory_order_relaxed);
    stats_.output_batches.fetch_add(1, std::memory_order_relaxed);
    stats_.output_rows.fetch_add(ordered.size(), std::memory_order_relaxed);

    py::dict out;
    out["window_start"] = std::move(window_start_out);
    out["window_end"] = std::move(window_end_out);
    out["symbol_id"] = std::move(symbol_id_out);
    out["avg_value"] = std::move(avg_value_out);
    out["count"] = std::move(count_out);
    return out;
}

void NativeStream::save_checkpoint(const std::string& path) {
    std::ofstream out(path);
    if (!out) {
        throw std::runtime_error("write checkpoint failed");
    }
    {
        std::lock_guard<std::mutex> lock(processor_mutex_);
        out << "max_event_time\t" << processor_.max_event_time << "\n";
        out << "window_origin\t" << processor_.window_origin << "\n";
        out << "seen_event_ids\t" << processor_.seen_event_ids.size() << "\n";
        for (const auto& id : processor_.seen_event_ids) {
            out << id << "\n";
        }
        out << "window_state\t" << processor_.window_state.size() << "\n";
        for (const auto& [key, aggregate] : processor_.window_state) {
            out << key << "\t" << aggregate.sum << "\t" << aggregate.count << "\t" << aggregate.window_start << "\t"
                << aggregate.window_end << "\t" << aggregate.group_value << "\n";
        }
    }
    stats_.checkpoint_saves.fetch_add(1, std::memory_order_relaxed);
}

void NativeStream::load_checkpoint(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("read checkpoint failed");
    }
    std::stringstream buffer;
    buffer << in.rdbuf();
    ProcessorState loaded;
    std::string label;
    buffer >> label >> loaded.max_event_time;
    buffer >> label >> loaded.window_origin;
    size_t seen_count = 0;
    buffer >> label >> seen_count;
    std::string line;
    std::getline(buffer, line);
    for (size_t i = 0; i < seen_count; ++i) {
        std::getline(buffer, line);
        loaded.seen_event_ids.insert(line);
    }
    size_t window_count = 0;
    buffer >> label >> window_count;
    std::getline(buffer, line);
    for (size_t i = 0; i < window_count; ++i) {
        std::getline(buffer, line);
        std::stringstream row(line);
        std::string key;
        WindowAggregate aggregate;
        std::getline(row, key, '\t');
        row >> aggregate.sum;
        row.ignore(1);
        row >> aggregate.count;
        row.ignore(1);
        row >> aggregate.window_start;
        row.ignore(1);
        row >> aggregate.window_end;
        row.ignore(1);
        std::getline(row, aggregate.group_value);
        loaded.window_state[key] = aggregate;
    }
    {
        std::lock_guard<std::mutex> lock(processor_mutex_);
        processor_ = std::move(loaded);
    }
    stats_.checkpoint_loads.fetch_add(1, std::memory_order_relaxed);
}

std::map<std::string, uint64_t> NativeStream::stats() const {
    std::lock_guard<std::mutex> lock(processor_mutex_);
    return {
        {"ingested", stats_.ingested.load(std::memory_order_relaxed)},
        {"dropped", stats_.dropped.load(std::memory_order_relaxed)},
        {"microbatches_emitted", stats_.microbatches_emitted.load(std::memory_order_relaxed)},
        {"output_batches", stats_.output_batches.load(std::memory_order_relaxed)},
        {"output_rows", stats_.output_rows.load(std::memory_order_relaxed)},
        {"duplicate_events", stats_.duplicate_events.load(std::memory_order_relaxed)},
        {"late_events", stats_.late_events.load(std::memory_order_relaxed)},
        {"checkpoint_saves", stats_.checkpoint_saves.load(std::memory_order_relaxed)},
        {"checkpoint_loads", stats_.checkpoint_loads.load(std::memory_order_relaxed)},
        {"open_windows", processor_.window_state.size()},
        {"seen_event_ids", processor_.seen_event_ids.size()},
    };
}

bool NativeStream::is_closed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return closed_;
}

void NativeStream::close() {
    stop(true);
}

void NativeStream::bind_udp() {
    socket_fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (socket_fd_ < 0) {
        throw std::runtime_error("udp socket create failed");
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (::inet_pton(AF_INET, address_.c_str(), &addr.sin_addr) != 1) {
        ::close(socket_fd_);
        socket_fd_ = -1;
        throw py::value_error("invalid IPv4 address");
    }
    if (::bind(socket_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(socket_fd_);
        socket_fd_ = -1;
        throw std::runtime_error("udp bind failed");
    }
    sockaddr_in local{};
    socklen_t len = sizeof(local);
    if (::getsockname(socket_fd_, reinterpret_cast<sockaddr*>(&local), &len) == 0) {
        char host[INET_ADDRSTRLEN] = {0};
        ::inet_ntop(AF_INET, &local.sin_addr, host, sizeof(host));
        address_ = host;
        port_ = ntohs(local.sin_port);
    }
    int flags = ::fcntl(socket_fd_, F_GETFL, 0);
    ::fcntl(socket_fd_, F_SETFL, flags | O_NONBLOCK);
}

void NativeStream::push_event(Row row) {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.ingested.fetch_add(1, std::memory_order_relaxed);
    if (queue_.size() >= capacity_) {
        stats_.dropped.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    queue_.push_back(std::move(row));
    cv_.notify_all();
}

void NativeStream::worker_loop() {
    auto last_flush = std::chrono::steady_clock::now();
    while (true) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                drain_queue_locked();
                cv_.notify_all();
                break;
            }
        }

        if (udp_) {
            receive_udp_once();
        }

        const auto now = std::chrono::steady_clock::now();
        if (now - last_flush >= std::chrono::milliseconds(linger_ms_)) {
            std::lock_guard<std::mutex> lock(mutex_);
            drain_queue_locked();
            last_flush = now;
        }

        std::this_thread::sleep_for(1ms);
    }
}

void NativeStream::receive_udp_once() {
    if (socket_fd_ < 0) return;
    char buffer[65536];
    sockaddr_in src{};
    socklen_t src_len = sizeof(src);
    ssize_t size = ::recvfrom(socket_fd_, buffer, sizeof(buffer), 0, reinterpret_cast<sockaddr*>(&src), &src_len);
    if (size <= 0) {
        return;
    }
    auto row = parse_flat_json_object(std::string(buffer, static_cast<size_t>(size)));
    if (row) {
        push_event(std::move(*row));
    }
}

void NativeStream::drain_queue_locked() {
    std::vector<Row> rows;
    rows.reserve(batch_size_);
    while (!queue_.empty() && rows.size() < batch_size_) {
        rows.push_back(std::move(queue_.front()));
        queue_.pop_front();
    }
    if (!rows.empty()) {
        pending_chunks_.push_back(std::move(rows));
        stats_.microbatches_emitted.fetch_add(1, std::memory_order_relaxed);
        cv_.notify_all();
    }
}

void NativeStream::stop(bool join) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_ && (!join || !worker_.joinable())) {
            return;
        }
        closed_ = true;
        cv_.notify_all();
    }
    if (socket_fd_ >= 0) {
        ::shutdown(socket_fd_, SHUT_RDWR);
    }
    if (join && worker_.joinable()) {
        if (PyGILState_Check()) {
            py::gil_scoped_release release;
            worker_.join();
        } else {
            worker_.join();
        }
    }
    if (socket_fd_ >= 0) {
        ::close(socket_fd_);
        socket_fd_ = -1;
    }
}
