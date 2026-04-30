#pragma once

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace py = pybind11;

struct Null {};
using Cell = std::variant<Null, bool, int64_t, double, std::string>;
using Row = std::map<std::string, Cell>;

struct FilterSpec {
    std::string field;
    std::string op;
    Cell value;
};

struct WindowSpec {
    int64_t seconds = 0;
    std::optional<std::string> by;
    std::string time_field = "timestamp";
};

struct MeanSpec {
    std::string field;
    std::string alias;
};

struct PlanSpec {
    std::vector<FilterSpec> filters;
    std::optional<std::vector<std::string>> select;
    std::optional<WindowSpec> window;
    std::optional<MeanSpec> mean;
    std::optional<int64_t> watermark_ms;
    std::optional<int64_t> allowed_lateness_ms;
    std::optional<std::string> event_id_field;
    bool drop_late = true;
};

struct Stats {
    std::atomic<uint64_t> ingested{0};
    std::atomic<uint64_t> dropped{0};
    std::atomic<uint64_t> microbatches_emitted{0};
    std::atomic<uint64_t> output_batches{0};
    std::atomic<uint64_t> output_rows{0};
    std::atomic<uint64_t> duplicate_events{0};
    std::atomic<uint64_t> late_events{0};
    std::atomic<uint64_t> checkpoint_saves{0};
    std::atomic<uint64_t> checkpoint_loads{0};
};

struct WindowAggregate {
    double sum = 0.0;
    uint64_t count = 0;
    int64_t window_start = 0;
    int64_t window_end = 0;
    std::string group_value;
};

struct ProcessorState {
    int64_t max_event_time = std::numeric_limits<int64_t>::min();
    int64_t window_origin = std::numeric_limits<int64_t>::min();
    std::set<std::string> seen_event_ids;
    std::map<std::string, WindowAggregate> window_state;
};

struct NumericAggregate {
    double sum = 0.0;
    int64_t count = 0;
};

int64_t now_millis();
bool is_none(const py::handle& obj);
Cell cell_from_py(const py::handle& obj);
Row dict_to_row(const py::dict& payload);
std::vector<Row> rows_from_columns(const py::dict& columns);
std::optional<double> to_f64(const Cell& cell);
std::optional<int64_t> to_i64(const Cell& cell);
std::string display(const Cell& cell);
bool compare_cells(const Cell& left, const std::string& op, const Cell& right);
bool passes_filters(const Row& row, const std::vector<FilterSpec>& filters);
std::vector<Row> project_rows(std::vector<Row> rows, const std::optional<std::vector<std::string>>& select);
py::dict rows_to_columns(const std::vector<Row>& rows);
PlanSpec parse_plan(const py::dict& plan_dict);
int64_t plan_watermark(const PlanSpec& plan, const ProcessorState& state);
int64_t window_bucket_for(ProcessorState& state, int64_t event_time, int64_t bucket_ms);
std::vector<Row> process_rows(Stats& stats, ProcessorState& state, std::vector<Row> rows, const PlanSpec& plan);
std::vector<Row> flush_all_windows(ProcessorState& state, const PlanSpec& plan);
std::optional<Row> parse_flat_json_object(const std::string& payload);

class NativeStream {
public:
    static std::unique_ptr<NativeStream> udp(const std::string& host, uint16_t port, size_t batch_size, uint64_t linger_ms, size_t capacity);
    static std::unique_ptr<NativeStream> manual(size_t batch_size, uint64_t linger_ms, size_t capacity);

    NativeStream(NativeStream&& other) = delete;
    NativeStream& operator=(NativeStream&& other) = delete;
    NativeStream(const NativeStream&) = delete;
    NativeStream& operator=(const NativeStream&) = delete;
    ~NativeStream();

    std::pair<std::string, uint16_t> address() const;
    void emit_json(const py::dict& payload);
    void emit_columns(const py::dict& columns);
    py::object next_batch(const py::dict& plan_dict, uint64_t timeout_ms);
    py::dict aggregate_columns(const py::dict& columns, const py::dict& plan_dict);
    py::dict aggregate_trade_numeric(
        py::array_t<int64_t, py::array::c_style | py::array::forcecast> timestamps,
        py::array_t<uint8_t, py::array::c_style | py::array::forcecast> is_trade,
        py::array_t<int64_t, py::array::c_style | py::array::forcecast> symbol_id,
        py::array_t<double, py::array::c_style | py::array::forcecast> values,
        int64_t window_ms);
    void save_checkpoint(const std::string& path);
    void load_checkpoint(const std::string& path);
    std::map<std::string, uint64_t> stats() const;
    bool is_closed() const;
    void close();

private:
    NativeStream(std::string host, uint16_t requested_port, size_t batch_size, uint64_t linger_ms, size_t capacity, bool udp);

    void bind_udp();
    void push_event(Row row);
    void worker_loop();
    void receive_udp_once();
    void drain_queue_locked();
    void stop(bool join);

    std::string address_;
    uint16_t port_ = 0;
    size_t batch_size_ = 1;
    uint64_t linger_ms_ = 0;
    size_t capacity_ = 1;
    bool udp_ = false;
    int socket_fd_ = -1;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    bool closed_ = false;
    std::deque<Row> queue_;
    std::deque<std::vector<Row>> pending_chunks_;
    std::thread worker_;

    mutable std::mutex processor_mutex_;
    ProcessorState processor_;
    mutable Stats stats_;
};
