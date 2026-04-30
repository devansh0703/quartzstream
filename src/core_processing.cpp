#include "core.hpp"

namespace {

std::string trim(std::string value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

std::string unquote(const std::string& value) {
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
        std::string out;
        out.reserve(value.size() - 2);
        for (size_t i = 1; i + 1 < value.size(); ++i) {
            if (value[i] == '\\' && i + 2 < value.size()) {
                ++i;
            }
            out.push_back(value[i]);
        }
        return out;
    }
    return value;
}

Cell parse_scalar(std::string value) {
    value = trim(std::move(value));
    if (value == "null") return Null{};
    if (value == "true") return true;
    if (value == "false") return false;
    if (!value.empty() && value.front() == '"') return unquote(value);
    try {
        if (value.find('.') != std::string::npos || value.find('e') != std::string::npos || value.find('E') != std::string::npos) {
            return std::stod(value);
        }
        return static_cast<int64_t>(std::stoll(value));
    } catch (const std::exception&) {
        return value;
    }
}

bool compare_ord(double left, const std::string& op, double right) {
    if (op == "==") return left == right;
    if (op == "!=") return left != right;
    if (op == ">") return left > right;
    if (op == ">=") return left >= right;
    if (op == "<") return left < right;
    if (op == "<=") return left <= right;
    return false;
}

enum class ColumnType { Bool, Int, Float, String };

ColumnType cell_type(const Cell& cell) {
    if (std::holds_alternative<bool>(cell)) return ColumnType::Bool;
    if (std::holds_alternative<int64_t>(cell)) return ColumnType::Int;
    if (std::holds_alternative<double>(cell)) return ColumnType::Float;
    return ColumnType::String;
}

std::vector<std::pair<std::string, ColumnType>> infer_columns(const std::vector<Row>& rows) {
    std::map<std::string, ColumnType> order;
    for (const auto& row : rows) {
        for (const auto& [name, cell] : row) {
            auto type = cell_type(cell);
            auto [it, inserted] = order.emplace(name, type);
            if (!inserted && it->second != type) {
                if ((it->second == ColumnType::Int && type == ColumnType::Float) ||
                    (it->second == ColumnType::Float && type == ColumnType::Int)) {
                    it->second = ColumnType::Float;
                } else if (!std::holds_alternative<Null>(cell)) {
                    it->second = ColumnType::String;
                }
            }
        }
    }
    return {order.begin(), order.end()};
}

}  // namespace

int64_t now_millis() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}

bool is_none(const py::handle& obj) {
    return obj.is_none();
}

Cell cell_from_py(const py::handle& obj) {
    if (is_none(obj)) {
        return Null{};
    }
    if (py::isinstance<py::bool_>(obj)) {
        return obj.cast<bool>();
    }
    if (py::isinstance<py::int_>(obj)) {
        return obj.cast<int64_t>();
    }
    if (py::isinstance<py::float_>(obj)) {
        return obj.cast<double>();
    }
    if (py::isinstance<py::str>(obj)) {
        return obj.cast<std::string>();
    }
    return py::str(obj).cast<std::string>();
}

Row dict_to_row(const py::dict& payload) {
    Row row;
    for (auto item : payload) {
        row[py::str(item.first).cast<std::string>()] = cell_from_py(item.second);
    }
    if (row.find("timestamp") == row.end()) {
        row["timestamp"] = now_millis();
    }
    return row;
}

std::vector<Row> rows_from_columns(const py::dict& columns) {
    std::vector<std::string> names;
    std::vector<py::sequence> sequences;
    names.reserve(columns.size());
    sequences.reserve(columns.size());
    size_t row_count = 0;
    bool first = true;
    for (auto item : columns) {
        names.push_back(py::str(item.first).cast<std::string>());
        sequences.push_back(py::reinterpret_borrow<py::sequence>(item.second));
        const size_t size = static_cast<size_t>(py::len(item.second));
        if (first) {
            row_count = size;
            first = false;
        } else if (size != row_count) {
            throw py::value_error("all columns must have the same length");
        }
    }

    std::vector<Row> rows;
    rows.reserve(row_count);
    for (size_t index = 0; index < row_count; ++index) {
        Row row;
        for (size_t col = 0; col < names.size(); ++col) {
            row[names[col]] = cell_from_py(sequences[col][py::int_(index)]);
        }
        if (row.find("timestamp") == row.end()) {
            row["timestamp"] = now_millis();
        }
        rows.push_back(std::move(row));
    }
    return rows;
}

std::optional<Row> parse_flat_json_object(const std::string& payload) {
    std::string text = trim(payload);
    if (text.size() < 2 || text.front() != '{' || text.back() != '}') {
        return std::nullopt;
    }
    Row row;
    size_t pos = 1;
    while (pos + 1 < text.size()) {
        while (pos < text.size() && (text[pos] == ' ' || text[pos] == ',')) ++pos;
        if (pos >= text.size() - 1) break;
        if (text[pos] != '"') return std::nullopt;
        const size_t key_start = pos++;
        bool escaped = false;
        while (pos < text.size()) {
            if (!escaped && text[pos] == '"') break;
            escaped = !escaped && text[pos] == '\\';
            if (text[pos] != '\\') escaped = false;
            ++pos;
        }
        if (pos >= text.size()) return std::nullopt;
        const std::string key = unquote(text.substr(key_start, pos - key_start + 1));
        ++pos;
        while (pos < text.size() && text[pos] != ':') ++pos;
        if (pos >= text.size()) return std::nullopt;
        ++pos;
        while (pos < text.size() && std::isspace(static_cast<unsigned char>(text[pos]))) ++pos;
        const size_t value_start = pos;
        if (pos < text.size() && text[pos] == '"') {
            ++pos;
            escaped = false;
            while (pos < text.size()) {
                if (!escaped && text[pos] == '"') break;
                escaped = !escaped && text[pos] == '\\';
                if (text[pos] != '\\') escaped = false;
                ++pos;
            }
            if (pos < text.size()) ++pos;
        } else {
            while (pos < text.size() && text[pos] != ',' && text[pos] != '}') ++pos;
        }
        row[key] = parse_scalar(text.substr(value_start, pos - value_start));
        while (pos < text.size() && text[pos] != ',' && text[pos] != '}') ++pos;
        if (pos < text.size() && text[pos] == ',') ++pos;
    }
    if (row.find("timestamp") == row.end()) {
        row["timestamp"] = now_millis();
    }
    return row;
}

std::optional<double> to_f64(const Cell& cell) {
    if (auto value = std::get_if<int64_t>(&cell)) {
        return static_cast<double>(*value);
    }
    if (auto value = std::get_if<double>(&cell)) {
        return *value;
    }
    return std::nullopt;
}

std::optional<int64_t> to_i64(const Cell& cell) {
    if (auto value = std::get_if<int64_t>(&cell)) {
        return *value;
    }
    return std::nullopt;
}

std::string display(const Cell& cell) {
    if (std::holds_alternative<Null>(cell)) {
        return "";
    }
    if (auto value = std::get_if<bool>(&cell)) {
        return *value ? "true" : "false";
    }
    if (auto value = std::get_if<int64_t>(&cell)) {
        return std::to_string(*value);
    }
    if (auto value = std::get_if<double>(&cell)) {
        std::ostringstream out;
        out << *value;
        return out.str();
    }
    return std::get<std::string>(cell);
}

bool compare_cells(const Cell& left, const std::string& op, const Cell& right) {
    auto left_num = to_f64(left);
    auto right_num = to_f64(right);
    if (left_num && right_num) {
        return compare_ord(*left_num, op, *right_num);
    }
    if (auto l = std::get_if<std::string>(&left)) {
        if (auto r = std::get_if<std::string>(&right)) {
            if (op == "==") return *l == *r;
            if (op == "!=") return *l != *r;
        }
    }
    if (auto l = std::get_if<bool>(&left)) {
        if (auto r = std::get_if<bool>(&right)) {
            if (op == "==") return *l == *r;
            if (op == "!=") return *l != *r;
        }
    }
    return false;
}

bool passes_filters(const Row& row, const std::vector<FilterSpec>& filters) {
    for (const auto& spec : filters) {
        auto found = row.find(spec.field);
        if (found == row.end() || !compare_cells(found->second, spec.op, spec.value)) {
            return false;
        }
    }
    return true;
}

std::vector<Row> project_rows(std::vector<Row> rows, const std::optional<std::vector<std::string>>& select) {
    if (!select) {
        return rows;
    }
    std::vector<Row> projected;
    projected.reserve(rows.size());
    for (const auto& row : rows) {
        Row out;
        for (const auto& field : *select) {
            auto found = row.find(field);
            out[field] = found == row.end() ? Cell{Null{}} : found->second;
        }
        projected.push_back(std::move(out));
    }
    return projected;
}

py::dict rows_to_columns(const std::vector<Row>& rows) {
    py::dict arrays;
    for (const auto& [name, type] : infer_columns(rows)) {
        py::list values;
        for (const auto& row : rows) {
            auto found = row.find(name);
            if (found == row.end() || std::holds_alternative<Null>(found->second)) {
                values.append(py::none());
                continue;
            }
            const auto& cell = found->second;
            switch (type) {
                case ColumnType::Bool:
                    values.append(std::get<bool>(cell));
                    break;
                case ColumnType::Int:
                    values.append(std::get<int64_t>(cell));
                    break;
                case ColumnType::Float:
                    if (auto value = std::get_if<double>(&cell)) {
                        values.append(*value);
                    } else if (auto value = std::get_if<int64_t>(&cell)) {
                        values.append(static_cast<double>(*value));
                    } else {
                        values.append(py::none());
                    }
                    break;
                case ColumnType::String:
                    values.append(display(cell));
                    break;
            }
        }
        arrays[py::str(name)] = values;
    }
    return arrays;
}

PlanSpec parse_plan(const py::dict& plan_dict) {
    PlanSpec plan;

    for (auto filter_obj : plan_dict["filters"].cast<py::list>()) {
        auto filter = filter_obj.cast<py::dict>();
        plan.filters.push_back(FilterSpec{
            filter["field"].cast<std::string>(),
            filter["op"].cast<std::string>(),
            cell_from_py(filter["value"]),
        });
    }

    if (!is_none(plan_dict["select"])) {
        std::vector<std::string> fields;
        for (auto item : plan_dict["select"].cast<py::list>()) {
            fields.push_back(item.cast<std::string>());
        }
        plan.select = std::move(fields);
    }

    if (!is_none(plan_dict["window"])) {
        auto win = plan_dict["window"].cast<py::dict>();
        WindowSpec spec;
        spec.seconds = win["seconds"].cast<int64_t>();
        if (!is_none(win["by"])) {
            spec.by = win["by"].cast<std::string>();
        }
        spec.time_field = win["time_field"].cast<std::string>();
        plan.window = std::move(spec);
    }

    if (!is_none(plan_dict["mean"])) {
        auto mean = plan_dict["mean"].cast<py::dict>();
        plan.mean = MeanSpec{mean["field"].cast<std::string>(), mean["alias"].cast<std::string>()};
    }

    if (!is_none(plan_dict["watermark_ms"])) {
        plan.watermark_ms = plan_dict["watermark_ms"].cast<int64_t>();
    }
    if (!is_none(plan_dict["allowed_lateness_ms"])) {
        plan.allowed_lateness_ms = plan_dict["allowed_lateness_ms"].cast<int64_t>();
    }
    if (!is_none(plan_dict["event_id_field"])) {
        plan.event_id_field = plan_dict["event_id_field"].cast<std::string>();
    }
    if (!is_none(plan_dict["drop_late"])) {
        plan.drop_late = plan_dict["drop_late"].cast<bool>();
    }
    return plan;
}

int64_t plan_watermark(const PlanSpec& plan, const ProcessorState& state) {
    return state.max_event_time - plan.watermark_ms.value_or(0);
}

int64_t window_bucket_for(ProcessorState& state, int64_t event_time, int64_t bucket_ms) {
    if (event_time > 1'000'000'000'000LL) {
        if (state.window_origin == std::numeric_limits<int64_t>::min()) {
            state.window_origin = event_time;
        }
        return state.window_origin + ((event_time - state.window_origin) / bucket_ms) * bucket_ms;
    }
    return event_time - (event_time % bucket_ms);
}

std::vector<Row> process_rows(Stats& stats, ProcessorState& state, std::vector<Row> rows, const PlanSpec& plan) {
    std::vector<Row> immediate_rows;
    const int64_t allowed_lateness = std::max<int64_t>(0, plan.allowed_lateness_ms.value_or(0));

    for (auto& row : rows) {
        if (!passes_filters(row, plan.filters)) {
            continue;
        }

        if (plan.event_id_field) {
            auto found = row.find(*plan.event_id_field);
            if (found != row.end()) {
                auto event_id = display(found->second);
                if (!event_id.empty() && !state.seen_event_ids.insert(event_id).second) {
                    stats.duplicate_events.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }
            }
        }

        std::string time_field = plan.window ? plan.window->time_field : "timestamp";
        auto time_found = row.find(time_field);
        int64_t event_time = time_found == row.end() ? now_millis() : to_i64(time_found->second).value_or(now_millis());
        state.max_event_time = std::max(state.max_event_time, event_time);

        const int64_t active_watermark = plan_watermark(plan, state);
        if (active_watermark > std::numeric_limits<int64_t>::min() / 2 &&
            event_time + allowed_lateness < active_watermark) {
            stats.late_events.fetch_add(1, std::memory_order_relaxed);
            if (plan.drop_late) {
                continue;
            }
        }

        if (plan.window && plan.mean) {
            const int64_t bucket_ms = plan.window->seconds * 1000;
            const int64_t bucket = window_bucket_for(state, event_time, bucket_ms);
            std::string group;
            if (plan.window->by) {
                auto found = row.find(*plan.window->by);
                if (found != row.end()) {
                    group = display(found->second);
                }
            }
            auto value_found = row.find(plan.mean->field);
            if (value_found != row.end()) {
                auto numeric = to_f64(value_found->second);
                if (numeric) {
                    const std::string key = std::to_string(bucket) + "|" + group;
                    auto& aggregate = state.window_state[key];
                    aggregate.window_start = bucket;
                    aggregate.window_end = bucket + bucket_ms;
                    aggregate.group_value = group;
                    aggregate.sum += *numeric;
                    aggregate.count += 1;
                }
            }
        } else {
            immediate_rows.push_back(std::move(row));
        }
    }

    if (plan.window && plan.mean) {
        const int64_t finalize_before = plan_watermark(plan, state) - std::max<int64_t>(0, plan.allowed_lateness_ms.value_or(0));
        std::vector<std::string> ready_keys;
        for (const auto& [key, aggregate] : state.window_state) {
            if (aggregate.window_end <= finalize_before) {
                ready_keys.push_back(key);
            }
        }
        for (const auto& key : ready_keys) {
            auto node = state.window_state.extract(key);
            if (!node.empty() && node.mapped().count > 0) {
                const auto& aggregate = node.mapped();
                Row out;
                out["window_start"] = aggregate.window_start;
                out["window_end"] = aggregate.window_end;
                if (plan.window->by) {
                    out[*plan.window->by] = aggregate.group_value;
                }
                out[plan.mean->alias] = aggregate.sum / static_cast<double>(aggregate.count);
                out["count"] = static_cast<int64_t>(aggregate.count);
                immediate_rows.push_back(std::move(out));
            }
        }
    }

    return immediate_rows;
}

std::vector<Row> flush_all_windows(ProcessorState& state, const PlanSpec& plan) {
    std::vector<Row> rows;
    if (!(plan.window && plan.mean)) {
        return rows;
    }
    std::vector<std::string> keys;
    for (const auto& [key, _] : state.window_state) {
        keys.push_back(key);
    }
    for (const auto& key : keys) {
        auto node = state.window_state.extract(key);
        if (!node.empty() && node.mapped().count > 0) {
            const auto& aggregate = node.mapped();
            Row out;
            out["window_start"] = aggregate.window_start;
            out["window_end"] = aggregate.window_end;
            if (plan.window->by) {
                out[*plan.window->by] = aggregate.group_value;
            }
            out[plan.mean->alias] = aggregate.sum / static_cast<double>(aggregate.count);
            out["count"] = static_cast<int64_t>(aggregate.count);
            rows.push_back(std::move(out));
        }
    }
    return rows;
}
