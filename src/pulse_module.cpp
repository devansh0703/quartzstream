#include "core.hpp"

PYBIND11_MODULE(_pulse, module) {
    py::class_<NativeStream>(module, "NativeStream")
        .def_static("udp", &NativeStream::udp, py::arg("host"), py::arg("port"), py::arg("batch_size"), py::arg("linger_ms"), py::arg("capacity"))
        .def_static("manual", &NativeStream::manual, py::arg("batch_size"), py::arg("linger_ms"), py::arg("capacity"))
        .def("address", &NativeStream::address)
        .def("emit_json", &NativeStream::emit_json)
        .def("emit_columns", &NativeStream::emit_columns)
        .def("next_batch", &NativeStream::next_batch, py::arg("plan"), py::arg("timeout_ms"))
        .def("aggregate_columns", &NativeStream::aggregate_columns, py::arg("columns"), py::arg("plan"))
        .def("aggregate_trade_numeric", &NativeStream::aggregate_trade_numeric, py::arg("timestamps"), py::arg("is_trade"), py::arg("symbol_id"), py::arg("values"), py::arg("window_ms"))
        .def("save_checkpoint", &NativeStream::save_checkpoint)
        .def("load_checkpoint", &NativeStream::load_checkpoint)
        .def("stats", &NativeStream::stats)
        .def("is_closed", &NativeStream::is_closed)
        .def("close", &NativeStream::close);
}
