from __future__ import annotations

from pybind11.setup_helpers import Pybind11Extension, build_ext
from setuptools import setup


ext_modules = [
    Pybind11Extension(
        "hydrastream._pulse",
        [
            "src/core_processing.cpp",
            "src/native_stream.cpp",
            "src/pulse_module.cpp",
        ],
        include_dirs=["src"],
        cxx_std=17,
        extra_compile_args=["-O3", "-pthread"],
        extra_link_args=["-pthread"],
    )
]


setup(
    packages=["hydrastream"],
    package_dir={"": "python"},
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
)
