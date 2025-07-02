"""
Setup script for building the optimized B+ tree Cython extension.
"""

from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np
import os

# Compiler optimization flags
extra_compile_args = [
    "-O3",
    "-ffast-math",
    "-march=native",
    "-mtune=native",
    "-ftree-vectorize",
]
extra_link_args = ["-O3"]

# Include directories
include_dirs = [np.get_include()]

# Define the extension
extensions = [
    Extension(
        "bptree",
        ["bptree.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
        language="c",
    )
]

# Build configuration
setup(
    name="bptree",
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            "language_level": 3,
            "boundscheck": False,
            "wraparound": False,
            "initializedcheck": False,
            "cdivision": True,
            "embedsignature": True,
        },
        annotate=True,  # Generate HTML annotation files for optimization analysis
    ),
    zip_safe=False,
)
