#!/usr/bin/env python3
"""
Build script for hyperoptimized sorting module.
Compiles Cython code with maximum optimization flags.
"""

from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np
import os

# Compiler optimization flags for maximum performance
extra_compile_args = [
    "-O3",                    # Maximum optimization
    "-march=native",          # Optimize for current CPU architecture
    "-mtune=native",          # Tune for current CPU
    "-ffast-math",           # Fast math operations
    "-funroll-loops",        # Unroll loops for better performance
    "-finline-functions",    # Inline functions
    "-fomit-frame-pointer",  # Omit frame pointer for better register usage
    "-msse4.2",              # Enable SSE 4.2 instructions
    "-mavx2",                # Enable AVX2 if available
    "-flto",                 # Link-time optimization
    "-fprofile-use",         # Use profile-guided optimization if available
    "-DNPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION",
]

# Linker flags
extra_link_args = [
    "-flto",                 # Link-time optimization
    "-Wl,-O3",              # Linker optimization
]

# Extensions to build
extensions = [
    Extension(
        "hyperoptimized_sort",
        sources=["hyperoptimized_sort.pyx"],
        include_dirs=[np.get_include()],
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
        language="c++",
        define_macros=[
            ("NPY_NO_DEPRECATED_API", "NPY_1_7_API_VERSION"),
            ("CYTHON_TRACE", "0"),  # Disable tracing for performance
        ],
    )
]

def build_extensions():
    """Build the Cython extensions with optimization."""
    print("Building hyperoptimized sorting module...")
    
    # Compile with Cython
    setup(
        name="hyperoptimized_sort",
        ext_modules=cythonize(
            extensions,
            compiler_directives={
                "boundscheck": False,      # Disable bounds checking
                "wraparound": False,       # Disable negative index wrapping
                "cdivision": True,         # Use C division semantics
                "nonecheck": False,        # Disable None checks
                "profile": False,          # Disable profiling
                "linetrace": False,        # Disable line tracing
                "binding": False,          # Disable binding
                "embedsignature": False,   # Don't embed signatures
                "optimize.use_switch": True,  # Use switch statements
                "optimize.unpack_method_calls": True,  # Optimize method calls
            },
            annotate=True,  # Generate HTML annotation files
        ),
        zip_safe=False,
    )

if __name__ == "__main__":
    build_extensions()
