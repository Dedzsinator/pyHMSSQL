from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np
import sys

# Determine compiler args based on platform
if sys.platform.startswith('win'):
    extra_compile_args = ["/O2"]
    extra_link_args = []
else:
    extra_compile_args = ["-O3", "-ffast-math"]
    extra_link_args = []

# Define the extension
extensions = [
    Extension(
        "bptree_optimized",
        ["bptree_optimized.pyx"],
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
        language="c"  # Use C instead of C++ for simpler building
    )
]

setup(
    name="bptree_optimized",
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            "language_level": 3,
            "boundscheck": False,
            "wraparound": False,
            "initializedcheck": False,
            "cdivision": True,
        },
    ),
)