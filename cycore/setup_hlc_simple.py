from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np

extensions = [
    Extension(
        "hlc_ts_simple",
        ["hlc_ts_simple.pyx"],
        include_dirs=[".", np.get_include()],
        language="c++",
        extra_compile_args=["-std=c++17", "-O3"]
    )
]

setup(
    ext_modules=cythonize(extensions, compiler_directives={'language_level': "3"})
)
