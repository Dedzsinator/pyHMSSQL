
from setuptools import setup
from Cython.Build import cythonize
from distutils.extension import Extension
import numpy as np

extensions = [
    Extension(
        "swiss_map",
        ["swiss_map_fallback.pyx"],
        include_dirs=[".", np.get_include()],
        language="c++",
        extra_compile_args=["-std=c++17"]
    )
]

setup(
    ext_modules=cythonize(extensions, compiler_directives={'language_level': "3"})
)
