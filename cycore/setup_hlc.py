
from setuptools import setup
from Cython.Build import cythonize
from distutils.extension import Extension
import numpy as np

extensions = [
    Extension(
        "hlc_ts",
        ["hlc_ts.pyx"],
        libraries=["pyhmssql_hlc"],
        library_dirs=["."],
        include_dirs=[".", np.get_include()],
        language="c++"
    )
]

setup(
    ext_modules=cythonize(extensions, compiler_directives={'language_level': "3"})
)
