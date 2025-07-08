# Build script for CyCore Cython extensions
import os
import subprocess
import sys
from pathlib import Path

def build_hlc():
    """Build HLC Rust library and Cython wrapper"""
    print("Building HLC (Hybrid Logical Clock)...")
    
    # Build Rust HLC library first
    rust_dir = Path("rustcore/hlc")
    if rust_dir.exists():
        os.chdir(rust_dir)
        result = subprocess.run(["cargo", "build", "--release"], capture_output=True)
        if result.returncode != 0:
            print(f"Rust build failed: {result.stderr.decode()}")
            return False
        os.chdir("../..")
    
    # Create setup.py for HLC
    hlc_setup = '''
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
'''
    
    with open("setup_hlc.py", "w") as f:
        f.write(hlc_setup)
    
    # Build Cython extension
    result = subprocess.run([
        sys.executable, "setup_hlc.py", "build_ext", "--inplace"
    ], capture_output=True)
    
    if result.returncode == 0:
        print("✓ HLC built successfully")
        return True
    else:
        print(f"✗ HLC build failed: {result.stderr.decode()}")
        return False

def build_swiss_map():
    """Build Swiss Table Cython wrapper"""
    print("Building Swiss Table...")
    
    # First, check if we need to install abseil-cpp
    check_abseil = subprocess.run(
        ["pkg-config", "--exists", "absl_container"], 
        capture_output=True
    )
    
    if check_abseil.returncode != 0:
        print("Installing Abseil library...")
        # Try to install abseil via package manager
        install_result = subprocess.run([
            "sudo", "apt-get", "install", "-y", "libabsl-dev"
        ], capture_output=True)
        
        if install_result.returncode != 0:
            print("Package manager install failed, building from source...")
            # Alternative: build from source or use header-only fallback
            return build_swiss_map_fallback()
    
    # Create setup.py for Swiss Map with proper Abseil linking
    swiss_setup = '''
from setuptools import setup
from Cython.Build import cythonize
from distutils.extension import Extension
import numpy as np
import subprocess

# Try to get abseil flags
try:
    abseil_cflags = subprocess.check_output(["pkg-config", "--cflags", "absl_container"]).decode().strip().split()
    abseil_libs = subprocess.check_output(["pkg-config", "--libs", "absl_container"]).decode().strip().split()
except:
    # Fallback to standard paths
    abseil_cflags = ["-I/usr/include", "-I/usr/local/include"]
    abseil_libs = ["-labsl_hash", "-labsl_container"]

extensions = [
    Extension(
        "swiss_map",
        ["swiss_map.pyx"],
        include_dirs=[".", np.get_include()],
        language="c++",
        extra_compile_args=["-std=c++17"] + abseil_cflags,
        extra_link_args=abseil_libs
    )
]

setup(
    ext_modules=cythonize(extensions, compiler_directives={'language_level': "3"})
)
'''
    
    with open("setup_swiss.py", "w") as f:
        f.write(swiss_setup)
    
    # Build Cython extension
    result = subprocess.run([
        sys.executable, "setup_swiss.py", "build_ext", "--inplace"
    ], capture_output=True)
    
    if result.returncode == 0:
        print("✓ Swiss Table built successfully")
        return True
    else:
        print(f"✗ Swiss Table build failed: {result.stderr.decode()}")
        return False

def build_swiss_map_fallback():
    """Build Swiss Table with header-only fallback"""
    print("Building Swiss Table with fallback implementation...")
    
    # Create a simplified header-only version
    fallback_setup = '''
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
'''
    
    with open("setup_swiss_fallback.py", "w") as f:
        f.write(fallback_setup)
    
    # Create fallback implementation using std::unordered_map
    create_fallback_swiss_map()
    
    result = subprocess.run([
        sys.executable, "setup_swiss_fallback.py", "build_ext", "--inplace"
    ], capture_output=True)
    
    if result.returncode == 0:
        print("✓ Swiss Table fallback built successfully")
        return True
    else:
        print(f"✗ Swiss Table fallback build failed: {result.stderr.decode()}")
        return False

def create_fallback_swiss_map():
    """Create a fallback Swiss Map implementation using std::unordered_map"""
    fallback_code = '''# distutils: language = c++
# cython: language_level=3

"""
Fallback Swiss Table implementation using std::unordered_map.
Used when Google Abseil is not available.
"""

from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp cimport bool
from libc.stdint cimport uint64_t
from cython.operator cimport dereference, preincrement
import json

# String-to-string map for general purpose use
cdef class SwissMap:
    """
    High-performance hash map fallback using std::unordered_map.
    """
    cdef unordered_map[string, string] _map
    
    def __setitem__(self, key, value):
        """Set map[key] = value"""
        cdef string ckey = str(key).encode('utf-8')
        cdef string cvalue = str(value).encode('utf-8')
        self._map[ckey] = cvalue
    
    def __getitem__(self, key):
        """Get map[key]"""
        cdef string ckey = str(key).encode('utf-8')
        if self._map.find(ckey) == self._map.end():
            raise KeyError(key)
        return self._map[ckey].decode('utf-8')
    
    def __delitem__(self, key):
        """Delete map[key]"""
        cdef string ckey = str(key).encode('utf-8')
        if self._map.erase(ckey) == 0:
            raise KeyError(key)
    
    def __contains__(self, key):
        """Check if key in map"""
        cdef string ckey = str(key).encode('utf-8')
        return self._map.find(ckey) != self._map.end()
    
    def __len__(self):
        """Get map size"""
        return self._map.size()
    
    def get(self, key, default=None):
        """Get value with default"""
        cdef string ckey = str(key).encode('utf-8')
        if self._map.find(ckey) != self._map.end():
            return self._map[ckey].decode('utf-8')
        return default
    
    def clear(self):
        """Clear all entries"""
        self._map.clear()
    
    def keys(self):
        """Get all keys"""
        cdef vector[string] result
        cdef unordered_map[string, string].iterator it = self._map.begin()
        cdef unordered_map[string, string].iterator end_it = self._map.end()
        while it != end_it:
            result.push_back(dereference(it).first)
            preincrement(it)
        
        return [key.decode('utf-8') for key in result]

# Simplified range map
cdef class RangeMap:
    """Range-optimized map for uint64 -> string mappings"""
    cdef unordered_map[uint64_t, string] _map
    
    def __setitem__(self, uint64_t key, value):
        cdef string cvalue = str(value).encode('utf-8')
        self._map[key] = cvalue
    
    def __getitem__(self, uint64_t key):
        if self._map.find(key) == self._map.end():
            raise KeyError(key)
        return self._map[key].decode('utf-8')
    
    def __contains__(self, uint64_t key):
        return self._map.find(key) != self._map.end()
    
    def __len__(self):
        return self._map.size()
    
    def clear(self):
        self._map.clear()

# Simplified tombstone map
cdef class TombstoneMap:
    """Tombstone tracking map"""
    cdef unordered_map[string, uint64_t] _map
    
    def mark_deleted(self, key, uint64_t timestamp):
        cdef string ckey = str(key).encode('utf-8')
        self._map[ckey] = timestamp
    
    def is_deleted(self, key, uint64_t after_timestamp):
        cdef string ckey = str(key).encode('utf-8')
        if self._map.find(ckey) == self._map.end():
            return False
        return self._map[ckey] > after_timestamp
    
    def __len__(self):
        return self._map.size()
    
    def clear(self):
        self._map.clear()
'''
    
    with open("swiss_map_fallback.pyx", "w") as f:
        f.write(fallback_code)

def main():
    """Build all CyCore modules"""
    print("Building CyCore performance modules...")
    
    success = True
    
    # Build simple HLC (reliable)
    print("Building Simple HLC...")
    result = subprocess.run([
        sys.executable, "setup_hlc_simple.py", "build_ext", "--inplace"
    ], capture_output=True)
    
    if result.returncode == 0:
        print("✓ Simple HLC built successfully")
    else:
        print(f"✗ Simple HLC build failed: {result.stderr.decode()}")
        success = False
    
    # Build simple Swiss Map (reliable)
    print("Building Simple Swiss Map...")
    result = subprocess.run([
        sys.executable, "setup_swiss_simple.py", "build_ext", "--inplace"
    ], capture_output=True)
    
    if result.returncode == 0:
        print("✓ Simple Swiss Map built successfully")
    else:
        print(f"✗ Simple Swiss Map build failed: {result.stderr.decode()}")
        success = False
    
    # Try to build Rust HLC if library exists
    if os.path.exists("libpyhmssql_hlc.so"):
        print("Building Rust HLC wrapper...")
        result = subprocess.run([
            sys.executable, "setup_hlc.py", "build_ext", "--inplace"
        ], capture_output=True)
        
        if result.returncode == 0:
            print("✓ Rust HLC wrapper built successfully")
        else:
            print(f"⚠ Rust HLC wrapper failed, using simple fallback: {result.stderr.decode()}")
    else:
        print("⚠ Rust library not found, using simple fallback")
    
    if success:
        print("✓ Core CyCore modules built successfully")
    else:
        print("✗ Some modules failed to build")
        sys.exit(1)

if __name__ == "__main__":
    main()
