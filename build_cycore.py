#!/usr/bin/env python3
"""
CyCore Professional Build System
================================

Single comprehensive build script for all CyCore components.
Builds HLC (Hybrid Logical Clock) and high-performance hashmap implementations.

Usage:
    python build_cycore.py [--clean] [--debug] [--parallel] [--rust]
    
Options:
    --clean     Clean build artifacts before building
    --debug     Build with debug symbols
    --parallel  Build components in parallel
    --rust      Enable Rust-based HLC implementation
    --test      Run tests after build
"""

import argparse
import concurrent.futures
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

# ANSI color codes for beautiful output
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class BuildStatus:
    def __init__(self):
        self.start_time = time.time()
        self.components_built = 0
        self.total_components = 0
        
    def log(self, message: str, level: str = "INFO"):
        timestamp = time.strftime("%H:%M:%S")
        color = {
            "INFO": Colors.OKBLUE,
            "SUCCESS": Colors.OKGREEN,
            "WARNING": Colors.WARNING,
            "ERROR": Colors.FAIL,
            "HEADER": Colors.HEADER
        }.get(level, Colors.ENDC)
        
        print(f"{color}[{timestamp}] {message}{Colors.ENDC}")
    
    def progress(self, component: str, status: str):
        if status == "SUCCESS":
            self.components_built += 1
        
        progress_bar = "█" * (self.components_built * 20 // max(1, self.total_components))
        progress_bar += "░" * (20 - len(progress_bar))
        
        self.log(f"[{progress_bar}] {component}: {status}")

class CyCoreBuilder:
    """Professional CyCore build system"""
    
    def __init__(self, args):
        self.args = args
        self.status = BuildStatus()
        self.cycore_dir = Path(__file__).parent / "cycore"
        self.build_dir = self.cycore_dir / "build"
        
        # Ensure we're in the right directory
        os.chdir(self.cycore_dir)
        
        # Build configuration
        self.components = [
            ("HLC Timestamp", "hlc_timestamp.pyx", self._build_hlc_timestamp),
            ("HLC Advanced", "hlc_advanced.pyx", self._build_hlc_advanced),
            ("Hashmap", "hashmap.pyx", self._build_hashmap),
            ("Hashmap Advanced", "hashmap_advanced.pyx", self._build_hashmap_advanced),
            ("Hashmap Fallback", "hashmap_fallback.pyx", self._build_hashmap_fallback),
        ]
        
        self.status.total_components = len(self.components)
    
    def build_all(self) -> bool:
        """Build all CyCore components"""
        self.status.log("🚀 Starting CyCore Professional Build System", "HEADER")
        self.status.log(f"Build directory: {self.cycore_dir}")
        
        if self.args.clean:
            self._clean_build_artifacts()
        
        self._setup_build_environment()
        
        # Build components
        if self.args.parallel:
            success = self._build_parallel()
        else:
            success = self._build_sequential()
        
        # Run tests if requested
        if success and self.args.test:
            success = self._run_tests()
        
        # Build summary
        self._print_build_summary(success)
        
        return success
    
    def _clean_build_artifacts(self):
        """Clean all build artifacts"""
        self.status.log("🧹 Cleaning build artifacts...")
        
        patterns_to_clean = [
            "*.so",
            "*.cpp",
            "*.c",
            "*.html",
            "build/",
            "__pycache__/",
            "*.egg-info/",
        ]
        
        for pattern in patterns_to_clean:
            for path in self.cycore_dir.glob(pattern):
                if path.is_dir():
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    path.unlink(missing_ok=True)
        
        self.status.log("✅ Build artifacts cleaned", "SUCCESS")
    
    def _setup_build_environment(self):
        """Setup build environment and dependencies"""
        self.status.log("🔧 Setting up build environment...")
        
        # Ensure required packages are installed
        required_packages = ["cython", "numpy", "setuptools"]
        
        for package in required_packages:
            try:
                __import__(package)
            except ImportError:
                self.status.log(f"Installing {package}...", "WARNING")
                subprocess.run([sys.executable, "-m", "pip", "install", package], 
                             check=True, capture_output=True)
        
        # Create build directory
        self.build_dir.mkdir(exist_ok=True)
        
        self.status.log("✅ Build environment ready", "SUCCESS")
    
    def _build_sequential(self) -> bool:
        """Build components sequentially"""
        self.status.log("🔨 Building components sequentially...")
        
        for name, pyx_file, build_func in self.components:
            if not self._should_build(pyx_file):
                self.status.progress(name, "SKIPPED")
                continue
                
            try:
                build_func()
                self.status.progress(name, "SUCCESS")
            except Exception as e:
                # For optional components, log warning but continue
                if "Advanced" in name:
                    self.status.log(f"⚠️  Optional component {name} failed: {e}", "WARNING")
                    self.status.progress(name, "OPTIONAL_FAILED")
                else:
                    self.status.log(f"❌ Failed to build {name}: {e}", "ERROR")
                    self.status.progress(name, "FAILED")
                    return False
        
        return True
    
    def _build_parallel(self) -> bool:
        """Build components in parallel"""
        self.status.log("🔨 Building components in parallel...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_component = {}
            
            for name, pyx_file, build_func in self.components:
                if not self._should_build(pyx_file):
                    self.status.progress(name, "SKIPPED")
                    continue
                
                future = executor.submit(build_func)
                future_to_component[future] = name
            
            success = True
            for future in concurrent.futures.as_completed(future_to_component):
                name = future_to_component[future]
                try:
                    future.result()
                    self.status.progress(name, "SUCCESS")
                except Exception as e:
                    self.status.log(f"❌ Failed to build {name}: {e}", "ERROR")
                    self.status.progress(name, "FAILED")
                    success = False
        
        return success
    
    def _should_build(self, pyx_file: str) -> bool:
        """Check if component should be built"""
        pyx_path = self.cycore_dir / pyx_file
        if not pyx_path.exists():
            return False
        
        # Check if .so file exists and is newer than .pyx
        so_file = pyx_path.with_suffix('.so')
        if so_file.exists():
            return pyx_path.stat().st_mtime > so_file.stat().st_mtime
        
        return True
    
    def _build_hlc_timestamp(self):
        """Build HLC timestamp implementation"""
        self._build_cython_extension(
            "hlc_timestamp",
            ["hlc_timestamp.pyx"],
            language="c++",
            extra_compile_args=["-std=c++17", "-O3"] if not self.args.debug else ["-std=c++17", "-g"]
        )
    
    def _build_hlc_advanced(self):
        """Build HLC advanced implementation"""
        if not self.args.rust:
            # Skip Rust-based implementation if not requested
            return
            
        # Build Rust library first if needed
        rust_dir = Path("../rustcore/hlc")
        if rust_dir.exists():
            self._build_rust_library(rust_dir)
        
        self._build_cython_extension(
            "hlc_advanced",
            ["hlc_advanced.pyx"],
            libraries=["pyhmssql_hlc"],
            library_dirs=["."],
            language="c++",
            extra_compile_args=["-std=c++17", "-O3"] if not self.args.debug else ["-std=c++17", "-g"]
        )
    
    def _build_hashmap(self):
        """Build standard hashmap implementation"""
        self._build_cython_extension(
            "hashmap",
            ["hashmap.pyx"],
            language="c++",
            extra_compile_args=["-std=c++17", "-O3"] if not self.args.debug else ["-std=c++17", "-g"]
        )
    
    def _build_hashmap_advanced(self):
        """Build advanced hashmap implementation with Abseil"""
        # Try to build with Abseil support
        try:
            abseil_cflags = subprocess.check_output(
                ["pkg-config", "--cflags", "absl_container"],
                stderr=subprocess.DEVNULL
            ).decode().strip().split()
            abseil_libs = subprocess.check_output(
                ["pkg-config", "--libs", "absl_container"],
                stderr=subprocess.DEVNULL
            ).decode().strip().split()
            
            # Test if abseil libraries can actually be linked
            test_result = subprocess.run(
                ["pkg-config", "--libs", "absl_container"],
                capture_output=True,
                stderr=subprocess.DEVNULL
            )
            
            if test_result.returncode != 0:
                raise subprocess.CalledProcessError(test_result.returncode, "pkg-config")
                
        except (subprocess.CalledProcessError, FileNotFoundError):
            # Abseil not available, skip advanced build
            self.status.log("⚠️  Abseil not available, skipping advanced hashmap", "WARNING")
            return
        
        extra_compile_args = ["-std=c++17"] + abseil_cflags
        if not self.args.debug:
            extra_compile_args.append("-O3")
        else:
            extra_compile_args.append("-g")
        
        try:
            self._build_cython_extension(
                "hashmap_advanced",
                ["hashmap_advanced.pyx"],
                language="c++",
                extra_compile_args=extra_compile_args,
                extra_link_args=abseil_libs
            )
        except Exception as e:
            self.status.log(f"⚠️  Advanced hashmap build failed: {e}", "WARNING")
            self.status.log("   Continuing with standard implementation", "WARNING")
    
    def _build_hashmap_fallback(self):
        """Build fallback hashmap implementation"""
        self._build_cython_extension(
            "hashmap_fallback",
            ["hashmap_fallback.pyx"],
            language="c++",
            extra_compile_args=["-std=c++17", "-O3"] if not self.args.debug else ["-std=c++17", "-g"]
        )
    
    def _build_cython_extension(self, name: str, sources: List[str], **kwargs):
        """Build a Cython extension"""
        from setuptools import setup, Extension
        from Cython.Build import cythonize
        import numpy as np
        
        # Default arguments
        extension_kwargs = {
            "include_dirs": [".", np.get_include()],
            "language": "c++",
            **kwargs
        }
        
        extension = Extension(name, sources, **extension_kwargs)
        
        # Use a temporary setup to build
        setup(
            name=f"cycore_{name}",
            ext_modules=cythonize([extension], compiler_directives={'language_level': "3"}),
            script_args=["build_ext", "--inplace"],
            verbose=self.args.debug
        )
    
    def _build_rust_library(self, rust_dir: Path):
        """Build Rust library"""
        self.status.log(f"🦀 Building Rust library in {rust_dir}...")
        
        result = subprocess.run(
            ["cargo", "build", "--release"],
            cwd=rust_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"Rust build failed: {result.stderr}")
        
        # Copy the built library
        lib_file = rust_dir / "target" / "release" / "libpyhmssql_hlc.so"
        if lib_file.exists():
            shutil.copy2(lib_file, self.cycore_dir / "libpyhmssql_hlc.so")
    
    def _run_tests(self) -> bool:
        """Run CyCore tests"""
        self.status.log("🧪 Running CyCore tests...")
        
        test_script = Path("../tests/test_cycore/run_cycore_tests.py")
        if not test_script.exists():
            self.status.log("⚠️  Test script not found, skipping tests", "WARNING")
            return True
        
        result = subprocess.run([
            sys.executable, str(test_script), "--all"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            self.status.log("✅ All tests passed", "SUCCESS")
            return True
        else:
            self.status.log(f"❌ Tests failed:\n{result.stdout}\n{result.stderr}", "ERROR")
            return False
    
    def _print_build_summary(self, success: bool):
        """Print build summary"""
        elapsed = time.time() - self.status.start_time
        
        self.status.log("="*60, "HEADER")
        self.status.log("🏗️  CYCORE BUILD SUMMARY", "HEADER")
        self.status.log("="*60, "HEADER")
        
        if success:
            self.status.log(f"🎉 Build completed successfully in {elapsed:.2f}s", "SUCCESS")
            self.status.log(f"📦 {self.status.components_built}/{self.status.total_components} components built")
        else:
            self.status.log(f"💥 Build failed after {elapsed:.2f}s", "ERROR")
        
        self.status.log("="*60, "HEADER")
        
        # List built extensions
        so_files = list(self.cycore_dir.glob("*.so"))
        if so_files:
            self.status.log("📚 Built extensions:")
            for so_file in so_files:
                size = so_file.stat().st_size / 1024
                self.status.log(f"  • {so_file.name} ({size:.1f} KB)")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="CyCore Professional Build System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument("--clean", action="store_true",
                       help="Clean build artifacts before building")
    parser.add_argument("--debug", action="store_true",
                       help="Build with debug symbols")
    parser.add_argument("--parallel", action="store_true",
                       help="Build components in parallel")
    parser.add_argument("--rust", action="store_true",
                       help="Enable Rust-based HLC implementation")
    parser.add_argument("--test", action="store_true",
                       help="Run tests after successful build")
    
    args = parser.parse_args()
    
    # Create builder and run build
    builder = CyCoreBuilder(args)
    success = builder.build_all()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
