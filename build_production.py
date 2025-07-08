#!/usr/bin/env python3
"""
Production Build Script for pyHMSSQL with Cross-Language Extensions

This script builds and compiles all components:
1. Rust HLC library (rustcore/hlc)
2. Cython extensions (cycore/*.pyx) 
3. Swiss Table C++ bindings
4. Production optimizations and packaging

Requirements:
- Rust toolchain (cargo, rustc)
- Python development headers
- Cython
- C++ compiler (g++ or clang++)
- pkg-config
"""

import os
import sys
import subprocess
import shutil
import platform
from pathlib import Path


class ProductionBuilder:
    """Production build orchestrator"""
    
    def __init__(self, workspace_root: str):
        self.workspace_root = Path(workspace_root)
        self.rust_dir = self.workspace_root / "rustcore"
        self.cycore_dir = self.workspace_root / "cycore"
        self.build_dir = self.workspace_root / "build"
        
        # Build configuration
        self.is_release = "--release" in sys.argv
        self.verbose = "--verbose" in sys.argv or "-v" in sys.argv
        
        print(f"🏗️  PyHMSSQL Production Builder")
        print(f"📁 Workspace: {self.workspace_root}")
        print(f"🎯 Mode: {'Release' if self.is_release else 'Debug'}")
        print(f"🖥️  Platform: {platform.system()} {platform.machine()}")
    
    def check_dependencies(self):
        """Check all build dependencies"""
        print("\n🔍 Checking dependencies...")
        
        # Check Rust toolchain
        try:
            result = subprocess.run(["cargo", "--version"], capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Rust: {result.stdout.strip()}")
            else:
                raise FileNotFoundError("Cargo not found")
        except FileNotFoundError:
            print("❌ Rust toolchain not found. Install from https://rustup.rs/")
            return False
        
        # Check Python development headers
        try:
            import distutils.util
            import distutils.sysconfig
            python_include = distutils.sysconfig.get_python_inc()
            if os.path.exists(os.path.join(python_include, "Python.h")):
                print(f"✅ Python headers: {python_include}")
            else:
                raise FileNotFoundError("Python.h not found")
        except (ImportError, FileNotFoundError):
            print("❌ Python development headers not found. Install python3-dev")
            return False
        
        # Check Cython
        try:
            import Cython
            print(f"✅ Cython: {Cython.__version__}")
        except ImportError:
            print("❌ Cython not found. Install with: pip install Cython")
            return False
        
        # Check C++ compiler
        compilers = ["g++", "clang++", "c++"]
        cpp_compiler = None
        for compiler in compilers:
            try:
                result = subprocess.run([compiler, "--version"], capture_output=True)
                if result.returncode == 0:
                    cpp_compiler = compiler
                    print(f"✅ C++ compiler: {compiler}")
                    break
            except FileNotFoundError:
                continue
        
        if not cpp_compiler:
            print("❌ C++ compiler not found. Install g++ or clang++")
            return False
        
        return True
    
    def build_rust_hlc(self):
        """Build Rust HLC library"""
        print("\n🦀 Building Rust HLC library...")
        
        hlc_dir = self.rust_dir / "hlc"
        if not hlc_dir.exists():
            print(f"❌ Rust HLC directory not found: {hlc_dir}")
            return False
        
        # Build Rust library
        cmd = ["cargo", "build"]
        if self.is_release:
            cmd.append("--release")
        
        try:
            env = os.environ.copy()
            result = subprocess.run(
                cmd, 
                cwd=hlc_dir, 
                env=env,
                capture_output=not self.verbose
            )
            
            if result.returncode == 0:
                profile = "release" if self.is_release else "debug"
                lib_path = hlc_dir / "target" / profile
                
                # Find the built library
                lib_extensions = [".so", ".dylib", ".dll"]
                hlc_lib = None
                for ext in lib_extensions:
                    # Try both possible names
                    potential_libs = [
                        lib_path / f"libhlc{ext}",
                        lib_path / f"libpyhmssql_hlc{ext}"
                    ]
                    for potential_lib in potential_libs:
                        if potential_lib.exists():
                            hlc_lib = potential_lib
                            break
                    if hlc_lib:
                        break
                
                if hlc_lib:
                    print(f"✅ Rust HLC library built: {hlc_lib}")
                    
                    # Copy to cycore for linking
                    dest = self.cycore_dir / hlc_lib.name
                    shutil.copy2(hlc_lib, dest)
                    print(f"📦 Copied to: {dest}")
                    return True
                else:
                    print("❌ Built library not found")
                    return False
            else:
                print(f"❌ Rust build failed with code {result.returncode}")
                return False
                
        except Exception as e:
            print(f"❌ Rust build error: {e}")
            return False
    
    def build_cython_extensions(self):
        """Build Cython extensions"""
        print("\n🐍 Building Cython extensions...")
        
        if not self.cycore_dir.exists():
            print(f"❌ Cycore directory not found: {self.cycore_dir}")
            return False
        
        # Run the cycore build script
        build_script = self.cycore_dir / "build.py"
        if not build_script.exists():
            print(f"❌ Build script not found: {build_script}")
            return False
        
        try:
            cmd = [sys.executable, str(build_script)]
            if self.is_release:
                cmd.append("--release")
            
            result = subprocess.run(
                cmd,
                cwd=self.cycore_dir,
                capture_output=not self.verbose
            )
            
            if result.returncode == 0:
                print("✅ Cython extensions built successfully")
                return True
            else:
                print(f"❌ Cython build failed with code {result.returncode}")
                return False
                
        except Exception as e:
            print(f"❌ Cython build error: {e}")
            return False
    
    def run_tests(self):
        """Run integration tests"""
        print("\n🧪 Running integration tests...")
        
        test_commands = [
            # Test HLC integration
            [sys.executable, "-c", "from cycore.hlc_ts import HybridLogicalClock; hlc = HybridLogicalClock('test'); print(f'HLC test: {hlc.now()}')"],
            
            # Test Swiss Table integration
            [sys.executable, "-c", "from cycore.swiss_map import SwissMap; sm = SwissMap(); sm['test'] = 'value'; print(f'SwissMap test: {sm[\"test\"]}')"],
            
            # Test range Raft
            [sys.executable, "-c", "from server.range_raft import RangeRouter; rr = RangeRouter('test'); print('Range Raft test: OK')"]
        ]
        
        all_passed = True
        for i, cmd in enumerate(test_commands, 1):
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                if result.returncode == 0:
                    print(f"✅ Test {i}: {result.stdout.strip()}")
                else:
                    print(f"❌ Test {i} failed: {result.stderr.strip()}")
                    all_passed = False
            except subprocess.TimeoutExpired:
                print(f"❌ Test {i} timed out")
                all_passed = False
            except Exception as e:
                print(f"❌ Test {i} error: {e}")
                all_passed = False
        
        return all_passed
    
    def package_build(self):
        """Package the build artifacts"""
        print("\n📦 Packaging build artifacts...")
        
        # Create build directory
        self.build_dir.mkdir(exist_ok=True)
        
        # Copy built extensions
        artifacts = []
        
        # Cycore extensions
        for ext_file in self.cycore_dir.glob("*.so"):
            dest = self.build_dir / ext_file.name
            shutil.copy2(ext_file, dest)
            artifacts.append(dest)
        
        # Rust libraries
        for lib_file in self.cycore_dir.glob("lib*.so"):
            dest = self.build_dir / lib_file.name
            shutil.copy2(lib_file, dest)
            artifacts.append(dest)
        
        print(f"✅ Packaged {len(artifacts)} artifacts:")
        for artifact in artifacts:
            print(f"   📄 {artifact.name}")
        
        return True
    
    def update_requirements(self):
        """Update requirements.txt with build dependencies"""
        print("\n📋 Updating requirements...")
        
        requirements_file = self.workspace_root / "requirements.txt"
        
        # Production dependencies for cross-language extensions
        production_deps = [
            "# Production cross-language dependencies",
            "Cython>=3.0.0",
            "psutil>=5.8.0",
            "geoip2>=4.6.0",
            "requests>=2.28.0",
            ""
        ]
        
        if requirements_file.exists():
            with open(requirements_file, 'r') as f:
                content = f.read()
            
            # Add production deps if not already present
            if "# Production cross-language dependencies" not in content:
                with open(requirements_file, 'a') as f:
                    f.write("\n")
                    f.write("\n".join(production_deps))
                print("✅ Updated requirements.txt")
        
        return True
    
    def build_all(self):
        """Build all components"""
        print("\n🚀 Starting production build...")
        
        if not self.check_dependencies():
            print("\n❌ Dependency check failed")
            return False
        
        # Build Rust components
        if not self.build_rust_hlc():
            print("\n❌ Rust build failed")
            return False
        
        # Build Cython extensions
        if not self.build_cython_extensions():
            print("\n❌ Cython build failed")
            return False
        
        # Run tests
        if not self.run_tests():
            print("\n⚠️  Some tests failed, but build continues")
        
        # Package artifacts
        if not self.package_build():
            print("\n❌ Packaging failed")
            return False
        
        # Update requirements
        self.update_requirements()
        
        print("\n🎉 Production build completed successfully!")
        print("\n📖 Next steps:")
        print("   1. Install pyHMSSQL: pip install -e .")
        print("   2. Start server: python server/server.py --name production")
        print("   3. Run benchmarks: python tests/benchmark.py")
        
        return True


def main():
    """Main build entry point"""
    workspace_root = os.path.dirname(os.path.abspath(__file__))
    builder = ProductionBuilder(workspace_root)
    
    if "--help" in sys.argv or "-h" in sys.argv:
        print("""
PyHMSSQL Production Builder

Usage: python build_production.py [options]

Options:
  --release     Build optimized release version
  --verbose, -v Show detailed build output
  --help, -h    Show this help message

Examples:
  python build_production.py              # Debug build
  python build_production.py --release    # Optimized build
  python build_production.py --release -v # Verbose optimized build
        """)
        return
    
    success = builder.build_all()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
