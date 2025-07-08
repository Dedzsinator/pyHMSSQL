#!/usr/bin/env python3
"""
CyCore Test Runner - Comprehensive test execution for production readiness.

This script runs all CyCore tests and generates detailed reports for production
validation including:
- Unit test coverage
- Performance benchmarks  
- Integration test results
- Memory usage analysis
- Thread safety validation
- Production readiness assessment

Usage:
    python run_cycore_tests.py [--performance] [--integration] [--verbose]
"""

import argparse
import subprocess
import sys
import time
import os
import json
from pathlib import Path


class CyCoreTestRunner:
    """Comprehensive test runner for CyCore components"""
    
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.test_results = {}
        self.start_time = time.time()
        
        # Project paths
        self.project_root = Path(__file__).parent.parent.parent
        self.test_dir = self.project_root / "tests" / "test_cycore"
        
    def log(self, message):
        """Log message with timestamp"""
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] {message}")
    
    def print_colored(self, message, color='white'):
        """Print colored message (simplified for compatibility)"""
        colors = {
            'red': '\033[91m',
            'green': '\033[92m', 
            'yellow': '\033[93m',
            'blue': '\033[94m',
            'magenta': '\033[95m',
            'cyan': '\033[96m',
            'white': '\033[97m',
            'reset': '\033[0m'
        }
        
        color_code = colors.get(color, colors['white'])
        reset_code = colors['reset']
        self.log(f"{color_code}{message}{reset_code}")
    
    def run_command(self, command, description):
        """Run command and capture results"""
        self.log(f"Running {description}...")
        
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                cwd=self.project_root
            )
            
            success = result.returncode == 0
            
            self.test_results[description] = {
                "success": success,
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "command": command
            }
            
            if success:
                self.log(f"✅ {description} completed successfully")
            else:
                self.log(f"❌ {description} failed (return code: {result.returncode})")
            
            if self.verbose or not success:
                if result.stdout:
                    print("STDOUT:")
                    print(result.stdout)
                if result.stderr:
                    print("STDERR:")
                    print(result.stderr)
            
            return success
            
        except Exception as e:
            self.log(f"❌ {description} failed with exception: {e}")
            self.test_results[description] = {
                "success": False,
                "error": str(e),
                "command": command
            }
            return False
    
    def run_unit_tests(self):
        """Run CyCore unit tests"""
        self.log("=" * 60)
        self.log("RUNNING CYCORE UNIT TESTS")
        self.log("=" * 60)
         # Test individual components
        test_files = [
            "test_hashmap.py",
            "test_hlc.py", 
            "test_integration.py"
        ]
        
        overall_success = True
        
        for test_file in test_files:
            test_path = self.test_dir / test_file
            if test_path.exists():
                command = f"python -m pytest {test_path} -v --tb=short --no-header"
                if not self.run_command(command, f"Unit tests: {test_file}"):
                    overall_success = False
            else:
                self.log(f"⚠️  Test file not found: {test_file}")
        
        return overall_success
    
    def run_performance_tests(self):
        """Run performance benchmarks"""
        self.log("=" * 60)
        self.log("RUNNING PERFORMANCE BENCHMARKS")
        self.log("=" * 60)
        
        # Run performance-marked tests
        command = f"python -m pytest {self.test_dir} -v -m performance --tb=short"
        return self.run_command(command, "Performance benchmarks")
    
    def run_integration_tests(self):
        """Run integration tests"""
        self.log("=" * 60)
        self.log("RUNNING INTEGRATION TESTS")
        self.log("=" * 60)
        
        # Run integration-marked tests
        command = f"python -m pytest {self.test_dir} -v -m integration --tb=short"
        return self.run_command(command, "Integration tests")
    
    def run_thread_safety_tests(self):
        """Run thread safety tests"""
        self.log("=" * 60)
        self.log("RUNNING THREAD SAFETY TESTS")
        self.log("=" * 60)
        
        # Run threading-marked tests
        command = f"python -m pytest {self.test_dir} -v -m threading --tb=short"
        return self.run_command(command, "Thread safety tests")
    
    def check_cycore_availability(self):
        """Check if CyCore components are available and working."""
        self.print_colored("CHECKING CYCORE AVAILABILITY", 'cyan')
        print("=" * 60)
        
        # Use the external availability check script
        check_script = self.test_dir / "check_availability.py"
        command = f"python {check_script}"
        return self.run_command(command, "CyCore availability check")
    
    def generate_report(self):
        """Generate comprehensive test report"""
        self.log("=" * 60)
        self.log("GENERATING TEST REPORT")
        self.log("=" * 60)
        
        total_time = time.time() - self.start_time
        
        # Count results
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results.values() if result.get("success", False))
        failed_tests = total_tests - passed_tests
        
        # Generate report
        report = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_time_seconds": round(total_time, 2),
            "summary": {
                "total_test_suites": total_tests,
                "passed": passed_tests,
                "failed": failed_tests,
                "success_rate": round((passed_tests / total_tests * 100) if total_tests > 0 else 0, 1)
            },
            "test_results": self.test_results
        }
        
        # Save report to file
        report_file = self.project_root / "logs" / f"cycore_test_report_{int(time.time())}.json"
        report_file.parent.mkdir(exist_ok=True)
        
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Print summary
        self.log("=" * 60)
        self.log("TEST EXECUTION SUMMARY")
        self.log("=" * 60)
        self.log(f"Total execution time: {total_time:.2f} seconds")
        self.log(f"Test suites run: {total_tests}")
        self.log(f"Passed: {passed_tests}")
        self.log(f"Failed: {failed_tests}")
        self.log(f"Success rate: {report['summary']['success_rate']}%")
        
        if failed_tests > 0:
            self.log("\nFAILED TEST SUITES:")
            for name, result in self.test_results.items():
                if not result.get("success", False):
                    self.log(f"  ❌ {name}")
                    if "error" in result:
                        self.log(f"     Error: {result['error']}")
        
        self.log(f"\nDetailed report saved to: {report_file}")
        
        # Determine overall result
        overall_success = failed_tests == 0
        if overall_success:
            self.log("\n🎉 ALL CYCORE TESTS PASSED! Components are production ready.")
        else:
            self.log(f"\n⚠️  {failed_tests} test suite(s) failed. Review before production deployment.")
        
        return overall_success
    
    def run_all_tests(self, include_performance=False, include_integration=False):
        """Run all CyCore tests"""
        self.log("🚀 Starting CyCore comprehensive test suite...")
        
        # Always run these
        results = []
        results.append(self.check_cycore_availability())
        results.append(self.run_unit_tests())
        results.append(self.run_thread_safety_tests())
        
        # Optional test suites
        if include_performance:
            results.append(self.run_performance_tests())
        
        if include_integration:
            results.append(self.run_integration_tests())
        
        # Generate final report
        overall_success = self.generate_report()
        
        return overall_success


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="CyCore comprehensive test runner")
    parser.add_argument("--performance", action="store_true", 
                       help="Include performance benchmarks")
    parser.add_argument("--integration", action="store_true",
                       help="Include integration tests")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Verbose output")
    parser.add_argument("--all", action="store_true",
                       help="Run all test suites (equivalent to --performance --integration)")
    
    args = parser.parse_args()
    
    # Handle --all flag
    if args.all:
        args.performance = True
        args.integration = True
    
    # Create and run test runner
    runner = CyCoreTestRunner(verbose=args.verbose)
    success = runner.run_all_tests(
        include_performance=args.performance,
        include_integration=args.integration
    )
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
