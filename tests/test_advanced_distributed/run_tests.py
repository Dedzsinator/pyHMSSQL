#!/usr/bin/env python3
"""
Comprehensive test runner for advanced distributed database tests.

This script provides:
- Test discovery and execution
- Performance benchmarking
- Test categorization and filtering
- Report generation
- CI/CD integration

Usage:
    python run_tests.py [options]
    
Examples:
    python run_tests.py --unit                    # Run only unit tests
    python run_tests.py --integration             # Run integration tests
    python run_tests.py --benchmark               # Run performance benchmarks
    python run_tests.py --all                     # Run all tests
    python run_tests.py --fast                    # Run only fast tests
    python run_tests.py --coverage                # Generate coverage report
    python run_tests.py --component raft          # Test specific component
"""

import argparse
import os
import sys
import subprocess
import time
import json
from pathlib import Path
from typing import List, Dict, Any, Optional


class TestRunner:
    """Advanced test runner with categorization and reporting"""
    
    def __init__(self, test_root: str = None):
        self.test_root = Path(test_root or Path(__file__).parent)
        self.report_dir = self.test_root / "test_reports"
        self.report_dir.mkdir(exist_ok=True)
        
        # Test categories and their patterns
        self.test_categories = {
            'unit': [
                'test_consistency.py',
                'test_wal.py',
                'test_compression.py',
                'test_zerocopy.py',
                'test_sharding.py',
                'test_raft.py',
                'test_crdt.py',
                'test_networking.py'
            ],
            'integration': [
                'test_integration.py'
            ],
            'endtoend': [
                'test_end_to_end.py'
            ],
            'benchmark': []  # Special handling for benchmark markers
        }
        
        # Component mapping
        self.components = {
            'raft': ['test_raft.py', 'test_integration.py::TestRaftWALIntegration'],
            'wal': ['test_wal.py', 'test_integration.py::TestRaftWALIntegration'],
            'compression': ['test_compression.py', 'test_integration.py::TestShardingCompressionIntegration'],
            'zerocopy': ['test_zerocopy.py', 'test_integration.py::TestShardingCompressionIntegration'],
            'sharding': ['test_sharding.py', 'test_integration.py::TestShardingCompressionIntegration'],
            'consistency': ['test_consistency.py'],
            'crdt': ['test_crdt.py', 'test_integration.py::TestCRDTNetworkingIntegration'],
            'networking': ['test_networking.py', 'test_integration.py::TestCRDTNetworkingIntegration'],
            'pubsub': ['test_networking.py', 'test_integration.py::TestPubSubRaftIntegration']
        }
    
    def run_tests(self, 
                  category: Optional[str] = None,
                  component: Optional[str] = None,
                  benchmark: bool = False,
                  coverage: bool = False,
                  fast_only: bool = False,
                  parallel: bool = False,
                  verbose: bool = False,
                  dry_run: bool = False,
                  pattern: Optional[str] = None) -> Dict[str, Any]:
        """Run tests with specified options"""
        
        # Build pytest command
        cmd = ['python', '-m', 'pytest']
        
        # Add test paths
        test_files = self._get_test_files(category, component, pattern)
        if test_files:
            cmd.extend(test_files)
        else:
            cmd.append(str(self.test_root))
        
        # Add markers and filters
        if benchmark:
            cmd.extend(['-m', 'benchmark'])
        elif fast_only:
            cmd.extend(['-m', 'not slow'])
        
        # Add output options
        if verbose:
            cmd.extend(['-v', '-s'])
        
        if coverage:
            cmd.extend([
                '--cov=kvstore',
                '--cov-report=term-missing',
                '--cov-report=html:' + str(self.report_dir / 'coverage_html'),
                '--cov-report=xml:' + str(self.report_dir / 'coverage.xml')
            ])
        
        # Add parallel execution
        if parallel:
            cmd.extend(['-n', 'auto'])  # Requires pytest-xdist
        
        # Add reporting
        cmd.extend([
            '--junit-xml=' + str(self.report_dir / 'junit.xml'),
            '--tb=short'
        ])
        
        # Set environment
        env = os.environ.copy()
        env['PYTHONPATH'] = str(self.test_root.parent)
        
        # Execute tests
        print(f"Running command: {' '.join(cmd)}")
        
        if dry_run:
            return {'status': 'dry_run', 'command': cmd}
        
        start_time = time.time()
        
        try:
            result = subprocess.run(
                cmd,
                cwd=self.test_root,
                env=env,
                capture_output=True,
                text=True,
                timeout=1800  # 30 minute timeout
            )
            
            duration = time.time() - start_time
            
            # Parse results
            test_result = {
                'status': 'passed' if result.returncode == 0 else 'failed',
                'returncode': result.returncode,
                'duration': duration,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'command': cmd
            }
            
            # Generate reports
            self._generate_summary_report(test_result)
            
            return test_result
            
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            return {
                'status': 'timeout',
                'duration': duration,
                'command': cmd
            }
        
        except Exception as e:
            duration = time.time() - start_time
            return {
                'status': 'error',
                'error': str(e),
                'duration': duration,
                'command': cmd
            }
    
    def _get_test_files(self, category: Optional[str], 
                       component: Optional[str],
                       pattern: Optional[str]) -> List[str]:
        """Get test files based on criteria"""
        files = []
        
        if component:
            if component in self.components:
                files.extend(self.components[component])
            else:
                print(f"Warning: Unknown component '{component}'")
                print(f"Available components: {list(self.components.keys())}")
        
        elif category:
            if category in self.test_categories:
                files.extend(self.test_categories[category])
            else:
                print(f"Warning: Unknown category '{category}'")
                print(f"Available categories: {list(self.test_categories.keys())}")
        
        elif pattern:
            # Find files matching pattern
            for file in self.test_root.glob(f"**/*{pattern}*.py"):
                if file.name.startswith('test_'):
                    files.append(str(file.relative_to(self.test_root)))
        
        return files
    
    def _generate_summary_report(self, test_result: Dict[str, Any]):
        """Generate test summary report"""
        report_file = self.report_dir / 'test_summary.json'
        
        # Parse pytest output for test counts
        stdout = test_result.get('stdout', '')
        
        # Simple parsing - in production would use proper pytest JSON output
        passed_count = stdout.count(' PASSED')
        failed_count = stdout.count(' FAILED')
        skipped_count = stdout.count(' SKIPPED')
        error_count = stdout.count(' ERROR')
        
        summary = {
            'timestamp': time.time(),
            'status': test_result['status'],
            'duration': test_result['duration'],
            'counts': {
                'passed': passed_count,
                'failed': failed_count,
                'skipped': skipped_count,
                'errors': error_count,
                'total': passed_count + failed_count + skipped_count + error_count
            },
            'success_rate': passed_count / max(1, passed_count + failed_count) if passed_count or failed_count else 0
        }
        
        with open(report_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"\nTest Summary:")
        print(f"  Status: {summary['status']}")
        print(f"  Duration: {summary['duration']:.1f}s")
        print(f"  Passed: {summary['counts']['passed']}")
        print(f"  Failed: {summary['counts']['failed']}")
        print(f"  Skipped: {summary['counts']['skipped']}")
        print(f"  Success Rate: {summary['success_rate']:.1%}")
        print(f"  Report saved to: {report_file}")
    
    def run_benchmark_suite(self) -> Dict[str, Any]:
        """Run complete benchmark suite"""
        print("Running comprehensive benchmark suite...")
        
        benchmark_results = {}
        
        # Component benchmarks
        components_to_benchmark = ['raft', 'compression', 'zerocopy', 'sharding', 'crdt']
        
        for component in components_to_benchmark:
            print(f"\n--- Benchmarking {component} ---")
            result = self.run_tests(
                component=component,
                benchmark=True,
                verbose=True
            )
            benchmark_results[component] = result
        
        # Integration benchmarks
        print(f"\n--- Running Integration Benchmarks ---")
        result = self.run_tests(
            category='integration',
            benchmark=True,
            verbose=True
        )
        benchmark_results['integration'] = result
        
        # System benchmarks
        print(f"\n--- Running System Benchmarks ---")
        result = self.run_tests(
            category='endtoend',
            benchmark=True,
            verbose=True
        )
        benchmark_results['system'] = result
        
        # Save benchmark report
        benchmark_report = self.report_dir / 'benchmark_report.json'
        with open(benchmark_report, 'w') as f:
            json.dump(benchmark_results, f, indent=2)
        
        print(f"\nBenchmark report saved to: {benchmark_report}")
        return benchmark_results
    
    def run_ci_suite(self) -> bool:
        """Run CI/CD test suite"""
        print("Running CI/CD test suite...")
        
        # Fast unit tests first
        print("\n--- Phase 1: Unit Tests ---")
        unit_result = self.run_tests(category='unit', fast_only=True)
        
        if unit_result['status'] != 'passed':
            print("Unit tests failed, stopping CI run")
            return False
        
        # Integration tests
        print("\n--- Phase 2: Integration Tests ---")
        integration_result = self.run_tests(category='integration')
        
        if integration_result['status'] != 'passed':
            print("Integration tests failed, stopping CI run")
            return False
        
        # Basic system tests
        print("\n--- Phase 3: System Tests ---")
        system_result = self.run_tests(category='endtoend', fast_only=True)
        
        success = system_result['status'] == 'passed'
        
        print(f"\nCI Suite Result: {'PASSED' if success else 'FAILED'}")
        return success
    
    def list_tests(self):
        """List available tests and categories"""
        print("Available test categories:")
        for category, files in self.test_categories.items():
            print(f"  {category}:")
            for file in files:
                print(f"    - {file}")
        
        print("\nAvailable components:")
        for component, files in self.components.items():
            print(f"  {component}:")
            for file in files:
                print(f"    - {file}")


def main():
    """Main test runner entry point"""
    parser = argparse.ArgumentParser(description='Advanced distributed database test runner')
    
    # Test selection
    test_group = parser.add_mutually_exclusive_group()
    test_group.add_argument('--unit', action='store_true', help='Run unit tests')
    test_group.add_argument('--integration', action='store_true', help='Run integration tests')
    test_group.add_argument('--endtoend', action='store_true', help='Run end-to-end tests')
    test_group.add_argument('--benchmark', action='store_true', help='Run benchmark tests')
    test_group.add_argument('--all', action='store_true', help='Run all tests')
    
    # Component selection
    parser.add_argument('--component', choices=['raft', 'wal', 'compression', 'zerocopy', 
                                              'sharding', 'consistency', 'crdt', 'networking', 'pubsub'],
                       help='Test specific component')
    
    # Test filtering
    parser.add_argument('--fast', action='store_true', help='Run only fast tests')
    parser.add_argument('--pattern', help='Run tests matching pattern')
    
    # Execution options
    parser.add_argument('--parallel', action='store_true', help='Run tests in parallel')
    parser.add_argument('--coverage', action='store_true', help='Generate coverage report')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--dry-run', action='store_true', help='Show commands without executing')
    
    # Special modes
    parser.add_argument('--ci', action='store_true', help='Run CI/CD test suite')
    parser.add_argument('--benchmark-suite', action='store_true', help='Run complete benchmark suite')
    parser.add_argument('--list', action='store_true', help='List available tests')
    
    args = parser.parse_args()
    
    # Create test runner
    runner = TestRunner()
    
    # Handle special modes
    if args.list:
        runner.list_tests()
        return
    
    if args.ci:
        success = runner.run_ci_suite()
        sys.exit(0 if success else 1)
    
    if args.benchmark_suite:
        runner.run_benchmark_suite()
        return
    
    # Determine test category
    category = None
    if args.unit:
        category = 'unit'
    elif args.integration:
        category = 'integration'
    elif args.endtoend:
        category = 'endtoend'
    elif args.all:
        category = None  # Run all
    
    # Run tests
    result = runner.run_tests(
        category=category,
        component=args.component,
        benchmark=args.benchmark,
        coverage=args.coverage,
        fast_only=args.fast,
        parallel=args.parallel,
        verbose=args.verbose,
        dry_run=args.dry_run,
        pattern=args.pattern
    )
    
    # Exit with appropriate code
    if result['status'] == 'passed':
        print("\n✅ All tests passed!")
        sys.exit(0)
    elif result['status'] == 'dry_run':
        print("\n🏃‍♂️ Dry run completed")
        sys.exit(0)
    else:
        print(f"\n❌ Tests failed: {result['status']}")
        sys.exit(1)


if __name__ == '__main__':
    main()
