"""
Enhanced profiling module that integrates Linux 'perf' tool for detailed performance analysis.
Extends the existing SystemProfiler with low-level performance counters and CPU profiling.
"""

import subprocess
import tempfile
import os
import signal
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
import threading
import time
import psutil
from collections import deque

class PerfProfiler:
    """
    Integrates Linux 'perf' tool for detailed CPU profiling and performance counters.
    """

    def __init__(self, output_dir: str = None):
        """
        Initialize the perf profiler.
        
        Args:
            output_dir: Directory to store perf output files
        """
        self.output_dir = Path(output_dir) if output_dir else Path("/tmp/pyhmssql_perf")
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Active perf processes
        self._active_processes: Dict[str, subprocess.Popen] = {}
        self._perf_data_files: Dict[str, Path] = {}
        
        # Performance counters to monitor
        self.default_counters = [
            'cpu-cycles',
            'instructions',
            'cache-references',
            'cache-misses',
            'branch-instructions',
            'branch-misses',
            'page-faults',
            'context-switches',
            'cpu-migrations',
            'L1-dcache-loads',
            'L1-dcache-load-misses',
            'L1-icache-load-misses',
            'LLC-loads',
            'LLC-load-misses',
            'dTLB-loads',
            'dTLB-load-misses',
            'iTLB-loads',
            'iTLB-load-misses'
        ]
        
        logging.info(f"PerfProfiler initialized with output directory: {self.output_dir}")

    def start_cpu_profiling(self, session_name: str, frequency: int = 99, 
                           call_graph: str = "dwarf") -> bool:
        """
        Start CPU profiling using perf record.
        
        Args:
            session_name: Name for this profiling session
            frequency: Sampling frequency (Hz)
            call_graph: Call graph method ('fp', 'dwarf', 'lbr')
            
        Returns:
            bool: True if started successfully
        """
        if session_name in self._active_processes:
            logging.warning(f"Profiling session '{session_name}' already active")
            return False
            
        output_file = self.output_dir / f"{session_name}_cpu.data"
        self._perf_data_files[f"{session_name}_cpu"] = output_file
        
        # Build perf record command
        cmd = [
            'perf', 'record',
            '-F', str(frequency),
            '-g', '--call-graph=' + call_graph,
            '--pid', str(os.getpid()),
            '-o', str(output_file)
        ]
        
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid
            )
            self._active_processes[f"{session_name}_cpu"] = process
            logging.info(f"Started CPU profiling for session '{session_name}' (PID: {process.pid})")
            return True
            
        except Exception as e:
            logging.error(f"Failed to start CPU profiling: {e}")
            return False

    def start_counter_profiling(self, session_name: str, 
                               counters: List[str] = None,
                               interval: float = 1.0) -> bool:
        """
        Start performance counter monitoring using perf stat.
        
        Args:
            session_name: Name for this profiling session
            counters: List of performance counters to monitor
            interval: Sampling interval in seconds
            
        Returns:
            bool: True if started successfully
        """
        if session_name in self._active_processes:
            logging.warning(f"Counter profiling session '{session_name}' already active")
            return False
            
        if counters is None:
            counters = self.default_counters
            
        output_file = self.output_dir / f"{session_name}_counters.json"
        
        # Build perf stat command
        cmd = [
            'perf', 'stat',
            '-e', ','.join(counters),
            '-I', str(int(interval * 1000)),  # Convert to milliseconds
            '-x', ',',  # CSV output
            '--pid', str(os.getpid()),
            '-o', str(output_file)
        ]
        
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid
            )
            self._active_processes[f"{session_name}_counters"] = process
            self._perf_data_files[f"{session_name}_counters"] = output_file
            logging.info(f"Started counter profiling for session '{session_name}' (PID: {process.pid})")
            return True
            
        except Exception as e:
            logging.error(f"Failed to start counter profiling: {e}")
            return False

    def start_memory_profiling(self, session_name: str) -> bool:
        """
        Start memory profiling using perf record with memory events.
        
        Args:
            session_name: Name for this profiling session
            
        Returns:
            bool: True if started successfully
        """
        if session_name in self._active_processes:
            logging.warning(f"Memory profiling session '{session_name}' already active")
            return False
            
        output_file = self.output_dir / f"{session_name}_memory.data"
        self._perf_data_files[f"{session_name}_memory"] = output_file
        
        # Build perf record command for memory profiling
        cmd = [
            'perf', 'record',
            '-e', 'cpu/mem-loads,ldlat=30/P',
            '-e', 'cpu/mem-stores/P',
            '--pid', str(os.getpid()),
            '-o', str(output_file)
        ]
        
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid
            )
            self._active_processes[f"{session_name}_memory"] = process
            logging.info(f"Started memory profiling for session '{session_name}' (PID: {process.pid})")
            return True
            
        except Exception as e:
            logging.warning(f"Memory profiling may not be available on this system: {e}")
            return False

    def stop_profiling(self, session_name: str, profile_type: str = "all") -> Dict[str, Any]:
        """
        Stop profiling for a session and generate reports.
        
        Args:
            session_name: Name of the profiling session
            profile_type: Type to stop ('cpu', 'counters', 'memory', 'all')
            
        Returns:
            Dict containing profiling results
        """
        results = {}
        
        if profile_type == "all":
            types_to_stop = ["cpu", "counters", "memory"]
        else:
            types_to_stop = [profile_type]
            
        for ptype in types_to_stop:
            key = f"{session_name}_{ptype}"
            if key in self._active_processes:
                process = self._active_processes[key]
                
                try:
                    # Send SIGINT to perf process group
                    os.killpg(os.getpgid(process.pid), signal.SIGINT)
                    
                    # Wait for process to finish
                    stdout, stderr = process.communicate(timeout=10)
                    
                    # Process the results
                    if ptype == "cpu":
                        results[ptype] = self._process_cpu_results(session_name)
                    elif ptype == "counters":
                        results[ptype] = self._process_counter_results(session_name)
                    elif ptype == "memory":
                        results[ptype] = self._process_memory_results(session_name)
                        
                    del self._active_processes[key]
                    logging.info(f"Stopped {ptype} profiling for session '{session_name}'")
                    
                except subprocess.TimeoutExpired:
                    process.kill()
                    logging.warning(f"Had to force kill {ptype} profiling process")
                except Exception as e:
                    logging.error(f"Error stopping {ptype} profiling: {e}")
                    
        return results

    def _process_cpu_results(self, session_name: str) -> Dict[str, Any]:
        """Process CPU profiling results from perf record."""
        data_file = self._perf_data_files.get(f"{session_name}_cpu")
        if not data_file or not data_file.exists():
            return {"error": "No CPU profiling data found"}
            
        try:
            # Generate perf report
            report_file = self.output_dir / f"{session_name}_cpu_report.txt"
            cmd = ['perf', 'report', '-i', str(data_file), '--stdio']
            
            with open(report_file, 'w') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, 
                                      text=True, timeout=30)
                
            # Generate flame graph data if available
            flamegraph_file = self.output_dir / f"{session_name}_cpu_flamegraph.txt"
            cmd = ['perf', 'script', '-i', str(data_file)]
            
            with open(flamegraph_file, 'w') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, 
                                      text=True, timeout=30)
                                      
            # Parse top functions
            top_functions = self._parse_perf_report(report_file)
            
            return {
                "data_file": str(data_file),
                "report_file": str(report_file),
                "flamegraph_file": str(flamegraph_file),
                "top_functions": top_functions,
                "file_size_mb": data_file.stat().st_size / (1024 * 1024)
            }
            
        except Exception as e:
            logging.error(f"Error processing CPU results: {e}")
            return {"error": str(e)}

    def _process_counter_results(self, session_name: str) -> Dict[str, Any]:
        """Process performance counter results."""
        data_file = self._perf_data_files.get(f"{session_name}_counters")
        if not data_file or not data_file.exists():
            return {"error": "No counter profiling data found"}
            
        try:
            counters = {}
            with open(data_file, 'r') as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        parts = line.strip().split(',')
                        if len(parts) >= 3:
                            timestamp = parts[0]
                            value = parts[1]
                            counter_name = parts[2]
                            
                            if counter_name not in counters:
                                counters[counter_name] = []
                            
                            try:
                                counters[counter_name].append({
                                    'timestamp': float(timestamp),
                                    'value': int(value) if value.isdigit() else value
                                })
                            except ValueError:
                                continue
                                
            # Calculate statistics for each counter
            stats = {}
            for counter, values in counters.items():
                numeric_values = [v['value'] for v in values if isinstance(v['value'], int)]
                if numeric_values:
                    stats[counter] = {
                        'count': len(numeric_values),
                        'total': sum(numeric_values),
                        'average': sum(numeric_values) / len(numeric_values),
                        'min': min(numeric_values),
                        'max': max(numeric_values)
                    }
                    
            return {
                "data_file": str(data_file),
                "counters": counters,
                "statistics": stats
            }
            
        except Exception as e:
            logging.error(f"Error processing counter results: {e}")
            return {"error": str(e)}

    def _process_memory_results(self, session_name: str) -> Dict[str, Any]:
        """Process memory profiling results."""
        data_file = self._perf_data_files.get(f"{session_name}_memory")
        if not data_file or not data_file.exists():
            return {"error": "No memory profiling data found"}
            
        try:
            # Generate memory report
            report_file = self.output_dir / f"{session_name}_memory_report.txt"
            cmd = ['perf', 'report', '-i', str(data_file), '--stdio', '--sort=symbol']
            
            with open(report_file, 'w') as f:
                result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, 
                                      text=True, timeout=30)
                                      
            return {
                "data_file": str(data_file),
                "report_file": str(report_file),
                "file_size_mb": data_file.stat().st_size / (1024 * 1024)
            }
            
        except Exception as e:
            logging.error(f"Error processing memory results: {e}")
            return {"error": str(e)}

    def _parse_perf_report(self, report_file: Path) -> List[Dict[str, Any]]:
        """Parse perf report to extract top functions."""
        top_functions = []
        
        try:
            with open(report_file, 'r') as f:
                lines = f.readlines()
                
            # Look for the function listing section
            in_functions = False
            for line in lines:
                line = line.strip()
                
                if "Overhead" in line and "Command" in line:
                    in_functions = True
                    continue
                    
                if in_functions and line and not line.startswith('#'):
                    # Parse perf report line format: percentage, command, shared object, symbol
                    parts = line.split()
                    if len(parts) >= 4 and parts[0].endswith('%'):
                        try:
                            overhead = float(parts[0].rstrip('%'))
                            top_functions.append({
                                'overhead_percent': overhead,
                                'command': parts[1],
                                'shared_object': parts[2],
                                'symbol': ' '.join(parts[3:])
                            })
                        except ValueError:
                            continue
                            
                    # Limit to top 20 functions
                    if len(top_functions) >= 20:
                        break
                        
        except Exception as e:
            logging.error(f"Error parsing perf report: {e}")
            
        return top_functions

    def get_system_performance_info(self) -> Dict[str, Any]:
        """Get system performance capabilities and information."""
        info = {
            'perf_version': self._get_perf_version(),
            'available_events': self._get_available_events(),
            'cpu_info': self._get_cpu_info(),
            'perf_capabilities': self._check_perf_capabilities()
        }
        return info

    def _get_perf_version(self) -> str:
        """Get perf tool version."""
        try:
            result = subprocess.run(['perf', '--version'], 
                                  capture_output=True, text=True, timeout=5)
            return result.stdout.strip()
        except Exception:
            return "unknown"

    def _get_available_events(self) -> List[str]:
        """Get list of available perf events."""
        try:
            result = subprocess.run(['perf', 'list'], 
                                  capture_output=True, text=True, timeout=10)
            # Parse the output to extract event names
            events = []
            for line in result.stdout.split('\n'):
                line = line.strip()
                if line and not line.startswith('List of') and '[' in line:
                    event_name = line.split('[')[0].strip()
                    if event_name:
                        events.append(event_name)
            return events[:50]  # Limit to first 50 events
        except Exception:
            return []

    def _get_cpu_info(self) -> Dict[str, Any]:
        """Get CPU information relevant for profiling."""
        try:
            with open('/proc/cpuinfo', 'r') as f:
                content = f.read()
                
            cpu_info = {}
            for line in content.split('\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    if key in ['model name', 'cpu MHz', 'cache size', 'cpu cores']:
                        cpu_info[key] = value
                        
            return cpu_info
        except Exception:
            return {}

    def _check_perf_capabilities(self) -> Dict[str, bool]:
        """Check what perf capabilities are available."""
        capabilities = {}
        
        # Check if we can run perf record
        try:
            result = subprocess.run(['perf', 'record', '--help'], 
                                  capture_output=True, timeout=5)
            capabilities['record'] = result.returncode == 0
        except Exception:
            capabilities['record'] = False
            
        # Check if we can run perf stat
        try:
            result = subprocess.run(['perf', 'stat', '--help'], 
                                  capture_output=True, timeout=5)
            capabilities['stat'] = result.returncode == 0
        except Exception:
            capabilities['stat'] = False
            
        # Check if we can access hardware events
        try:
            result = subprocess.run(['perf', 'stat', '-e', 'cycles', 'true'], 
                                  capture_output=True, timeout=5)
            capabilities['hardware_events'] = result.returncode == 0
        except Exception:
            capabilities['hardware_events'] = False
            
        return capabilities

    def cleanup(self):
        """Clean up any running perf processes."""
        for session_name, process in list(self._active_processes.items()):
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                process.wait(timeout=5)
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass
                    
        self._active_processes.clear()
        logging.info("PerfProfiler cleaned up")


class EnhancedSystemProfiler:
    """
    Enhanced version of SystemProfiler that integrates perf profiling capabilities.
    """
    
    def __init__(self, catalog_manager, sample_interval=1.0, enable_perf=True, perf_output_dir=None):
        """
        Initialize enhanced profiler with perf integration.
        
        Args:
            catalog_manager: Reference to catalog manager
            sample_interval: How often to sample system metrics
            enable_perf: Whether to enable perf profiling
            perf_output_dir: Directory for perf output files
        """
        # Initialize base profiler components
        self.catalog_manager = catalog_manager
        self.sample_interval = sample_interval
        self.is_profiling = False
        self.profiling_thread = None
        
        # Metrics storage
        self.max_samples = 3600
        self.cpu_samples = deque(maxlen=self.max_samples)
        self.memory_samples = deque(maxlen=self.max_samples)
        self.disk_samples = deque(maxlen=self.max_samples)
        self.network_samples = deque(maxlen=self.max_samples)
        self.query_metrics = deque(maxlen=1000)
        
        self.metrics_lock = threading.RLock()
        self.process = psutil.Process()
        
        # Initialize perf profiler
        self.perf_enabled = enable_perf
        if enable_perf:
            try:
                self.perf_profiler = PerfProfiler(perf_output_dir)
                logging.info("Enhanced profiler initialized with perf support")
            except Exception as e:
                logging.warning(f"Failed to initialize perf profiler: {e}")
                self.perf_enabled = False
                self.perf_profiler = None
        else:
            self.perf_profiler = None
            
        # Active perf sessions
        self._active_perf_sessions = set()

    def start_comprehensive_profiling(self, session_name: str, 
                                    include_cpu=True, 
                                    include_counters=True, 
                                    include_memory=False) -> bool:
        """
        Start comprehensive profiling including both system metrics and perf.
        
        Args:
            session_name: Name for the profiling session
            include_cpu: Whether to include CPU profiling
            include_counters: Whether to include performance counters
            include_memory: Whether to include memory profiling
            
        Returns:
            bool: True if started successfully
        """
        # Start regular system profiling
        self.start_profiling()
        
        # Start perf profiling if enabled
        if self.perf_enabled and self.perf_profiler:
            success = True
            
            if include_cpu:
                success &= self.perf_profiler.start_cpu_profiling(session_name)
                
            if include_counters:
                success &= self.perf_profiler.start_counter_profiling(session_name)
                
            if include_memory:
                success &= self.perf_profiler.start_memory_profiling(session_name)
                
            if success:
                self._active_perf_sessions.add(session_name)
                
            return success
        
        return True

    def stop_comprehensive_profiling(self, session_name: str) -> Dict[str, Any]:
        """
        Stop comprehensive profiling and generate reports.
        
        Args:
            session_name: Name of the profiling session
            
        Returns:
            Dict containing all profiling results
        """
        results = {
            'session_name': session_name,
            'timestamp': time.time(),
            'system_metrics': {},
            'perf_results': {}
        }
        
        # Get system metrics summary
        results['system_metrics'] = self.get_metrics_summary()
        
        # Stop perf profiling if active
        if (self.perf_enabled and self.perf_profiler and 
            session_name in self._active_perf_sessions):
            
            results['perf_results'] = self.perf_profiler.stop_profiling(session_name)
            self._active_perf_sessions.discard(session_name)
            
        return results

    def start_profiling(self):
        """Start system metrics profiling (from base class)."""
        if self.is_profiling:
            return

        self.is_profiling = True
        self.profiling_thread = threading.Thread(target=self._profiling_loop, daemon=True)
        self.profiling_thread.start()
        logging.info("System profiling started")

    def stop_profiling(self):
        """Stop system metrics profiling (from base class)."""
        self.is_profiling = False
        if self.profiling_thread:
            self.profiling_thread.join(timeout=2.0)
        logging.info("System profiling stopped")

    def _profiling_loop(self):
        """Main profiling loop (simplified from base class)."""
        last_disk_io = None
        last_network_io = None

        while self.is_profiling:
            try:
                timestamp = time.time()
                
                # CPU metrics
                cpu_percent = psutil.cpu_percent(interval=None)
                memory = psutil.virtual_memory()
                process_memory = self.process.memory_info()
                
                with self.metrics_lock:
                    self.cpu_samples.append({
                        'timestamp': timestamp,
                        'cpu_percent': cpu_percent
                    })
                    
                    self.memory_samples.append({
                        'timestamp': timestamp,
                        'memory_percent': memory.percent,
                        'process_memory_mb': process_memory.rss / (1024**2)
                    })

                time.sleep(self.sample_interval)

            except Exception as e:
                logging.error(f"Error in profiling loop: {str(e)}")
                time.sleep(self.sample_interval)

    def get_metrics_summary(self, duration_minutes=60):
        """Get summary of system metrics (simplified from base class)."""
        cutoff_time = time.time() - (duration_minutes * 60)

        with self.metrics_lock:
            recent_cpu = [s for s in self.cpu_samples if s['timestamp'] >= cutoff_time]
            recent_memory = [s for s in self.memory_samples if s['timestamp'] >= cutoff_time]

            if not recent_cpu or not recent_memory:
                return {"error": "Insufficient data for summary"}

            cpu_values = [s['cpu_percent'] for s in recent_cpu]
            memory_values = [s['memory_percent'] for s in recent_memory]

            return {
                'period_minutes': duration_minutes,
                'sample_count': len(recent_cpu),
                'cpu_stats': {
                    'avg': sum(cpu_values) / len(cpu_values),
                    'min': min(cpu_values),
                    'max': max(cpu_values)
                },
                'memory_stats': {
                    'avg': sum(memory_values) / len(memory_values),
                    'min': min(memory_values),
                    'max': max(memory_values)
                }
            }

    def profile_query(self, query_text, execution_time, result_rows=0, error=None):
        """Profile a specific query execution."""
        with self.metrics_lock:
            query_metric = {
                'timestamp': time.time(),
                'query_text': query_text[:500],
                'execution_time_ms': execution_time * 1000,
                'result_rows': result_rows,
                'error': error
            }
            self.query_metrics.append(query_metric)

    def get_perf_system_info(self) -> Dict[str, Any]:
        """Get perf system information."""
        if self.perf_enabled and self.perf_profiler:
            return self.perf_profiler.get_system_performance_info()
        return {"error": "Perf profiling not enabled"}

    def cleanup(self):
        """Clean up profiler resources."""
        self.stop_profiling()
        
        if self.perf_enabled and self.perf_profiler:
            self.perf_profiler.cleanup()
            
        logging.info("Enhanced profiler cleaned up")
