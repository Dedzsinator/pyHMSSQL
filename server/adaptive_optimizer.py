"""
Adaptive Query Optimizer Extensions.

This module provides runtime adaptivity features that allow the optimizer
to learn from execution feedback and adjust future planning decisions.

Features:
- Execution feedback collection and analysis
- Plan baseline management with fallback strategies  
- Runtime plan switching based on performance metrics
- Adaptive cost model parameter tuning
- Query performance monitoring and alerting
"""

import logging
import time
import threading
from typing import Dict, List, Tuple, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import statistics
import json
import pickle
import os


class FeedbackType(Enum):
    """Types of execution feedback that can be collected."""
    CARDINALITY_ESTIMATE = "cardinality_estimate"
    COST_ESTIMATE = "cost_estimate"
    EXECUTION_TIME = "execution_time"
    RESOURCE_USAGE = "resource_usage"
    ERROR_OCCURRED = "error_occurred"


class PlanStatus(Enum):
    """Status of a query plan baseline."""
    ACTIVE = "active"
    RETIRED = "retired"
    BASELINE = "baseline"
    EXPERIMENTAL = "experimental"


@dataclass
class ExecutionFeedback:
    """Feedback from query execution for adaptive learning."""
    query_id: str
    plan_id: str
    feedback_type: FeedbackType
    estimated_value: float
    actual_value: float
    execution_time: float
    resource_usage: Dict[str, float]
    timestamp: float = field(default_factory=time.time)
    error_message: Optional[str] = None
    
    @property
    def estimation_error(self) -> float:
        """Calculate the estimation error as a ratio."""
        if self.estimated_value == 0:
            return float('inf') if self.actual_value != 0 else 0.0
        return abs(self.actual_value - self.estimated_value) / self.estimated_value
    
    @property
    def is_significant_error(self) -> bool:
        """Check if this represents a significant estimation error."""
        return self.estimation_error > 0.5  # 50% error threshold


@dataclass
class PlanBaseline:
    """A baseline query plan with performance history."""
    plan_id: str
    plan: Dict[str, Any]
    query_signature: str
    status: PlanStatus
    creation_time: float
    last_used: float
    execution_count: int = 0
    avg_execution_time: float = 0.0
    min_execution_time: float = float('inf')
    max_execution_time: float = 0.0
    success_rate: float = 1.0
    recent_executions: deque = field(default_factory=lambda: deque(maxlen=50))
    performance_trend: str = "stable"  # "improving", "degrading", "stable"
    
    def update_performance(self, execution_time: float, success: bool = True):
        """Update performance metrics with new execution data."""
        self.execution_count += 1
        self.last_used = time.time()
        
        if success:
            # Update timing statistics
            self.min_execution_time = min(self.min_execution_time, execution_time)
            self.max_execution_time = max(self.max_execution_time, execution_time)
            
            # Update rolling average
            self.avg_execution_time = (
                (self.avg_execution_time * (self.execution_count - 1) + execution_time) /
                self.execution_count
            )
            
            # Track recent executions for trend analysis
            self.recent_executions.append(execution_time)
            
            # Update success rate
            self.success_rate = (
                (self.success_rate * (self.execution_count - 1) + 1.0) /
                self.execution_count
            )
            
            # Analyze performance trend
            self._update_performance_trend()
        else:
            # Failed execution
            self.success_rate = (
                (self.success_rate * (self.execution_count - 1) + 0.0) /
                self.execution_count
            )
    
    def _update_performance_trend(self):
        """Analyze recent performance to determine trend."""
        if len(self.recent_executions) < 10:
            self.performance_trend = "stable"
            return
        
        recent_times = list(self.recent_executions)
        mid_point = len(recent_times) // 2
        
        early_avg = statistics.mean(recent_times[:mid_point])
        late_avg = statistics.mean(recent_times[mid_point:])
        
        # Determine trend based on 10% threshold
        if late_avg < early_avg * 0.9:
            self.performance_trend = "improving"
        elif late_avg > early_avg * 1.1:
            self.performance_trend = "degrading"
        else:
            self.performance_trend = "stable"
    
    @property
    def is_reliable(self) -> bool:
        """Check if this baseline has sufficient history to be reliable."""
        return (self.execution_count >= 5 and 
                self.success_rate >= 0.8 and
                self.performance_trend != "degrading")


class AdaptiveCostModel:
    """
    Adaptive cost model that learns from execution feedback.
    
    This model adjusts cost parameters based on observed performance
    to improve future cost estimations.
    """
    
    def __init__(self):
        self.cpu_cost_factor = 1.0
        self.io_cost_factor = 1.0
        self.memory_cost_factor = 1.0
        self.network_cost_factor = 1.0
        
        # Learning parameters
        self.learning_rate = 0.1
        self.feedback_history = deque(maxlen=1000)
        self.last_adjustment = time.time()
        self.adjustment_interval = 300  # 5 minutes
        
        self._lock = threading.Lock()
    
    def add_feedback(self, feedback: ExecutionFeedback):
        """Add execution feedback to improve cost estimates."""
        with self._lock:
            self.feedback_history.append(feedback)
            
            # Adjust parameters periodically
            if time.time() - self.last_adjustment > self.adjustment_interval:
                self._adjust_parameters()
                self.last_adjustment = time.time()
    
    def _adjust_parameters(self):
        """Adjust cost parameters based on recent feedback."""
        if len(self.feedback_history) < 10:
            return
        
        # Analyze recent cost estimation errors
        recent_feedback = list(self.feedback_history)[-50:]  # Last 50 executions
        cost_errors = [f for f in recent_feedback 
                      if f.feedback_type == FeedbackType.COST_ESTIMATE]
        
        if not cost_errors:
            return
        
        # Calculate average estimation error
        avg_error = statistics.mean([f.estimation_error for f in cost_errors])
        
        # Adjust factors if there's systematic bias
        if avg_error > 0.2:  # 20% threshold
            # Determine which component to adjust based on query types
            # This is a simplified adjustment strategy
            adjustment = min(0.1, avg_error * self.learning_rate)
            
            # For now, adjust all factors equally
            # In practice, we'd analyze which cost components are most off
            self.cpu_cost_factor *= (1 + adjustment)
            self.io_cost_factor *= (1 + adjustment)
            self.memory_cost_factor *= (1 + adjustment)
            
            logging.info(f"Adjusted cost factors by {adjustment:.3f} due to "
                        f"average estimation error of {avg_error:.3f}")
    
    def get_adjusted_factors(self) -> Dict[str, float]:
        """Get current cost adjustment factors."""
        with self._lock:
            return {
                'cpu_cost_factor': self.cpu_cost_factor,
                'io_cost_factor': self.io_cost_factor,
                'memory_cost_factor': self.memory_cost_factor,
                'network_cost_factor': self.network_cost_factor
            }


class RuntimePlanSwitcher:
    """
    Runtime plan switching for adaptive query execution.
    
    This component can switch query plans during execution if
    performance deviates significantly from expectations.
    """
    
    def __init__(self):
        self.switch_threshold = 2.0  # Switch if execution is 2x slower than expected
        self.min_execution_time = 1.0  # Don't switch for very fast queries
        self.active_executions = {}  # query_id -> execution info
        self._lock = threading.Lock()
    
    def register_execution(self, query_id: str, plan: Dict[str, Any], 
                          estimated_time: float, alternative_plans: List[Dict] = None):
        """Register a query execution for potential switching."""
        with self._lock:
            self.active_executions[query_id] = {
                'plan': plan,
                'estimated_time': estimated_time,
                'start_time': time.time(),
                'alternative_plans': alternative_plans or [],
                'can_switch': len(alternative_plans or []) > 0
            }
    
    def check_switch_opportunity(self, query_id: str, current_progress: float) -> Optional[Dict]:
        """
        Check if a query should be switched to an alternative plan.
        
        Args:
            query_id: The executing query ID
            current_progress: Progress as a fraction (0.0 to 1.0)
            
        Returns:
            Alternative plan to switch to, or None
        """
        with self._lock:
            if query_id not in self.active_executions:
                return None
            
            execution = self.active_executions[query_id]
            
            if not execution['can_switch']:
                return None
            
            # Calculate current execution time
            current_time = time.time() - execution['start_time']
            
            # Don't switch very fast queries
            if execution['estimated_time'] < self.min_execution_time:
                return None
            
            # Calculate expected progress based on time
            expected_progress = current_time / execution['estimated_time']
            
            # Check if we're significantly behind schedule
            if expected_progress > self.switch_threshold and current_progress < 0.5:
                # Consider switching to an alternative plan
                alternatives = execution['alternative_plans']
                if alternatives:
                    # Choose the fastest alternative
                    # (In practice, this would be more sophisticated)
                    best_alternative = min(alternatives, 
                                         key=lambda p: p.get('estimated_cost', float('inf')))
                    
                    logging.info(f"Switching query {query_id} to alternative plan - "
                               f"Expected progress: {expected_progress:.2f}, "
                               f"Actual progress: {current_progress:.2f}")
                    
                    return best_alternative
            
            return None
    
    def complete_execution(self, query_id: str, success: bool = True):
        """Mark a query execution as complete."""
        with self._lock:
            if query_id in self.active_executions:
                del self.active_executions[query_id]


class AdaptiveOptimizer:
    """
    Main adaptive optimizer that coordinates all adaptive features.
    
    This class integrates with the main optimizer to provide:
    - Execution feedback collection
    - Plan baseline management
    - Runtime adaptivity
    - Performance monitoring
    """
    
    def __init__(self, storage_path: str = "adaptive_optimizer_data"):
        self.storage_path = storage_path
        self.feedback_collector = []
        self.plan_baselines = {}  # query_signature -> PlanBaseline
        self.adaptive_cost_model = AdaptiveCostModel()
        self.runtime_switcher = RuntimePlanSwitcher()
        
        # Performance monitoring
        self.query_performance = defaultdict(list)
        self.performance_alerts = []
        
        # Configuration
        self.baseline_retention_days = 30
        self.max_baselines_per_query = 5
        self.enable_runtime_switching = True
        self.enable_cost_adaptation = True
        
        self._lock = threading.Lock()
        
        # Load existing data
        self._load_state()
        
        # Start background maintenance
        self._start_maintenance_thread()
    
    def add_execution_feedback(self, feedback: ExecutionFeedback):
        """Add execution feedback for adaptive learning."""
        with self._lock:
            self.feedback_collector.append(feedback)
            
            # Update cost model
            if self.enable_cost_adaptation:
                self.adaptive_cost_model.add_feedback(feedback)
            
            # Update plan baseline if exists
            plan_baseline = self.plan_baselines.get(feedback.query_id)
            if plan_baseline:
                plan_baseline.update_performance(
                    feedback.execution_time,
                    feedback.error_message is None
                )
            
            # Track performance for monitoring
            self.query_performance[feedback.query_id].append({
                'timestamp': feedback.timestamp,
                'execution_time': feedback.execution_time,
                'success': feedback.error_message is None
            })
            
            # Check for performance degradation
            self._check_performance_alerts(feedback)
    
    def create_plan_baseline(self, query_signature: str, plan: Dict[str, Any]) -> PlanBaseline:
        """Create a new plan baseline for a query."""
        with self._lock:
            plan_id = f"{query_signature}_{int(time.time())}"
            
            baseline = PlanBaseline(
                plan_id=plan_id,
                plan=plan,
                query_signature=query_signature,
                status=PlanStatus.EXPERIMENTAL,
                creation_time=time.time(),
                last_used=time.time()
            )
            
            # Manage baseline count per query
            existing_baselines = [b for b in self.plan_baselines.values() 
                                if b.query_signature == query_signature]
            
            if len(existing_baselines) >= self.max_baselines_per_query:
                # Remove oldest unreliable baseline
                oldest = min(existing_baselines, 
                           key=lambda b: b.last_used if not b.is_reliable else float('inf'))
                if not oldest.is_reliable:
                    del self.plan_baselines[oldest.plan_id]
            
            self.plan_baselines[plan_id] = baseline
            return baseline
    
    def get_plan_baseline(self, query_signature: str) -> Optional[PlanBaseline]:
        """Get the best plan baseline for a query."""
        with self._lock:
            candidates = [b for b in self.plan_baselines.values() 
                         if b.query_signature == query_signature and b.is_reliable]
            
            if not candidates:
                return None
            
            # Return the baseline with best average performance
            return min(candidates, key=lambda b: b.avg_execution_time)
    
    def should_use_baseline(self, query_signature: str, new_plan: Dict[str, Any]) -> bool:
        """Determine if we should use a baseline plan instead of a new plan."""
        baseline = self.get_plan_baseline(query_signature)
        if not baseline:
            return False
        
        # Use baseline if it's significantly more reliable
        # This is a simple heuristic - could be more sophisticated
        return (baseline.execution_count >= 10 and 
                baseline.success_rate >= 0.95 and
                baseline.performance_trend != "degrading")
    
    def register_query_execution(self, query_id: str, query_signature: str,
                               plan: Dict[str, Any], estimated_time: float,
                               alternative_plans: List[Dict] = None):
        """Register a query execution for adaptive monitoring."""
        # Register for potential runtime switching
        if self.enable_runtime_switching:
            self.runtime_switcher.register_execution(
                query_id, plan, estimated_time, alternative_plans
            )
        
        # Create baseline if needed
        if query_signature not in [b.query_signature for b in self.plan_baselines.values()]:
            self.create_plan_baseline(query_signature, plan)
    
    def check_runtime_switch(self, query_id: str, progress: float) -> Optional[Dict]:
        """Check if a query should switch to an alternative plan."""
        if not self.enable_runtime_switching:
            return None
        
        return self.runtime_switcher.check_switch_opportunity(query_id, progress)
    
    def complete_query_execution(self, query_id: str, success: bool = True):
        """Mark a query execution as complete."""
        self.runtime_switcher.complete_execution(query_id, success)
    
    def _check_performance_alerts(self, feedback: ExecutionFeedback):
        """Check for performance issues that require attention."""
        # Check for significant cardinality estimation errors
        if (feedback.feedback_type == FeedbackType.CARDINALITY_ESTIMATE and 
            feedback.is_significant_error):
            alert = {
                'type': 'cardinality_estimation_error',
                'query_id': feedback.query_id,
                'error_ratio': feedback.estimation_error,
                'timestamp': feedback.timestamp
            }
            self.performance_alerts.append(alert)
        
        # Check for unexpectedly slow execution
        query_history = self.query_performance[feedback.query_id]
        if len(query_history) >= 5:
            recent_times = [h['execution_time'] for h in query_history[-5:]]
            avg_time = statistics.mean(recent_times)
            
            if feedback.execution_time > avg_time * 2:  # 2x slower than average
                alert = {
                    'type': 'performance_degradation',
                    'query_id': feedback.query_id,
                    'current_time': feedback.execution_time,
                    'average_time': avg_time,
                    'timestamp': feedback.timestamp
                }
                self.performance_alerts.append(alert)
    
    def get_cost_adjustments(self) -> Dict[str, float]:
        """Get current cost model adjustments."""
        return self.adaptive_cost_model.get_adjusted_factors()
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get a summary of adaptive optimizer performance."""
        with self._lock:
            return {
                'total_feedback_entries': len(self.feedback_collector),
                'active_baselines': len([b for b in self.plan_baselines.values() 
                                       if b.status == PlanStatus.ACTIVE]),
                'reliable_baselines': len([b for b in self.plan_baselines.values() 
                                         if b.is_reliable]),
                'recent_alerts': len([a for a in self.performance_alerts 
                                    if time.time() - a['timestamp'] < 3600]),  # Last hour
                'cost_adjustments': self.get_cost_adjustments(),
                'runtime_switching_enabled': self.enable_runtime_switching,
                'cost_adaptation_enabled': self.enable_cost_adaptation
            }
    
    def get_recent_alerts(self, hours: int = 24) -> List[Dict]:
        """Get performance alerts from the last N hours."""
        cutoff = time.time() - (hours * 3600)
        return [alert for alert in self.performance_alerts 
                if alert['timestamp'] >= cutoff]
    
    def _start_maintenance_thread(self):
        """Start background thread for maintenance tasks."""
        def maintenance_worker():
            while True:
                try:
                    time.sleep(300)  # Run every 5 minutes
                    self._cleanup_old_data()
                    self._save_state()
                except Exception as e:
                    logging.error(f"Adaptive optimizer maintenance error: {e}")
        
        thread = threading.Thread(target=maintenance_worker, daemon=True)
        thread.start()
    
    def _cleanup_old_data(self):
        """Clean up old data to prevent memory leaks."""
        with self._lock:
            current_time = time.time()
            cutoff_time = current_time - (self.baseline_retention_days * 24 * 3600)
            
            # Remove old baselines
            to_remove = [plan_id for plan_id, baseline in self.plan_baselines.items()
                        if baseline.last_used < cutoff_time and not baseline.is_reliable]
            
            for plan_id in to_remove:
                del self.plan_baselines[plan_id]
            
            # Trim old feedback
            if len(self.feedback_collector) > 5000:
                self.feedback_collector = self.feedback_collector[-2000:]
            
            # Trim old alerts
            alert_cutoff = current_time - (7 * 24 * 3600)  # Keep 7 days
            self.performance_alerts = [a for a in self.performance_alerts
                                     if a['timestamp'] >= alert_cutoff]
    
    def _save_state(self):
        """Save adaptive optimizer state to disk."""
        try:
            os.makedirs(self.storage_path, exist_ok=True)
            
            # Save plan baselines
            baselines_file = os.path.join(self.storage_path, "plan_baselines.pkl")
            with open(baselines_file, 'wb') as f:
                pickle.dump(self.plan_baselines, f)
            
            # Save feedback (recent only)
            feedback_file = os.path.join(self.storage_path, "recent_feedback.pkl")
            recent_feedback = self.feedback_collector[-1000:] if self.feedback_collector else []
            with open(feedback_file, 'wb') as f:
                pickle.dump(recent_feedback, f)
                
        except Exception as e:
            logging.error(f"Failed to save adaptive optimizer state: {e}")
    
    def _load_state(self):
        """Load adaptive optimizer state from disk."""
        try:
            # Load plan baselines
            baselines_file = os.path.join(self.storage_path, "plan_baselines.pkl")
            if os.path.exists(baselines_file):
                with open(baselines_file, 'rb') as f:
                    self.plan_baselines = pickle.load(f)
            
            # Load recent feedback
            feedback_file = os.path.join(self.storage_path, "recent_feedback.pkl")
            if os.path.exists(feedback_file):
                with open(feedback_file, 'rb') as f:
                    self.feedback_collector = pickle.load(f)
                    
            logging.info(f"Loaded adaptive optimizer state: "
                        f"{len(self.plan_baselines)} baselines, "
                        f"{len(self.feedback_collector)} feedback entries")
                        
        except Exception as e:
            logging.warning(f"Failed to load adaptive optimizer state: {e}")
    
    def shutdown(self):
        """Shutdown the adaptive optimizer and save state."""
        logging.info("Shutting down adaptive optimizer...")
        self._save_state()
        logging.info("Adaptive optimizer shutdown complete")
