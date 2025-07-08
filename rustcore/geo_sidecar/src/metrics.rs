//! Performance metrics collection

use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub avg_latency_micros: f64,
    pub min_latency_micros: u64,
    pub max_latency_micros: u64,
}

pub struct MetricsCollector {
    total_requests: AtomicU64,
    successful_requests: AtomicU64,
    failed_requests: AtomicU64,
    total_latency_micros: AtomicU64,
    min_latency_micros: AtomicU64,
    max_latency_micros: AtomicU64,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            total_latency_micros: AtomicU64::new(0),
            min_latency_micros: AtomicU64::new(u64::MAX),
            max_latency_micros: AtomicU64::new(0),
        }
    }

    pub fn record_request(&self, latency_micros: u64, success: bool) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.total_latency_micros.fetch_add(latency_micros, Ordering::Relaxed);

        if success {
            self.successful_requests.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed_requests.fetch_add(1, Ordering::Relaxed);
        }

        // Update min/max latency
        let mut current_min = self.min_latency_micros.load(Ordering::Relaxed);
        while latency_micros < current_min {
            match self.min_latency_micros.compare_exchange_weak(
                current_min,
                latency_micros,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_min = actual,
            }
        }

        let mut current_max = self.max_latency_micros.load(Ordering::Relaxed);
        while latency_micros > current_max {
            match self.max_latency_micros.compare_exchange_weak(
                current_max,
                latency_micros,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    pub fn get_snapshot(&self) -> MetricsSnapshot {
        let total_requests = self.total_requests.load(Ordering::Relaxed);
        let successful_requests = self.successful_requests.load(Ordering::Relaxed);
        let failed_requests = self.failed_requests.load(Ordering::Relaxed);
        let total_latency_micros = self.total_latency_micros.load(Ordering::Relaxed);
        let min_latency_micros = self.min_latency_micros.load(Ordering::Relaxed);
        let max_latency_micros = self.max_latency_micros.load(Ordering::Relaxed);

        let avg_latency_micros = if total_requests > 0 {
            total_latency_micros as f64 / total_requests as f64
        } else {
            0.0
        };

        let min_latency_micros = if min_latency_micros == u64::MAX {
            0
        } else {
            min_latency_micros
        };

        MetricsSnapshot {
            total_requests,
            successful_requests,
            failed_requests,
            avg_latency_micros,
            min_latency_micros,
            max_latency_micros,
        }
    }

    pub fn reset(&self) {
        self.total_requests.store(0, Ordering::Relaxed);
        self.successful_requests.store(0, Ordering::Relaxed);
        self.failed_requests.store(0, Ordering::Relaxed);
        self.total_latency_micros.store(0, Ordering::Relaxed);
        self.min_latency_micros.store(u64::MAX, Ordering::Relaxed);
        self.max_latency_micros.store(0, Ordering::Relaxed);
    }
}
