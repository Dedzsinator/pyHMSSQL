//! High-performance routing engine

use crate::geo::{GeoLocation, GeoResolver};
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub is_leader: bool,
    pub healthy: bool,
    pub zone: String,
    pub geo_location: GeoLocation,
    pub load_score: f64,
    pub latency_ms: f64,
}

#[derive(Debug)]
pub struct RoutingRequest {
    pub client_ip: IpAddr,
    pub query_type: String,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RoutingResponse {
    pub node_id: String,
    pub host: String,
    pub port: u16,
    pub distance_km: f64,
    pub routing_strategy: String,
    pub response_time_micros: u64,
}

pub struct RoutingEngine {
    replicas: DashMap<String, ReplicaInfo>,
    zone_replicas: DashMap<String, Vec<String>>,
}

impl RoutingEngine {
    pub fn new() -> Self {
        Self {
            replicas: DashMap::new(),
            zone_replicas: DashMap::new(),
        }
    }

    pub fn update_replicas(&mut self, replicas: Vec<ReplicaInfo>) -> Result<()> {
        // Clear existing data
        self.replicas.clear();
        self.zone_replicas.clear();

        // Update with new replicas
        for replica in replicas {
            let zone = replica.zone.clone();
            let node_id = replica.node_id.clone();

            self.replicas.insert(node_id.clone(), replica);

            self.zone_replicas
                .entry(zone)
                .or_insert_with(Vec::new)
                .push(node_id);
        }

        tracing::info!(
            "Updated routing table with {} replicas",
            self.replicas.len()
        );
        Ok(())
    }

    pub fn route_request(
        &self,
        request: &RoutingRequest,
        geo_resolver: &GeoResolver,
    ) -> Result<RoutingResponse> {
        let start_time = std::time::Instant::now();

        // Resolve client location
        let client_location = geo_resolver.resolve(request.client_ip)?;

        // Get available replicas
        let healthy_replicas: Vec<_> = self
            .replicas
            .iter()
            .filter(|entry| entry.value().healthy)
            .map(|entry| entry.value().clone())
            .collect();

        if healthy_replicas.is_empty() {
            return Err(anyhow!("No healthy replicas available"));
        }

        // Select best replica based on query type
        let selected_replica = if request.query_type == "write" {
            self.select_best_leader(&healthy_replicas, &client_location, geo_resolver)?
        } else {
            self.select_best_replica(&healthy_replicas, &client_location, geo_resolver)?
        };

        let distance_km =
            geo_resolver.calculate_distance(&client_location, &selected_replica.geo_location);

        let response_time_micros = start_time.elapsed().as_micros() as u64;

        Ok(RoutingResponse {
            node_id: selected_replica.node_id,
            host: selected_replica.host,
            port: selected_replica.port,
            distance_km,
            routing_strategy: "closest_healthy".to_string(),
            response_time_micros,
        })
    }

    fn select_best_leader(
        &self,
        candidates: &[ReplicaInfo],
        client_location: &GeoLocation,
        geo_resolver: &GeoResolver,
    ) -> Result<ReplicaInfo> {
        let leaders: Vec<_> = candidates.iter().filter(|r| r.is_leader).collect();

        if leaders.is_empty() {
            return Err(anyhow!("No healthy leaders available"));
        }

        // Find closest leader
        let best_leader = leaders
            .into_iter()
            .min_by(|a, b| {
                let dist_a = geo_resolver.calculate_distance(client_location, &a.geo_location);
                let dist_b = geo_resolver.calculate_distance(client_location, &b.geo_location);

                // Factor in load as well
                let score_a = dist_a + a.load_score * 100.0; // 100km penalty per load unit
                let score_b = dist_b + b.load_score * 100.0;

                score_a
                    .partial_cmp(&score_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .ok_or_else(|| anyhow!("Failed to select leader"))?;

        Ok(best_leader.clone())
    }

    fn select_best_replica(
        &self,
        candidates: &[ReplicaInfo],
        client_location: &GeoLocation,
        geo_resolver: &GeoResolver,
    ) -> Result<ReplicaInfo> {
        // For reads, we can use any healthy replica (including followers)
        let best_replica = candidates
            .iter()
            .min_by(|a, b| {
                let dist_a = geo_resolver.calculate_distance(client_location, &a.geo_location);
                let dist_b = geo_resolver.calculate_distance(client_location, &b.geo_location);

                // Prefer leaders for consistency, but factor in distance and load
                let leader_bonus_a = if a.is_leader { -50.0 } else { 0.0 }; // 50km bonus for leaders
                let leader_bonus_b = if b.is_leader { -50.0 } else { 0.0 };

                let score_a = dist_a + a.load_score * 100.0 + a.latency_ms + leader_bonus_a;
                let score_b = dist_b + b.load_score * 100.0 + b.latency_ms + leader_bonus_b;

                score_a
                    .partial_cmp(&score_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .ok_or_else(|| anyhow!("Failed to select replica"))?;

        Ok(best_replica.clone())
    }

    pub fn get_replica_count(&self) -> usize {
        self.replicas.len()
    }

    pub fn get_healthy_replica_count(&self) -> usize {
        self.replicas
            .iter()
            .filter(|entry| entry.value().healthy)
            .count()
    }

    pub fn get_leader_count(&self) -> usize {
        self.replicas
            .iter()
            .filter(|entry| entry.value().is_leader && entry.value().healthy)
            .count()
    }
}
