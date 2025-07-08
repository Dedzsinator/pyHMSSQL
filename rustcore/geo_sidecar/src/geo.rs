//! Geo-location resolution module

use anyhow::{Context, Result};
use maxminddb::{geoip2, Reader};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoLocation {
    pub country: String,
    pub region: String,
    pub city: String,
    pub latitude: f64,
    pub longitude: f64,
    pub timezone: String,
}

impl Default for GeoLocation {
    fn default() -> Self {
        Self {
            country: "Unknown".to_string(),
            region: "Unknown".to_string(),
            city: "Unknown".to_string(),
            latitude: 0.0,
            longitude: 0.0,
            timezone: "UTC".to_string(),
        }
    }
}

pub struct GeoResolver {
    reader: Option<Reader<Vec<u8>>>,
}

impl GeoResolver {
    pub fn new(geoip_db_path: Option<PathBuf>) -> Result<Self> {
        let reader = if let Some(path) = geoip_db_path {
            if path.exists() {
                Some(Reader::open_readfile(&path).context("Failed to open GeoIP database")?)
            } else {
                tracing::warn!("GeoIP database not found at {:?}", path);
                None
            }
        } else {
            None
        };

        Ok(Self { reader })
    }

    pub fn resolve(&self, ip: IpAddr) -> Result<GeoLocation> {
        if let Some(ref reader) = self.reader {
            match reader.lookup::<geoip2::City>(ip) {
                Ok(city) => {
                    let country = city
                        .country
                        .as_ref()
                        .and_then(|c| c.names.as_ref())
                        .and_then(|n| n.get("en"))
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "Unknown".to_string());

                    let region = city
                        .subdivisions
                        .as_ref()
                        .and_then(|subdivisions| subdivisions.iter().next())
                        .and_then(|subdivision| subdivision.names.as_ref())
                        .and_then(|names| names.get("en"))
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "Unknown".to_string());

                    let city_name = city
                        .city
                        .as_ref()
                        .and_then(|c| c.names.as_ref())
                        .and_then(|n| n.get("en"))
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "Unknown".to_string());

                    let (latitude, longitude) = city
                        .location
                        .as_ref()
                        .map(|loc| (loc.latitude.unwrap_or(0.0), loc.longitude.unwrap_or(0.0)))
                        .unwrap_or((0.0, 0.0));

                    let timezone = city
                        .location
                        .as_ref()
                        .and_then(|loc| loc.time_zone)
                        .map(|tz| tz.to_string())
                        .unwrap_or_else(|| "UTC".to_string());

                    Ok(GeoLocation {
                        country,
                        region,
                        city: city_name,
                        latitude,
                        longitude,
                        timezone,
                    })
                }
                Err(e) => {
                    tracing::debug!("GeoIP lookup failed for {}: {}", ip, e);
                    Ok(GeoLocation::default())
                }
            }
        } else {
            // No GeoIP database, return default location
            Ok(GeoLocation::default())
        }
    }

    pub fn calculate_distance(&self, loc1: &GeoLocation, loc2: &GeoLocation) -> f64 {
        haversine_distance(loc1.latitude, loc1.longitude, loc2.latitude, loc2.longitude)
    }
}

/// Calculate haversine distance between two points in kilometers
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_KM: f64 = 6371.0;

    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_KM * c
}
