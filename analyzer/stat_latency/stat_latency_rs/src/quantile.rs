use crate::model::NodePercentile;
use std::cmp::Ordering;

fn exact_quantile(values: &[f64], q: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let idx = ((sorted.len() - 1) as f64 * q) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[derive(Debug, Clone)]
pub struct QuantileAgg {
    pub count: u32,
    sum: f64,
    min: f64,
    max: f64,
    values: Vec<f64>,
}

impl QuantileAgg {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            values: Vec::new(),
        }
    }

    pub fn insert(&mut self, x: f64) {
        if x.is_nan() {
            return;
        }
        self.count += 1;
        self.sum += x;
        self.min = self.min.min(x);
        self.max = self.max.max(x);
        self.values.push(x);
    }

    pub fn value_for(&self, p: NodePercentile) -> f64 {
        match p {
            NodePercentile::Min => self.min,
            NodePercentile::Max => self.max,
            NodePercentile::Avg => match self.count {
                0 => f64::NAN,
                _ => (self.sum / (self.count as f64) * 100.0).round() / 100.0,
            },
            NodePercentile::P10 => exact_quantile(&self.values, 0.1),
            NodePercentile::P30 => exact_quantile(&self.values, 0.3),
            NodePercentile::P50 => exact_quantile(&self.values, 0.5),
            NodePercentile::P80 => exact_quantile(&self.values, 0.8),
            NodePercentile::P90 => exact_quantile(&self.values, 0.9),
            NodePercentile::P95 => exact_quantile(&self.values, 0.95),
            NodePercentile::P99 => exact_quantile(&self.values, 0.99),
            NodePercentile::P999 => exact_quantile(&self.values, 0.999),
        }
    }
}
