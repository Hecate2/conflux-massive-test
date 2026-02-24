use crate::model::NodePercentile;
use std::cmp::Ordering;

/// Streaming quantile estimator using the P² algorithm (one quantile per instance).
///
/// This intentionally uses constant memory regardless of sample size.
#[derive(Debug, Clone)]
pub struct P2Quantile {
    p: f64,
    count: usize,
    init: Vec<f64>,
    q: [f64; 5],
    n: [i32; 5],
    np: [f64; 5],
    dn: [f64; 5],
}

impl P2Quantile {
    pub fn new(p: f64) -> Self {
        Self {
            p,
            count: 0,
            init: Vec::with_capacity(5),
            q: [0.0; 5],
            n: [0; 5],
            np: [0.0; 5],
            dn: [0.0; 5],
        }
    }

    pub fn insert(&mut self, x: f64) {
        if self.count < 5 {
            self.init.push(x);
            self.count += 1;
            if self.count == 5 {
                self.init.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                for i in 0..5 {
                    self.q[i] = self.init[i];
                    self.n[i] = (i as i32) + 1;
                }
                let p = self.p;
                self.np = [1.0, 1.0 + 2.0 * p, 1.0 + 4.0 * p, 3.0 + 2.0 * p, 5.0];
                self.dn = [0.0, p / 2.0, p, (1.0 + p) / 2.0, 1.0];
            }
            return;
        }

        self.count += 1;

        let k: usize;
        if x < self.q[0] {
            self.q[0] = x;
            k = 0;
        } else if x < self.q[1] {
            k = 0;
        } else if x < self.q[2] {
            k = 1;
        } else if x < self.q[3] {
            k = 2;
        } else if x <= self.q[4] {
            k = 3;
        } else {
            self.q[4] = x;
            k = 3;
        }

        for i in (k + 1)..5 {
            self.n[i] += 1;
        }
        for i in 0..5 {
            self.np[i] += self.dn[i];
        }

        for i in 1..4 {
            let d = self.np[i] - (self.n[i] as f64);
            if (d >= 1.0 && (self.n[i + 1] - self.n[i]) > 1)
                || (d <= -1.0 && (self.n[i - 1] - self.n[i]) < -1)
            {
                let ds: i32 = if d >= 0.0 { 1 } else { -1 };
                let n_im1 = self.n[i - 1] as f64;
                let n_i = self.n[i] as f64;
                let n_ip1 = self.n[i + 1] as f64;

                let q_im1 = self.q[i - 1];
                let q_i = self.q[i];
                let q_ip1 = self.q[i + 1];

                let numerator = (ds as f64)
                    * ((n_i - n_im1 + (ds as f64)) * (q_ip1 - q_i) / (n_ip1 - n_i)
                        + (n_ip1 - n_i - (ds as f64)) * (q_i - q_im1) / (n_i - n_im1));
                let q_parabolic = q_i + numerator / (n_ip1 - n_im1);

                let q_new = match (q_parabolic > q_im1, q_parabolic < q_ip1) {
                    (true, true) => q_parabolic,
                    _ => {
                        let j = (i as i32 + ds) as usize;
                        q_i + (ds as f64) * (self.q[j] - q_i) / ((self.n[j] - self.n[i]) as f64)
                    }
                };

                self.q[i] = q_new;
                self.n[i] += ds;
            }
        }
    }

    pub fn estimate(&self) -> f64 {
        if self.count == 0 {
            return f64::NAN;
        }
        if self.count < 5 {
            let mut tmp = self.init.clone();
            tmp.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            let idx = ((tmp.len() - 1) as f64 * self.p).round() as usize;
            return tmp[idx.min(tmp.len() - 1)];
        }
        self.q[2]
    }
}

#[derive(Debug, Clone)]
pub struct QuantileAgg {
    pub count: u32,
    sum: f64,
    min: f64,
    max: f64,
    p10: P2Quantile,
    p30: P2Quantile,
    p50: P2Quantile,
    p80: P2Quantile,
    p90: P2Quantile,
    p95: P2Quantile,
    p99: P2Quantile,
    p999: P2Quantile,
}

impl QuantileAgg {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            p10: P2Quantile::new(0.1),
            p30: P2Quantile::new(0.3),
            p50: P2Quantile::new(0.5),
            p80: P2Quantile::new(0.8),
            p90: P2Quantile::new(0.9),
            p95: P2Quantile::new(0.95),
            p99: P2Quantile::new(0.99),
            p999: P2Quantile::new(0.999),
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
        self.p10.insert(x);
        self.p30.insert(x);
        self.p50.insert(x);
        self.p80.insert(x);
        self.p90.insert(x);
        self.p95.insert(x);
        self.p99.insert(x);
        self.p999.insert(x);
    }

    pub fn value_for(&self, p: NodePercentile) -> f64 {
        match p {
            NodePercentile::Min => self.min,
            NodePercentile::Max => self.max,
            NodePercentile::Avg => match self.count {
                0 => f64::NAN,
                _ => (self.sum / (self.count as f64) * 100.0).round() / 100.0,
            },
            NodePercentile::P10 => self.p10.estimate(),
            NodePercentile::P30 => self.p30.estimate(),
            NodePercentile::P50 => self.p50.estimate(),
            NodePercentile::P80 => self.p80.estimate(),
            NodePercentile::P90 => self.p90.estimate(),
            NodePercentile::P95 => self.p95.estimate(),
            NodePercentile::P99 => self.p99.estimate(),
            NodePercentile::P999 => self.p999.estimate(),
        }
    }
}
