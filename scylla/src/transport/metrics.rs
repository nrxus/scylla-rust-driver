use histogram::Histogram;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

const ORDER_TYPE: Ordering = Ordering::Relaxed;

#[derive(Debug)]
pub enum MetricsError<'a> {
    Poison(PoisonError<MutexGuard<'a, Histogram>>),
    Histogram(&'static str),
}

impl<'a> From<PoisonError<MutexGuard<'a, Histogram>>> for MetricsError<'a> {
    fn from(err: PoisonError<MutexGuard<'_, Histogram>>) -> MetricsError {
        MetricsError::Poison(err)
    }
}

impl From<&'static str> for MetricsError<'_> {
    fn from(err: &'static str) -> MetricsError {
        MetricsError::Histogram(err)
    }
}

impl std::fmt::Display for MetricsError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Default)]
pub struct Metrics {
    errors_num: AtomicU64,
    queries_num: AtomicU64,
    errors_iter_num: AtomicU64,
    queries_iter_num: AtomicU64,
    retries_num: AtomicU64,
    histogram: Arc<Mutex<Histogram>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            errors_num: AtomicU64::new(0),
            queries_num: AtomicU64::new(0),
            errors_iter_num: AtomicU64::new(0),
            queries_iter_num: AtomicU64::new(0),
            retries_num: AtomicU64::new(0),
            histogram: Arc::new(Mutex::new(Histogram::new())),
        }
    }

    /// Increments counter for errors that occured in nonpaged queries.
    pub fn inc_failed_nonpaged_queries(&self) {
        self.errors_num.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for nonpaged queries.
    pub fn inc_total_nonpaged_queries(&self) {
        self.queries_num.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for errors that occured in paged queries.
    pub fn inc_failed_paged_queries(&self) {
        self.errors_iter_num.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for page queries in paged queries.
    /// If query_iter would return 4 pages then this counter should be incremented 4 times.
    pub fn inc_total_paged_queries(&self) {
        self.queries_iter_num.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter measuring how many times a retry policy has decided to retry a query
    pub fn inc_retries_num(&self) {
        self.retries_num.fetch_add(1, ORDER_TYPE);
    }

    /// Saves to histogram latency of completing single query.
    /// For paged queries it should log latency for every page.
    ///
    /// # Arguments
    ///
    /// * `latency` - time in milliseconds that should be logged
    pub fn log_query_latency(&self, latency: u64) -> Result<(), MetricsError> {
        let mut histogram_unlocked = self.histogram.lock()?;
        histogram_unlocked.increment(latency)?;
        Ok(())
    }

    /// Returns average latency in milliseconds
    pub fn get_latency_avg_ms(&self) -> Result<u64, MetricsError> {
        let histogram_unlocked = self.histogram.lock()?;
        Ok(histogram_unlocked.mean()?)
    }

    /// Returns latency from histogram for a given percentile
    /// # Arguments
    ///
    /// * `percentile` - float value (0.0 - 100.0)
    pub fn get_latency_percentile_ms(&self, percentile: f64) -> Result<u64, MetricsError> {
        let histogram_unlocked = self.histogram.lock()?;
        Ok(histogram_unlocked.percentile(percentile)?)
    }

    /// Returns counter for errors occured in nonpaged queries
    pub fn get_errors_num(&self) -> u64 {
        self.errors_num.load(ORDER_TYPE)
    }

    /// Returns counter for nonpaged queries
    pub fn get_queries_num(&self) -> u64 {
        self.queries_num.load(ORDER_TYPE)
    }

    /// Returns counter for errors occured in paged queries
    pub fn get_errors_iter_num(&self) -> u64 {
        self.errors_iter_num.load(ORDER_TYPE)
    }

    /// Returns counter for pages requested in paged queries
    pub fn get_queries_iter_num(&self) -> u64 {
        self.queries_iter_num.load(ORDER_TYPE)
    }

    /// Returns counter measuring how many times a retry policy has decided to retry a query
    pub fn get_retries_num(&self) -> u64 {
        self.retries_num.load(ORDER_TYPE)
    }
}

pub struct MetricsView {
    metrics: Arc<Metrics>,
}

impl MetricsView {
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self { metrics }
    }

    /// Returns average latency in milliseconds
    pub fn get_latency_avg_ms(&self) -> Result<u64, MetricsError> {
        self.metrics.get_latency_avg_ms()
    }

    /// Returns latency from histogram for a given percentile
    /// # Arguments
    ///
    /// * `percentile` - float value (0.0 - 100.0)
    pub fn get_latency_percentile_ms(&self, percentile: f64) -> Result<u64, MetricsError> {
        self.metrics.get_latency_percentile_ms(percentile)
    }

    /// Returns counter for errors occured in nonpaged queries
    pub fn get_errors_num(&self) -> u64 {
        self.metrics.get_errors_num()
    }

    /// Returns counter for nonpaged queries
    pub fn get_queries_num(&self) -> u64 {
        self.metrics.get_queries_num()
    }

    /// Returns counter for errors occured in paged queries
    pub fn get_errors_iter_num(&self) -> u64 {
        self.metrics.get_errors_iter_num()
    }

    /// Returns counter for pages requested in paged queries
    pub fn get_queries_iter_num(&self) -> u64 {
        self.metrics.get_queries_iter_num()
    }

    /// Returns counter measuring how many times a retry policy has decided to retry a query
    pub fn get_retries_num(&self) -> u64 {
        self.metrics.get_retries_num()
    }
}
