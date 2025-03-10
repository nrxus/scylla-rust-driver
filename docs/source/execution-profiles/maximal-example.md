# All options supported by a profile

### Example
`ExecutionProfile` supports all the following options:
```rust
# extern crate scylla;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::statement::unprepared::Statement;
use scylla::policies::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::statement::{Consistency, SerialConsistency};
use scylla::client::execution_profile::ExecutionProfile;
use scylla::policies::load_balancing::DefaultPolicy;
use scylla::policies::retry::FallthroughRetryPolicy;
use std::{sync::Arc, time::Duration};

let profile = ExecutionProfile::builder()
    .consistency(Consistency::All)
    .serial_consistency(Some(SerialConsistency::Serial))
    .request_timeout(Some(Duration::from_secs(30)))
    .retry_policy(Arc::new(FallthroughRetryPolicy::new()))
    .load_balancing_policy(Arc::new(DefaultPolicy::default()))
    .speculative_execution_policy(
        Some(
            Arc::new(
                SimpleSpeculativeExecutionPolicy {
                    max_retry_count: 3,
                    retry_interval: Duration::from_millis(100),
                }
            )
        )
    )
    .build();

let mut query = Statement::from("SELECT * FROM ks.table");
query.set_execution_profile_handle(Some(profile.into_handle()));

# Ok(())
# }
```