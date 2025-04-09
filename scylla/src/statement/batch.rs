use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use scylla_cql::frame::frame_errors::{
    BatchSerializationError, BatchStatementSerializationError, CqlRequestSerializationError,
};
use scylla_cql::frame::request;
use scylla_cql::serialize::batch::{BatchValues, BatchValuesIterator};
use scylla_cql::serialize::row::{RowSerializationContext, SerializedValues};
use scylla_cql::serialize::{RowWriter, SerializationError};

use crate::client::execution_profile::ExecutionProfileHandle;
use crate::errors::{BadQuery, ExecutionError, RequestAttemptError};
use crate::observability::history::HistoryListener;
use crate::policies::retry::RetryPolicy;
use crate::routing::Token;
use crate::statement::prepared::{PartitionKeyError, PreparedStatement};
use crate::statement::unprepared::Statement;

use super::bound::BoundStatement;
use super::StatementConfig;
use super::{Consistency, SerialConsistency};
pub use crate::frame::request::batch::BatchType;

/// CQL batch statement.
///
/// This represents a CQL batch that can be executed on a server.
#[derive(Clone)]
pub struct Batch {
    pub(crate) config: StatementConfig,

    pub statements: Vec<BatchStatement>,
    batch_type: BatchType,
}

impl Batch {
    /// Creates a new, empty `Batch` of `batch_type` type.
    pub fn new(batch_type: BatchType) -> Self {
        Self {
            batch_type,
            ..Default::default()
        }
    }

    /// Creates an empty batch, with the configuration of existing batch.
    pub(crate) fn new_from(batch: &Batch) -> Batch {
        let batch_type = batch.get_type();
        let config = batch.config.clone();
        Batch {
            batch_type,
            config,
            ..Default::default()
        }
    }

    /// Creates a new, empty `Batch` of `batch_type` type with the provided statements.
    pub fn new_with_statements(batch_type: BatchType, statements: Vec<BatchStatement>) -> Self {
        Self {
            batch_type,
            statements,
            ..Default::default()
        }
    }

    /// Appends a new statement to the batch.
    pub fn append_statement(&mut self, statement: impl Into<BatchStatement>) {
        self.statements.push(statement.into());
    }

    /// Gets type of batch.
    pub fn get_type(&self) -> BatchType {
        self.batch_type
    }

    /// Sets the consistency to be used when executing this batch.
    pub fn set_consistency(&mut self, c: Consistency) {
        self.config.consistency = Some(c);
    }

    /// Gets the consistency to be used when executing this batch if it is filled.
    /// If this is empty, the default_consistency of the session will be used.
    pub fn get_consistency(&self) -> Option<Consistency> {
        self.config.consistency
    }

    /// Sets the serial consistency to be used when executing this batch.
    /// (Ignored unless the batch is an LWT)
    pub fn set_serial_consistency(&mut self, sc: Option<SerialConsistency>) {
        self.config.serial_consistency = Some(sc);
    }

    /// Gets the serial consistency to be used when executing this batch.
    /// (Ignored unless the batch is an LWT)
    pub fn get_serial_consistency(&self) -> Option<SerialConsistency> {
        self.config.serial_consistency.flatten()
    }

    /// Sets the idempotence of this batch
    /// A query is idempotent if it can be applied multiple times without changing the result of the initial application
    /// If set to `true` we can be sure that it is idempotent
    /// If set to `false` it is unknown whether it is idempotent
    /// This is used in [`RetryPolicy`] to decide if retrying a query is safe
    pub fn set_is_idempotent(&mut self, is_idempotent: bool) {
        self.config.is_idempotent = is_idempotent;
    }

    /// Gets the idempotence of this batch
    pub fn get_is_idempotent(&self) -> bool {
        self.config.is_idempotent
    }

    /// Enable or disable CQL Tracing for this batch
    /// If enabled session.batch() will return a QueryResult containing tracing_id
    /// which can be used to query tracing information about the execution of this query
    pub fn set_tracing(&mut self, should_trace: bool) {
        self.config.tracing = should_trace;
    }

    /// Gets whether tracing is enabled for this batch
    pub fn get_tracing(&self) -> bool {
        self.config.tracing
    }

    /// Sets the default timestamp for this batch in microseconds.
    /// If not None, it will replace the server side assigned timestamp as default timestamp for
    /// all the statements contained in the batch.
    pub fn set_timestamp(&mut self, timestamp: Option<i64>) {
        self.config.timestamp = timestamp
    }

    /// Gets the default timestamp for this batch in microseconds.
    pub fn get_timestamp(&self) -> Option<i64> {
        self.config.timestamp
    }

    /// Set the retry policy for this batch, overriding the one from execution profile if not None.
    #[inline]
    pub fn set_retry_policy(&mut self, retry_policy: Option<Arc<dyn RetryPolicy>>) {
        self.config.retry_policy = retry_policy;
    }

    /// Get the retry policy set for the batch.
    #[inline]
    pub fn get_retry_policy(&self) -> Option<&Arc<dyn RetryPolicy>> {
        self.config.retry_policy.as_ref()
    }

    /// Sets the listener capable of listening what happens during query execution.
    pub fn set_history_listener(&mut self, history_listener: Arc<dyn HistoryListener>) {
        self.config.history_listener = Some(history_listener);
    }

    /// Removes the listener set by `set_history_listener`.
    pub fn remove_history_listener(&mut self) -> Option<Arc<dyn HistoryListener>> {
        self.config.history_listener.take()
    }

    /// Associates the batch with execution profile referred by the provided handle.
    /// Handle may be later remapped to another profile, and batch will reflect those changes.
    pub fn set_execution_profile_handle(&mut self, profile_handle: Option<ExecutionProfileHandle>) {
        self.config.execution_profile_handle = profile_handle;
    }

    /// Borrows the execution profile handle associated with this batch.
    pub fn get_execution_profile_handle(&self) -> Option<&ExecutionProfileHandle> {
        self.config.execution_profile_handle.as_ref()
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            statements: Vec::new(),
            batch_type: BatchType::Logged,
            config: Default::default(),
        }
    }
}

/// This enum represents a CQL statement, that can be part of batch.
#[derive(Clone)]
#[non_exhaustive]
pub enum BatchStatement {
    Query(Statement),
    PreparedStatement(PreparedStatement),
}

impl From<&str> for BatchStatement {
    fn from(s: &str) -> Self {
        BatchStatement::Query(Statement::from(s))
    }
}

impl From<Statement> for BatchStatement {
    fn from(q: Statement) -> Self {
        BatchStatement::Query(q)
    }
}

impl From<PreparedStatement> for BatchStatement {
    fn from(p: PreparedStatement) -> Self {
        BatchStatement::PreparedStatement(p)
    }
}

impl<'a: 'b, 'b> From<&'a BatchStatement>
    for scylla_cql::frame::request::batch::BatchStatement<'b>
{
    fn from(val: &'a BatchStatement) -> Self {
        match val {
            BatchStatement::Query(query) => {
                scylla_cql::frame::request::batch::BatchStatement::Query {
                    text: Cow::Borrowed(&query.contents),
                }
            }
            BatchStatement::PreparedStatement(prepared) => {
                scylla_cql::frame::request::batch::BatchStatement::Prepared {
                    id: Cow::Borrowed(prepared.get_id()),
                }
            }
        }
    }
}

/// A batch with all of its statements bound to values
pub(crate) struct BoundBatch {
    pub(crate) config: StatementConfig,
    batch_type: BatchType,
    pub(crate) buffer: Vec<u8>,
    pub(crate) prepared: HashMap<Bytes, PreparedStatement>,
    pub(crate) first_prepared: Option<(PreparedStatement, Token)>,
    pub(crate) statements_len: u16,
}

impl BoundBatch {
    pub(crate) fn from_batch(
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<Self, ExecutionError> {
        let mut bound_batch = BoundBatch {
            config: batch.config.clone(),
            batch_type: batch.batch_type,
            prepared: HashMap::new(),
            buffer: vec![],
            first_prepared: None,
            statements_len: batch.statements.len().try_into().map_err(|_| {
                ExecutionError::BadQuery(BadQuery::TooManyQueriesInBatchStatement(
                    batch.statements.len(),
                ))
            })?,
        };

        let mut values = values.batch_values_iter();
        let mut statements = batch.statements.iter().enumerate();

        if let Some((idx, statement)) = statements.next() {
            match statement {
                BatchStatement::Query(_) => {
                    bound_batch
                        .serialize(statement, idx, |writer, _| {
                            let ctx = RowSerializationContext::empty();
                            values.serialize_next(&ctx, writer)
                        })
                        .map_err(|e| {
                            ExecutionError::LastAttemptError(
                                RequestAttemptError::CqlRequestSerialization(
                                    CqlRequestSerializationError::BatchSerialization(e),
                                ),
                            )
                        })?;
                }
                BatchStatement::PreparedStatement(ps) => {
                    let values = bound_batch
                        .serialize(statement, idx, |writer, _| {
                            let ctx =
                                RowSerializationContext::from_prepared(ps.get_prepared_metadata());

                            let values = SerializedValues::from_closure(|writer| {
                                values.serialize_next(&ctx, writer).transpose()
                            })
                            .map(|(values, opt)| opt.map(|_| values))
                            .transpose();

                            if let Some(Ok(values)) = &values {
                                writer.append_serialize_row(values);
                            }

                            values
                        })
                        .map_err(|e| {
                            ExecutionError::LastAttemptError(
                                RequestAttemptError::CqlRequestSerialization(
                                    CqlRequestSerializationError::BatchSerialization(e),
                                ),
                            )
                        })?;

                    let statement = BoundStatement::new_untyped(ps.clone(), values);
                    bound_batch.first_prepared = statement
                        .token()
                        .map_err(PartitionKeyError::into_execution_error)?
                        .map(|token| (statement.prepared.clone(), token));
                    bound_batch
                        .prepared
                        .insert(statement.prepared.get_id().to_owned(), statement.prepared);
                }
            }
        }

        for (idx, statement) in statements {
            bound_batch
                .serialize(statement, idx, |writer, prepared| {
                    let ctx = match statement {
                        BatchStatement::Query(_) => RowSerializationContext::empty(),
                        BatchStatement::PreparedStatement(ps) => {
                            if !prepared.contains_key(ps.get_id()) {
                                prepared.insert(ps.get_id().to_owned(), ps.clone());
                            }

                            RowSerializationContext::from_prepared(ps.get_prepared_metadata())
                        }
                    };
                    values.serialize_next(&ctx, writer)
                })
                .map_err(|e| {
                    ExecutionError::LastAttemptError(RequestAttemptError::CqlRequestSerialization(
                        CqlRequestSerializationError::BatchSerialization(e),
                    ))
                })?;
        }

        // At this point, we have all statements serialized. If any values are still left, we have a mismatch.
        if values.skip_next().is_some() {
            return Err(ExecutionError::LastAttemptError(
                RequestAttemptError::CqlRequestSerialization(
                    CqlRequestSerializationError::BatchSerialization(counts_mismatch_err(
                        bound_batch.statements_len as usize + 1 /*skipped above*/ + values.count(),
                        bound_batch.statements_len,
                    )),
                ),
            ));
        }

        Ok(bound_batch)
    }

    /// Borrows the execution profile handle associated with this batch.
    pub(crate) fn get_execution_profile_handle(&self) -> Option<&ExecutionProfileHandle> {
        self.config.execution_profile_handle.as_ref()
    }

    /// Gets the default timestamp for this batch in microseconds.
    pub(crate) fn get_timestamp(&self) -> Option<i64> {
        self.config.timestamp
    }

    /// Gets type of batch.
    pub(crate) fn get_type(&self) -> BatchType {
        self.batch_type
    }

    fn serialize<T>(
        &mut self,
        statement: &BatchStatement,
        statement_idx: usize,
        serialize: impl FnOnce(
            &mut RowWriter<'_>,
            &mut HashMap<Bytes, PreparedStatement>,
        ) -> Option<Result<T, SerializationError>>,
    ) -> Result<T, BatchSerializationError> {
        request::batch::BatchStatement::from(statement)
            .serialize(&mut self.buffer)
            .map_err(|error| BatchSerializationError::StatementSerialization {
                statement_idx,
                error,
            })?;

        // Reserve two bytes for length
        let length_pos = self.buffer.len();
        self.buffer.extend_from_slice(&[0, 0]);

        // serialize the values
        let mut writer = RowWriter::new(&mut self.buffer);
        let res = serialize(&mut writer, &mut self.prepared)
            .map(|r| {
                r.map_err(|e| BatchSerializationError::StatementSerialization {
                    statement_idx,
                    error: BatchStatementSerializationError::ValuesSerialiation(e),
                })
            })
            .unwrap_or_else(|| Err(counts_mismatch_err(statement_idx, self.statements_len)))?;

        // Go back and put the length
        let count: u16 = writer.value_count().try_into().map_err(|_| {
            BatchSerializationError::StatementSerialization {
                statement_idx,
                error: BatchStatementSerializationError::TooManyValues(writer.value_count()),
            }
        })?;

        self.buffer[length_pos..length_pos + 2].copy_from_slice(&count.to_be_bytes());

        Ok(res)
    }
}

fn counts_mismatch_err(n_value_lists: usize, n_statements: u16) -> BatchSerializationError {
    BatchSerializationError::ValuesAndStatementsLengthMismatch {
        n_value_lists,
        n_statements: n_statements as usize,
    }
}
