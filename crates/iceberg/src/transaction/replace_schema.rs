// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use async_trait::async_trait;

use crate::spec::Schema;
use crate::table::Table;
use crate::transaction::action::{ActionCommit, TransactionAction};
use crate::{Error, ErrorKind, Result, TableUpdate};

/// Action to replace a table's schema wholesale by adding a new schema version
/// and setting it as the current schema.
///
/// Unlike [`super::update_schema::UpdateSchemaAction`], which applies incremental
/// column changes, this takes a complete, caller-built [`Schema`].
pub struct ReplaceSchemaAction {
    schema: Option<Schema>,
}

impl ReplaceSchemaAction {
    /// Creates a new empty `ReplaceSchemaAction`.
    pub fn new() -> Self {
        Self { schema: None }
    }

    /// Sets the schema that will replace the current one.
    pub fn set_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }
}

impl Default for ReplaceSchemaAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for ReplaceSchemaAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        let schema = self.schema.clone().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "ReplaceSchemaAction requires a schema to be set",
            )
        })?;

        Ok(ActionCommit::new(
            vec![
                TableUpdate::AddSchema { schema },
                TableUpdate::SetCurrentSchema { schema_id: -1 },
            ],
            vec![],
        ))
    }
}
