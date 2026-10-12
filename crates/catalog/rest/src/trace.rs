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

use std::future::Future;

tokio::task_local! {
    static TRACE_ID: u128;
}

/// Runs `operation` with `trace_id` as the W3C trace id of every request it sends.
/// Tasks spawned inside `operation` don't inherit it. A zero id is invalid in W3C
/// and is treated as no trace id.
pub async fn with_trace_id<F: Future>(trace_id: u128, operation: F) -> F::Output {
    TRACE_ID.scope(trace_id, operation).await
}

/// The trace id set by the enclosing [`with_trace_id`], if any.
pub fn current_trace_id() -> Option<u128> {
    TRACE_ID
        .try_with(|trace_id| *trace_id)
        .ok()
        .filter(|&trace_id| trace_id != 0)
}

/// A W3C `traceparent` header value: version `00`, sampled.
pub fn traceparent(trace_id: u128, span_id: u64) -> String {
    format!("00-{trace_id:032x}-{span_id:016x}-01")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_current_trace_id_is_scoped_to_with_trace_id() {
        assert_eq!(current_trace_id(), None);
        assert_eq!(
            with_trace_id(42, async { current_trace_id() }).await,
            Some(42)
        );
        assert_eq!(current_trace_id(), None);
    }

    #[tokio::test]
    async fn test_zero_trace_id_is_ignored() {
        assert_eq!(with_trace_id(0, async { current_trace_id() }).await, None);
    }

    #[test]
    fn test_traceparent_format() {
        assert_eq!(
            traceparent(1, 2),
            "00-00000000000000000000000000000001-0000000000000002-01"
        );
    }
}
