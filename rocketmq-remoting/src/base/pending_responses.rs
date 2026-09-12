// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use super::response_future::ResponseFuture;
use rocketmq_error::RocketMQError;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PendingKey {
    connection_id: String,
    opaque: i32,
}

pub(crate) struct PendingEntry {
    token: u64,
    future: ResponseFuture,
}

struct PendingResponsesInner {
    entries: Mutex<HashMap<PendingKey, PendingEntry>>,
    next_token: AtomicU64,
}

/// A synchronized owner for response waiters shared by remoting tasks.
#[derive(Clone)]
pub struct PendingResponses {
    inner: Arc<PendingResponsesInner>,
}

/// Identity of one pending request. It is local bookkeeping and is never sent on the wire.
#[derive(Clone)]
pub(crate) struct PendingIdentity {
    key: PendingKey,
    token: u64,
    timeout_millis: u64,
}

pub(crate) struct PendingRegistration {
    table: PendingResponses,
    identity: PendingIdentity,
}

#[derive(Clone)]
pub(crate) struct PendingRequest {
    table: PendingResponses,
    identity: PendingIdentity,
}

impl PendingResponses {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: Arc::new(PendingResponsesInner {
                entries: Mutex::new(HashMap::with_capacity(capacity)),
                next_token: AtomicU64::new(1),
            }),
        }
    }

    pub(crate) fn register(
        &self,
        connection_id: &str,
        future: ResponseFuture,
    ) -> Result<PendingRegistration, ResponseFuture> {
        let key = PendingKey {
            connection_id: connection_id.to_owned(),
            opaque: future.opaque,
        };
        let timeout_millis = future.timeout_millis;
        let token = self.inner.next_token.fetch_add(1, Ordering::Relaxed);
        let mut entries = self.inner.entries.lock().expect("pending response lock poisoned");
        if entries.contains_key(&key) {
            return Err(future);
        }
        entries.insert(key.clone(), PendingEntry { token, future });
        crate::metrics::pending_registered();
        Ok(PendingRegistration {
            table: self.clone(),
            identity: PendingIdentity {
                key,
                token,
                timeout_millis,
            },
        })
    }

    pub(crate) fn is_registered(&self, identity: &PendingIdentity) -> bool {
        self.inner
            .entries
            .lock()
            .expect("pending response lock poisoned")
            .get(&identity.key)
            .is_some_and(|entry| entry.token == identity.token)
    }

    pub(crate) fn take(&self, connection_id: &str, opaque: i32) -> Option<ResponseFuture> {
        self.inner
            .entries
            .lock()
            .expect("pending response lock poisoned")
            .remove(&PendingKey {
                connection_id: connection_id.to_owned(),
                opaque,
            })
            .map(|entry| {
                crate::metrics::pending_removed();
                entry.future
            })
    }

    pub(crate) fn remove_if_token_matches(&self, identity: &PendingIdentity) -> Option<ResponseFuture> {
        let mut entries = self.inner.entries.lock().expect("pending response lock poisoned");
        let matches = entries
            .get(&identity.key)
            .is_some_and(|entry| entry.token == identity.token);
        matches.then(|| {
            crate::metrics::pending_removed();
            entries.remove(&identity.key).expect("pending entry disappeared").future
        })
    }

    fn finish_identity(&self, identity: &PendingIdentity, error: RocketMQError) {
        if let Some(future) = self.remove_if_token_matches(identity) {
            let _ = future.tx.send(Err(error));
        }
    }

    pub(crate) fn fail_identity(&self, identity: &PendingIdentity, message: impl Into<String>) {
        self.finish_identity(
            identity,
            RocketMQError::network_connection_failed("remoting", message.into()),
        );
    }

    pub(crate) fn timeout_identity(&self, identity: &PendingIdentity, timeout_millis: u64) {
        self.finish_identity(
            identity,
            RocketMQError::Timeout {
                operation: "send_request",
                timeout_ms: timeout_millis,
            },
        );
    }

    pub(crate) fn fail_connection(&self, connection_id: &str, message: impl Into<String>) {
        let entries = {
            let mut pending = self.inner.entries.lock().expect("pending response lock poisoned");
            let keys: Vec<_> = pending
                .keys()
                .filter(|key| key.connection_id == connection_id)
                .cloned()
                .collect();
            keys.into_iter()
                .filter_map(|key| {
                    pending.remove(&key).map(|entry| {
                        crate::metrics::pending_removed();
                        entry.future
                    })
                })
                .collect::<Vec<_>>()
        };
        let message = message.into();
        for future in entries {
            let _ = future.tx.send(Err(RocketMQError::network_connection_failed(
                "remoting",
                message.clone(),
            )));
        }
    }

    pub fn len(&self) -> usize {
        self.inner.entries.lock().expect("pending response lock poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    fn len_for_test(&self) -> usize {
        self.inner.entries.lock().expect("pending response lock poisoned").len()
    }
}

impl PendingRegistration {
    pub(crate) fn timeout(&self, timeout_millis: u64) {
        self.table.timeout_identity(&self.identity, timeout_millis);
    }

    pub(crate) fn request(&self) -> PendingRequest {
        PendingRequest {
            table: self.table.clone(),
            identity: self.identity.clone(),
        }
    }
}

impl PendingRequest {
    pub(crate) fn timeout_millis(&self) -> u64 {
        self.identity.timeout_millis
    }

    pub(crate) fn is_registered(&self) -> bool {
        self.table.is_registered(&self.identity)
    }

    pub(crate) fn fail(&self, message: impl Into<String>) {
        self.table.fail_identity(&self.identity, message);
    }

    pub(crate) fn timeout(&self, timeout_millis: u64) {
        self.table.timeout_identity(&self.identity, timeout_millis);
    }
}

impl Drop for PendingRegistration {
    fn drop(&mut self) {
        let _ = self.table.remove_if_token_matches(&self.identity);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocketmq_error::RocketMQResult;
    use tokio::sync::oneshot;

    fn future(
        opaque: i32,
    ) -> (
        ResponseFuture,
        oneshot::Receiver<RocketMQResult<crate::protocol::remoting_command::RemotingCommand>>,
    ) {
        let (tx, rx) = oneshot::channel();
        (ResponseFuture::new(opaque, 1000, true, tx), rx)
    }

    #[test]
    fn registration_is_connection_scoped_and_cleanup_is_token_safe() {
        let table = PendingResponses::with_capacity(1);
        let (future_a, _rx_a) = future(7);
        let registration_a = table.register("a", future_a).map_err(|_| ()).unwrap();
        assert!(table.register("a", future(7).0).is_err());
        let registration_b = table.register("b", future(7).0).map_err(|_| ()).unwrap();
        assert!(table.remove_if_token_matches(&registration_a.identity).is_some());
        assert_eq!(table.len_for_test(), 1);
        drop(registration_b);
    }

    #[test]
    fn response_take_removes_before_completion() {
        let table = PendingResponses::with_capacity(1);
        let (future, _rx) = future(9);
        let _registration = table.register("a", future).map_err(|_| ()).unwrap();
        assert!(table.take("a", 9).is_some());
        assert_eq!(table.len_for_test(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_growth_response_and_connection_cleanup_are_race_safe() {
        const CONNECTIONS: usize = 4;
        const REQUESTS_PER_CONNECTION: usize = 1024;
        let table = PendingResponses::with_capacity(1);
        let barrier = Arc::new(tokio::sync::Barrier::new(CONNECTIONS));
        let mut workers = Vec::with_capacity(CONNECTIONS);

        for connection in 0..CONNECTIONS {
            let table = table.clone();
            let barrier = barrier.clone();
            workers.push(tokio::spawn(async move {
                let connection_id = format!("connection-{connection}");
                let mut registrations = Vec::with_capacity(REQUESTS_PER_CONNECTION);
                let mut receivers = Vec::with_capacity(REQUESTS_PER_CONNECTION);
                for offset in 0..REQUESTS_PER_CONNECTION {
                    let opaque = (connection * REQUESTS_PER_CONNECTION + offset) as i32;
                    let (request, receiver) = future(opaque);
                    receivers.push(receiver);
                    registrations.push(
                        table
                            .register(&connection_id, request)
                            .unwrap_or_else(|_| panic!("unique opaque must register: {opaque}")),
                    );
                }
                barrier.wait().await;

                for offset in (0..REQUESTS_PER_CONNECTION).step_by(2) {
                    let opaque = (connection * REQUESTS_PER_CONNECTION + offset) as i32;
                    assert!(table.take(&connection_id, opaque).is_some());
                }
                table.fail_connection(&connection_id, "test disconnect");

                for receiver in receivers {
                    match receiver.await {
                        Ok(Err(_)) | Err(_) => {}
                        Ok(Ok(_)) => panic!("test disconnect must not produce a response"),
                    }
                }
                drop(registrations);
            }));
        }

        for worker in workers {
            worker.await.expect("worker must not panic");
        }
        assert!(table.is_empty());
    }
}
