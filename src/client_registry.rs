use crate::network::Connection;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub id: usize,
    pub name: Option<String>,
    pub addr: Option<SocketAddr>,
    pub fd: i32,
    pub connected_at: u64,
    pub commands_processed: u64,
    pub flags: Vec<String>,
    pub thread_id: usize,
    pub db: usize,
}

/// Global registry for all client connections
pub struct ClientRegistry {
    clients: Arc<DashMap<usize, ClientInfo>>,
}

impl ClientRegistry {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(DashMap::new()),
        }
    }

    /// Register a new client connection
    pub fn register(&self, conn: &Connection, thread_id: usize) {
        let info = ClientInfo {
            id: conn.connection_id,
            name: conn.client_name.clone(),
            addr: conn.client_addr,
            fd: conn.fd(),
            connected_at: conn.connected_at,
            commands_processed: conn.commands_processed,
            flags: conn.flags.clone(),
            thread_id,
            db: 0,
        };
        self.clients.insert(conn.connection_id, info);
    }

    /// Update client info
    pub fn update(&self, conn: &Connection) {
        if let Some(mut entry) = self.clients.get_mut(&conn.connection_id) {
            entry.name = conn.client_name.clone();
            entry.commands_processed = conn.commands_processed;
            entry.flags = conn.flags.clone();
        }
    }

    /// Unregister a client connection
    pub fn unregister(&self, connection_id: usize) {
        self.clients.remove(&connection_id);
    }

    /// Get all client information
    pub fn get_all_clients(&self) -> Vec<ClientInfo> {
        self.clients
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get client by ID
    pub fn get_client(&self, connection_id: usize) -> Option<ClientInfo> {
        self.clients.get(&connection_id).map(|e| e.clone())
    }

    /// Kill clients matching criteria
    pub fn kill_clients(
        &self,
        filter_id: Option<usize>,
        filter_addr: Option<&str>,
        filter_type: Option<&str>,
    ) -> Vec<usize> {
        let mut to_kill = Vec::new();

        for entry in self.clients.iter() {
            let client = entry.value();
            let mut should_kill = false;

            if let Some(id) = filter_id {
                if client.id == id {
                    should_kill = true;
                }
            }

            if let Some(addr_str) = filter_addr {
                if let Some(addr) = client.addr {
                    if addr.to_string() == addr_str {
                        should_kill = true;
                    }
                }
            }

            if let Some(client_type) = filter_type {
                match client_type {
                    "normal" => {
                        if !client.flags.contains(&"pubsub".to_string()) {
                            should_kill = true;
                        }
                    }
                    "pubsub" => {
                        if client.flags.contains(&"pubsub".to_string()) {
                            should_kill = true;
                        }
                    }
                    _ => {}
                }
            }

            if should_kill {
                to_kill.push(client.id);
            }
        }

        to_kill
    }

    /// Count total clients
    pub fn client_count(&self) -> usize {
        self.clients.len()
    }
}

impl Default for ClientRegistry {
    fn default() -> Self {
        Self::new()
    }
}
