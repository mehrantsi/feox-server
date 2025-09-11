use crate::client_registry::ClientRegistry;
use crate::protocol::resp::RespValue;
use bytes::Bytes;
use std::sync::Arc;

/// Handles CLIENT command operations
pub struct ClientOperations {
    registry: Option<Arc<ClientRegistry>>,
}

impl ClientOperations {
    pub fn new() -> Self {
        Self { registry: None }
    }

    pub fn with_registry(registry: Arc<ClientRegistry>) -> Self {
        Self {
            registry: Some(registry),
        }
    }

    pub fn execute(
        &self,
        subcommand: &str,
        args: &[Vec<u8>],
        connection_id: Option<usize>,
    ) -> RespValue {
        match subcommand.to_uppercase().as_str() {
            "ID" => self.client_id(connection_id),
            "LIST" => self.client_list(),
            "SETNAME" => self.client_setname(args),
            "GETNAME" => self.client_getname(connection_id),
            "KILL" => self.client_kill(args),
            "INFO" => self.client_info(connection_id),
            "PAUSE" => self.client_pause(args),
            "UNPAUSE" => self.client_unpause(),
            _ => RespValue::Error(format!("-ERR Unknown CLIENT subcommand '{}'", subcommand)),
        }
    }

    fn client_id(&self, connection_id: Option<usize>) -> RespValue {
        if let Some(conn_id) = connection_id {
            RespValue::Integer(conn_id as i64)
        } else {
            RespValue::Error("-ERR Client info not available".to_string())
        }
    }

    fn client_list(&self) -> RespValue {
        if let Some(ref registry) = self.registry {
            let clients = registry.get_all_clients();
            let mut output = String::new();

            for client in clients {
                output.push_str(&format!(
                    "id={} addr={} fd={} name={} age={} idle={} flags={} db={} sub={} psub={} ssub={} multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client user=default redir=-1 resp=2\n",
                    client.id,
                    client.addr.map(|a| a.to_string()).unwrap_or_else(|| "N/A".to_string()),
                    client.fd,
                    client.name.as_deref().unwrap_or(""),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                        .saturating_sub(client.connected_at),
                    0,
                    if client.flags.is_empty() { "N".to_string() } else { client.flags.join("") },
                    client.db,
                    0,  // subscriptions
                    0,  // pattern subscriptions
                    0,  // shard subscriptions
                ));
            }

            RespValue::BulkString(Some(Bytes::from(output)))
        } else {
            RespValue::Error("-ERR Client registry not available".to_string())
        }
    }

    fn client_setname(&self, args: &[Vec<u8>]) -> RespValue {
        if args.is_empty() {
            RespValue::Error(
                "-ERR wrong number of arguments for 'CLIENT SETNAME' command".to_string(),
            )
        } else {
            // Name is stored in connection and updated in registry by caller
            RespValue::SimpleString(Bytes::from_static(b"OK"))
        }
    }

    fn client_getname(&self, connection_id: Option<usize>) -> RespValue {
        if let (Some(ref registry), Some(conn_id)) = (&self.registry, connection_id) {
            if let Some(client) = registry.get_client(conn_id) {
                if let Some(name) = client.name {
                    RespValue::BulkString(Some(Bytes::from(name)))
                } else {
                    RespValue::BulkString(None)
                }
            } else {
                RespValue::BulkString(None)
            }
        } else {
            RespValue::BulkString(None)
        }
    }

    fn client_kill(&self, args: &[Vec<u8>]) -> RespValue {
        if args.is_empty() {
            return RespValue::Error(
                "-ERR wrong number of arguments for 'CLIENT KILL' command".to_string(),
            );
        }

        let mut filter_id = None;
        let mut filter_addr = None;
        let mut filter_type = None;

        let mut i = 0;
        while i < args.len() {
            let arg_str = String::from_utf8_lossy(&args[i]).to_uppercase();
            match arg_str.as_str() {
                "ID" if i + 1 < args.len() => {
                    if let Ok(id) = String::from_utf8_lossy(&args[i + 1]).parse::<usize>() {
                        filter_id = Some(id);
                    }
                    i += 2;
                }
                "ADDR" if i + 1 < args.len() => {
                    filter_addr = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                    i += 2;
                }
                "TYPE" if i + 1 < args.len() => {
                    filter_type = Some(String::from_utf8_lossy(&args[i + 1]).to_string());
                    i += 2;
                }
                _ => {
                    // Legacy format: CLIENT KILL <addr>
                    if i == 0 && args.len() == 1 {
                        filter_addr = Some(String::from_utf8_lossy(&args[0]).to_string());
                    }
                    i += 1;
                }
            }
        }

        if let Some(ref registry) = self.registry {
            let to_kill =
                registry.kill_clients(filter_id, filter_addr.as_deref(), filter_type.as_deref());
            RespValue::Integer(to_kill.len() as i64)
        } else {
            RespValue::Integer(0)
        }
    }

    fn client_info(&self, connection_id: Option<usize>) -> RespValue {
        if let (Some(ref registry), Some(conn_id)) = (&self.registry, connection_id) {
            if let Some(client) = registry.get_client(conn_id) {
                let info = format!(
                    "id={}\naddr={}\nfd={}\nname={}\nage={}\nidle={}\nflags={}\ndb={}\nsub={}\npsub={}\nssub={}\nmulti=-1\nqbuf=0\nqbuf-free=0\nargv-mem=0\nmulti-mem=0\nrbs=0\nrbp=0\nobl=0\noll=0\nomem=0\ntot-mem=0\nevents=r\ncmd=client\nuser=default\nredir=-1\nresp=2",
                    client.id,
                    client.addr.map(|a| a.to_string()).unwrap_or_else(|| "N/A".to_string()),
                    client.fd,
                    client.name.as_deref().unwrap_or(""),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                        .saturating_sub(client.connected_at),
                    0,
                    if client.flags.is_empty() { "N".to_string() } else { client.flags.join("") },
                    client.db,
                    0,  // subscriptions
                    0,  // pattern subscriptions
                    0,  // shard subscriptions
                );
                RespValue::BulkString(Some(Bytes::from(info)))
            } else {
                RespValue::Error("-ERR Client not found".to_string())
            }
        } else {
            RespValue::Error("-ERR Client info not available".to_string())
        }
    }

    fn client_pause(&self, args: &[Vec<u8>]) -> RespValue {
        if args.is_empty() {
            RespValue::Error(
                "-ERR wrong number of arguments for 'CLIENT PAUSE' command".to_string(),
            )
        } else {
            // Simple implementation - actual pausing would require server-level support
            RespValue::SimpleString(Bytes::from_static(b"OK"))
        }
    }

    fn client_unpause(&self) -> RespValue {
        RespValue::SimpleString(Bytes::from_static(b"OK"))
    }
}

impl Clone for ClientOperations {
    fn clone(&self) -> Self {
        Self {
            registry: self.registry.clone(),
        }
    }
}
