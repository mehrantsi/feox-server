mod common;

use common::test_data::*;
use common::*;
use redis::Commands;

#[test]
fn test_ping() {
    let server = TestServer::new();
    let mut conn = server.client();

    let result: String = redis::cmd("PING").query(&mut conn).unwrap();
    assert_eq!(result, "PONG");
}

#[test]
fn test_ping_with_message() {
    let server = TestServer::new();
    let mut conn = server.client();

    let message = "hello";
    let result: String = redis::cmd("PING").arg(message).query(&mut conn).unwrap();
    assert_eq!(result, message);
}

#[test]
fn test_echo() {
    let server = TestServer::new();
    let mut conn = server.client();

    let message = "test message";
    let result: String = redis::cmd("ECHO").arg(message).query(&mut conn).unwrap();
    assert_eq!(result, message);
}

#[test]
fn test_info() {
    let server = TestServer::new();
    let mut conn = server.client();

    let info: String = redis::cmd("INFO").query(&mut conn).unwrap();
    assert!(info.contains("redis_version"));
    assert!(info.contains("feox"));
}

#[test]
fn test_info_server() {
    let server = TestServer::new();
    let mut conn = server.client();

    let info: String = redis::cmd("INFO").arg("server").query(&mut conn).unwrap();
    assert!(info.contains("redis_version"));
}

#[test]
fn test_command() {
    let server = TestServer::new();
    let mut conn = server.client();

    let result: Vec<redis::Value> = redis::cmd("COMMAND").query(&mut conn).unwrap();

    assert!(!result.is_empty());
    assert!(result.len() > 50);
}

#[test]
fn test_command_count() {
    let server = TestServer::new();
    let mut conn = server.client();

    let count: i64 = redis::cmd("COMMAND").arg("COUNT").query(&mut conn).unwrap();

    assert!(count > 50);
}

#[test]
fn test_command_info() {
    let server = TestServer::new();
    let mut conn = server.client();

    let result: Vec<redis::Value> = redis::cmd("COMMAND")
        .arg("INFO")
        .arg("GET")
        .arg("SET")
        .query(&mut conn)
        .unwrap();

    assert_eq!(result.len(), 2);
}

#[test]
fn test_command_info_feox_extensions() {
    let server = TestServer::new();
    let mut conn = server.client();

    let result: Vec<redis::Value> = redis::cmd("COMMAND")
        .arg("INFO")
        .arg("JSONPATCH")
        .arg("CAS")
        .query(&mut conn)
        .unwrap();

    assert_eq!(result.len(), 2);

    for (value, expected_name, expected_arity) in [
        (&result[0], b"jsonpatch".as_slice(), 3),
        (&result[1], b"cas".as_slice(), 4),
    ] {
        let redis::Value::Array(fields) = value else {
            panic!("expected command metadata array");
        };
        assert_eq!(fields.len(), 10);
        assert!(matches!(
            &fields[0],
            redis::Value::BulkString(name) if name == expected_name
        ));
        assert!(matches!(&fields[1], redis::Value::Int(arity) if *arity == expected_arity));
        assert!(matches!(&fields[3], redis::Value::Int(1)));
        assert!(matches!(&fields[4], redis::Value::Int(1)));

        let redis::Value::Array(flags) = &fields[2] else {
            panic!("expected command flags array");
        };
        assert!(flags
            .iter()
            .any(|flag| matches!(flag, redis::Value::BulkString(value) if value == b"write")));
        assert!(flags
            .iter()
            .any(|flag| matches!(flag, redis::Value::BulkString(value) if value == b"denyoom")));
    }
}

#[test]
fn test_command_list() {
    let server = TestServer::new();
    let mut conn = server.client();

    let commands: Vec<String> = redis::cmd("COMMAND").arg("LIST").query(&mut conn).unwrap();

    assert!(commands.contains(&"get".to_string()));
    assert!(commands.contains(&"set".to_string()));
    assert!(commands.contains(&"hset".to_string()));
    assert!(commands.contains(&"lpush".to_string()));
    assert!(commands.contains(&"jsonpatch".to_string()));
    assert!(commands.contains(&"cas".to_string()));
}

#[test]
fn test_command_docs() {
    let server = TestServer::new();
    let mut conn = server.client();

    let result: Vec<redis::Value> = redis::cmd("COMMAND").arg("DOCS").query(&mut conn).unwrap();

    assert_eq!(result.len(), 0);
}

#[test]
fn test_config_get() {
    let server = TestServer::new();
    let mut conn = server.client();

    let result: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory")
        .query(&mut conn)
        .unwrap();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0], "maxmemory");
}

#[test]
fn test_client_list() {
    let server = TestServer::new();
    let mut conn = server.client();

    let result: String = redis::cmd("CLIENT").arg("LIST").query(&mut conn).unwrap();

    assert!(result.contains("addr="));
}

#[test]
fn test_client_setname_getname() {
    let server = TestServer::new();
    let mut conn = server.client();

    let _: String = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg("test-client")
        .query(&mut conn)
        .unwrap();

    let name: String = redis::cmd("CLIENT")
        .arg("GETNAME")
        .query(&mut conn)
        .unwrap();

    assert_eq!(name, "test-client");
}

#[test]
fn test_client_id() {
    let server = TestServer::new();
    let mut conn = server.client();

    let id: i64 = redis::cmd("CLIENT").arg("ID").query(&mut conn).unwrap();

    assert!(id > 0);
}

#[test]
fn test_client_info() {
    let server = TestServer::new();
    let mut conn = server.client();

    let info: String = redis::cmd("CLIENT").arg("INFO").query(&mut conn).unwrap();

    assert!(info.contains("addr="));
    assert!(info.contains("id="));
}

#[test]
fn test_quit() {
    let server = TestServer::new();
    let mut conn = server.client();

    let key = unique_key("test");
    let _: () = conn.set(&key, "value").unwrap();

    let result: String = redis::cmd("QUIT").query(&mut conn).unwrap();
    assert_eq!(result, "OK");
}

#[test]
fn test_multiple_clients() {
    let server = TestServer::new();

    let mut conn1 = server.client();
    let mut conn2 = server.client();
    let mut conn3 = server.client();

    let key = unique_key("multi");
    let _: () = conn1.set(&key, "value1").unwrap();

    let value: String = conn2.get(&key).unwrap();
    assert_eq!(value, "value1");

    let _: () = conn3.set(&key, "value2").unwrap();

    let value: String = conn1.get(&key).unwrap();
    assert_eq!(value, "value2");
}

#[test]
fn test_concurrent_operations() {
    let server = TestServer::new();
    let url = server.url();

    let handles: Vec<_> = (0..10)
        .map(|i| {
            let url = url.clone();
            std::thread::spawn(move || {
                let client = redis::Client::open(url).unwrap();
                let mut conn = client.get_connection().unwrap();

                let key = format!("concurrent_{}", i);
                let value = format!("value_{}", i);

                let _: () = conn.set(&key, &value).unwrap();
                let result: String = conn.get(&key).unwrap();
                assert_eq!(result, value);
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_server_with_different_thread_counts() {
    let server1 = TestServer::with_threads(1);
    let mut conn1 = server1.client();
    let _: String = redis::cmd("PING").query(&mut conn1).unwrap();

    let server2 = TestServer::with_threads(8);
    let mut conn2 = server2.client();
    let _: String = redis::cmd("PING").query(&mut conn2).unwrap();
}

#[test]
fn test_info_contains_expected_fields() {
    let server = TestServer::new();
    let mut conn = server.client();

    let info: String = redis::cmd("INFO").query(&mut conn).unwrap();

    assert!(info.contains("redis_version"));
    assert!(info.contains("arch_bits"));
    assert!(info.contains("process_id"));
    assert!(info.contains("tcp_port"));
    assert!(info.contains("uptime_in_seconds"));
}
