mod common;

use common::test_data::*;
use common::*;
use redis::Commands;

#[test]
fn test_multi_exec_basic() {
    let server = TestServer::new();
    let mut conn = server.client();

    let key = unique_key("tx");

    let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();

    redis::cmd("SET")
        .arg(&key)
        .arg("value1")
        .query::<()>(&mut conn)
        .unwrap();

    redis::cmd("SET")
        .arg(&key)
        .arg("value2")
        .query::<()>(&mut conn)
        .unwrap();

    let result: Vec<String> = redis::cmd("EXEC").query(&mut conn).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], "OK");
    assert_eq!(result[1], "OK");

    let value: String = conn.get(&key).unwrap();
    assert_eq!(value, "value2");
}

#[test]
fn test_multi_exec_with_get() {
    let server = TestServer::new();
    let mut conn = server.client();

    let key = unique_key("tx");
    let _: () = conn.set(&key, "initial").unwrap();

    let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();

    redis::cmd("GET").arg(&key).query::<()>(&mut conn).unwrap();

    redis::cmd("SET")
        .arg(&key)
        .arg("updated")
        .query::<()>(&mut conn)
        .unwrap();

    redis::cmd("GET").arg(&key).query::<()>(&mut conn).unwrap();

    let result: Vec<redis::Value> = redis::cmd("EXEC").query(&mut conn).unwrap();
    assert_eq!(result.len(), 3);

    if let redis::Value::BulkString(data) = &result[0] {
        assert_eq!(String::from_utf8_lossy(data), "initial");
    }

    if let redis::Value::BulkString(data) = &result[2] {
        assert_eq!(String::from_utf8_lossy(data), "updated");
    }
}

#[test]
fn test_discard() {
    let server = TestServer::new();
    let mut conn = server.client();

    let key = unique_key("discard");
    let _: () = conn.set(&key, "original").unwrap();

    let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();

    redis::cmd("SET")
        .arg(&key)
        .arg("should_not_be_set")
        .query::<()>(&mut conn)
        .unwrap();

    let _: String = redis::cmd("DISCARD").query(&mut conn).unwrap();

    let value: String = conn.get(&key).unwrap();
    assert_eq!(value, "original");
}

#[test]
fn test_watch_success() {
    let server = TestServer::new();
    let mut conn = server.client();

    let key = unique_key("watch");
    let _: () = conn.set(&key, "initial").unwrap();

    let _: () = redis::cmd("WATCH").arg(&key).query(&mut conn).unwrap();

    let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();

    redis::cmd("SET")
        .arg(&key)
        .arg("updated")
        .query::<()>(&mut conn)
        .unwrap();

    let result: Vec<String> = redis::cmd("EXEC").query(&mut conn).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], "OK");

    let value: String = conn.get(&key).unwrap();
    assert_eq!(value, "updated");
}

#[test]
fn test_watch_abort_on_modification() {
    let server = TestServer::new();

    let mut conn1 = server.client();
    let mut conn2 = server.client();

    let key = unique_key("watch");
    let _: () = conn1.set(&key, "initial").unwrap();

    let _: () = redis::cmd("WATCH").arg(&key).query(&mut conn1).unwrap();

    let _: () = conn2.set(&key, "modified_by_conn2").unwrap();

    let _: () = redis::cmd("MULTI").query(&mut conn1).unwrap();

    redis::cmd("SET")
        .arg(&key)
        .arg("should_fail")
        .query::<()>(&mut conn1)
        .unwrap();

    let result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert!(result.is_none());

    let value: String = conn1.get(&key).unwrap();
    assert_eq!(value, "modified_by_conn2");
}

#[test]
fn test_watch_abort_on_jsonpatch() {
    let server = TestServer::new();

    let mut conn1 = server.client();
    let mut conn2 = server.client();

    let key = unique_key("watch_jsonpatch");
    let _: () = conn1.set(&key, r#"{"status":"pending"}"#).unwrap();
    let _: () = redis::cmd("WATCH").arg(&key).query(&mut conn1).unwrap();

    let patch = r#"[{"op":"replace","path":"/status","value":"complete"}]"#;
    let result: String = redis::cmd("JSONPATCH")
        .arg(&key)
        .arg(patch)
        .query(&mut conn2)
        .unwrap();
    assert_eq!(result, "OK");

    let _: () = redis::cmd("MULTI").query(&mut conn1).unwrap();
    redis::cmd("SET")
        .arg(&key)
        .arg("should_not_be_set")
        .query::<()>(&mut conn1)
        .unwrap();

    let result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert!(result.is_none());

    let value: String = conn1.get(&key).unwrap();
    assert!(value.contains(r#""status":"complete""#));
}

#[test]
fn test_watch_abort_on_successful_cas() {
    let server = TestServer::new();

    let mut conn1 = server.client();
    let mut conn2 = server.client();

    let key = unique_key("watch_cas_success");
    let _: () = conn1.set(&key, "v1").unwrap();
    let _: () = redis::cmd("WATCH").arg(&key).query(&mut conn1).unwrap();

    let swapped: i64 = redis::cmd("CAS")
        .arg(&key)
        .arg("v1")
        .arg("v2")
        .query(&mut conn2)
        .unwrap();
    assert_eq!(swapped, 1);

    let _: () = redis::cmd("MULTI").query(&mut conn1).unwrap();
    redis::cmd("SET")
        .arg(&key)
        .arg("should_not_be_set")
        .query::<()>(&mut conn1)
        .unwrap();

    let result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert!(result.is_none());

    let value: String = conn1.get(&key).unwrap();
    assert_eq!(value, "v2");
}

#[test]
fn test_watch_not_aborted_on_failed_cas() {
    let server = TestServer::new();

    let mut conn1 = server.client();
    let mut conn2 = server.client();

    let key = unique_key("watch_cas_failure");
    let _: () = conn1.set(&key, "v1").unwrap();
    let _: () = redis::cmd("WATCH").arg(&key).query(&mut conn1).unwrap();

    let swapped: i64 = redis::cmd("CAS")
        .arg(&key)
        .arg("wrong-value")
        .arg("v2")
        .query(&mut conn2)
        .unwrap();
    assert_eq!(swapped, 0);

    let _: () = redis::cmd("MULTI").query(&mut conn1).unwrap();
    redis::cmd("SET")
        .arg(&key)
        .arg("transaction-value")
        .query::<()>(&mut conn1)
        .unwrap();

    let result: Vec<String> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(result, vec!["OK"]);

    let value: String = conn1.get(&key).unwrap();
    assert_eq!(value, "transaction-value");
}

#[test]
fn test_unwatch() {
    let server = TestServer::new();

    let mut conn1 = server.client();
    let mut conn2 = server.client();

    let key = unique_key("unwatch");
    let _: () = conn1.set(&key, "initial").unwrap();

    let _: () = redis::cmd("WATCH").arg(&key).query(&mut conn1).unwrap();

    let _: () = redis::cmd("UNWATCH").query(&mut conn1).unwrap();

    let _: () = conn2.set(&key, "modified_by_conn2").unwrap();

    let _: () = redis::cmd("MULTI").query(&mut conn1).unwrap();

    redis::cmd("SET")
        .arg(&key)
        .arg("should_succeed")
        .query::<()>(&mut conn1)
        .unwrap();

    let result: Vec<String> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], "OK");

    let value: String = conn1.get(&key).unwrap();
    assert_eq!(value, "should_succeed");
}

#[test]
fn test_watch_multiple_keys() {
    let server = TestServer::new();

    let mut conn1 = server.client();
    let mut conn2 = server.client();

    let key1 = unique_key("watch1");
    let key2 = unique_key("watch2");

    let _: () = conn1.set(&key1, "initial1").unwrap();
    let _: () = conn1.set(&key2, "initial2").unwrap();

    let _: () = redis::cmd("WATCH")
        .arg(&key1)
        .arg(&key2)
        .query(&mut conn1)
        .unwrap();

    let _: () = conn2.set(&key2, "modified").unwrap();

    let _: () = redis::cmd("MULTI").query(&mut conn1).unwrap();

    redis::cmd("SET")
        .arg(&key1)
        .arg("should_fail")
        .query::<()>(&mut conn1)
        .unwrap();

    let result: Option<Vec<String>> = redis::cmd("EXEC").query(&mut conn1).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_multi_incr() {
    let server = TestServer::new();
    let mut conn = server.client();

    let key = unique_key("counter");
    let _: () = conn.set(&key, "0").unwrap();

    let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();

    redis::cmd("INCR").arg(&key).query::<()>(&mut conn).unwrap();
    redis::cmd("INCR").arg(&key).query::<()>(&mut conn).unwrap();
    redis::cmd("INCR").arg(&key).query::<()>(&mut conn).unwrap();

    let result: Vec<i64> = redis::cmd("EXEC").query(&mut conn).unwrap();
    assert_eq!(result, vec![1, 2, 3]);

    let value: i64 = conn.get(&key).unwrap();
    assert_eq!(value, 3);
}

#[test]
fn test_multi_list_operations() {
    let server = TestServer::new();
    let mut conn = server.client();

    let key = unique_key("list");

    let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();

    redis::cmd("RPUSH")
        .arg(&key)
        .arg("v1")
        .query::<()>(&mut conn)
        .unwrap();

    redis::cmd("RPUSH")
        .arg(&key)
        .arg("v2")
        .query::<()>(&mut conn)
        .unwrap();

    redis::cmd("LLEN").arg(&key).query::<()>(&mut conn).unwrap();

    let result: Vec<i64> = redis::cmd("EXEC").query(&mut conn).unwrap();
    assert_eq!(result, vec![1, 2, 2]);
}

#[test]
fn test_multi_hash_operations() {
    let server = TestServer::new();
    let mut conn = server.client();

    let key = unique_key("hash");

    let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();

    redis::cmd("HSET")
        .arg(&key)
        .arg("field1")
        .arg("value1")
        .query::<()>(&mut conn)
        .unwrap();

    redis::cmd("HSET")
        .arg(&key)
        .arg("field2")
        .arg("value2")
        .query::<()>(&mut conn)
        .unwrap();

    redis::cmd("HLEN").arg(&key).query::<()>(&mut conn).unwrap();

    let result: Vec<i64> = redis::cmd("EXEC").query(&mut conn).unwrap();
    assert_eq!(result, vec![1, 1, 2]);
}

#[test]
fn test_exec_without_multi() {
    let server = TestServer::new();
    let mut conn = server.client();

    let result: Result<Vec<String>, _> = redis::cmd("EXEC").query(&mut conn);
    assert!(result.is_err());
}

#[test]
fn test_discard_without_multi() {
    let server = TestServer::new();
    let mut conn = server.client();

    let result: Result<String, _> = redis::cmd("DISCARD").query(&mut conn);
    assert!(result.is_err());
}

#[test]
fn test_nested_multi_not_allowed() {
    let server = TestServer::new();
    let mut conn = server.client();

    let _: () = redis::cmd("MULTI").query(&mut conn).unwrap();

    let result: Result<(), _> = redis::cmd("MULTI").query(&mut conn);
    assert!(result.is_err());

    let _: String = redis::cmd("DISCARD").query(&mut conn).unwrap();
}
