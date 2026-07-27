#![cfg(unix)]

use feox_server::Config;
use redis::Commands;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

fn available_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn start_server(config_path: &std::path::Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_feox-server"))
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn signal_server(process: &Child, signal: libc::c_int) {
    let result = unsafe { libc::kill(process.id() as libc::pid_t, signal) };
    assert_eq!(result, 0, "failed to signal test server");
}

fn stop_server(mut process: Child) {
    signal_server(&process, libc::SIGINT);
    assert!(process.wait().unwrap().success());
}

fn kill_server(mut process: Child) {
    signal_server(&process, libc::SIGKILL);
    assert!(!process.wait().unwrap().success());
}

fn connect(port: u16) -> redis::Connection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();

    for _ in 0..200 {
        if let Ok(connection) = client.get_connection() {
            return connection;
        }
        thread::sleep(Duration::from_millis(25));
    }

    panic!("persistent test server did not start");
}

#[test]
fn data_survives_graceful_server_restart() {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_path = temp_dir.path().join("data.db");
    let config_path = temp_dir.path().join("server.toml");

    let mut config = Config {
        port: available_port(),
        threads: 2,
        data_path: Some(data_path.to_string_lossy().into_owned()),
        file_size: Some(64 * 1024 * 1024),
        ..Default::default()
    };
    config.to_file(&config_path).unwrap();

    let process = start_server(&config_path);
    {
        let mut connection = connect(config.port);
        let _: () = connection.set("persistent:string", "survives").unwrap();
        let added: i64 = redis::cmd("HSET")
            .arg("persistent:hash")
            .arg("field")
            .arg("value")
            .query(&mut connection)
            .unwrap();
        assert_eq!(added, 1);
    }
    stop_server(process);

    config.port = available_port();
    config.to_file(&config_path).unwrap();

    let process = start_server(&config_path);
    {
        let mut connection = connect(config.port);
        let value: String = connection.get("persistent:string").unwrap();
        assert_eq!(value, "survives");

        let value: String = redis::cmd("HGET")
            .arg("persistent:hash")
            .arg("field")
            .query(&mut connection)
            .unwrap();
        assert_eq!(value, "value");

        let length: i64 = redis::cmd("HLEN")
            .arg("persistent:hash")
            .query(&mut connection)
            .unwrap();
        assert_eq!(length, 1);
    }
    stop_server(process);
}

#[test]
fn background_flushed_data_survives_abrupt_downtime() {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_path = temp_dir.path().join("data.db");
    let config_path = temp_dir.path().join("server.toml");

    let mut config = Config {
        port: available_port(),
        threads: 2,
        data_path: Some(data_path.to_string_lossy().into_owned()),
        file_size: Some(64 * 1024 * 1024),
        ..Default::default()
    };
    config.to_file(&config_path).unwrap();

    let process = start_server(&config_path);
    {
        let mut connection = connect(config.port);
        let _: () = connection.set("power-loss:key", "survives").unwrap();
    }

    // FeoxDB uses a 100ms write-behind interval. SIGKILL models physical
    // downtime after the storage engine's documented background flush window.
    thread::sleep(Duration::from_secs(1));
    kill_server(process);

    config.port = available_port();
    config.to_file(&config_path).unwrap();

    let process = start_server(&config_path);
    let mut connection = connect(config.port);
    let value: String = connection.get("power-loss:key").unwrap();
    assert_eq!(value, "survives");
    drop(connection);
    stop_server(process);
}
