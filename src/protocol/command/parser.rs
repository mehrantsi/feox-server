use super::Command;
use crate::protocol::resp::RespValue;
use bytes::Bytes;

/// Parse command from RESP array
#[inline(always)]
pub fn parse_command(value: RespValue) -> Result<Command, String> {
    match value {
        RespValue::Array(Some(mut args)) if !args.is_empty() => {
            // Get command name
            let cmd_name = match args.remove(0) {
                RespValue::BulkString(Some(s)) => s,
                _ => return Err("Invalid command format".to_string()),
            };

            // Convert to uppercase for case-insensitive matching
            let cmd_upper = cmd_name.to_ascii_uppercase();

            match &cmd_upper[..] {
                b"GET" => {
                    if args.len() != 1 {
                        return Err("wrong number of arguments for 'GET' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?;
                    Ok(Command::Get(key.to_vec()))
                }

                b"SET" => {
                    if args.len() < 2 {
                        return Err("wrong number of arguments for 'SET' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let value = extract_bytes(&args[1])?;

                    // Parse optional arguments (EX, PX, etc.)
                    let mut ex = None;
                    let mut px = None;
                    let mut i = 2;

                    while i < args.len() {
                        let opt = extract_bytes(&args[i])?;
                        let opt_upper = opt.to_ascii_uppercase();

                        match &opt_upper[..] {
                            b"EX" if i + 1 < args.len() => {
                                ex = Some(extract_integer(&args[i + 1])? as u64);
                                i += 2;
                            }
                            b"PX" if i + 1 < args.len() => {
                                px = Some(extract_integer(&args[i + 1])? as u64);
                                i += 2;
                            }
                            _ => i += 1,
                        }
                    }

                    Ok(Command::Set { key, value, ex, px })
                }

                b"DEL" => {
                    if args.is_empty() {
                        return Err("wrong number of arguments for 'DEL' command".to_string());
                    }
                    let keys = args
                        .into_iter()
                        .map(|arg| extract_bytes(&arg).map(|b| b.to_vec()))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Command::Del(keys))
                }

                b"EXISTS" => {
                    if args.is_empty() {
                        return Err("wrong number of arguments for 'EXISTS' command".to_string());
                    }
                    let keys = args
                        .into_iter()
                        .map(|arg| extract_bytes(&arg).map(|b| b.to_vec()))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Command::Exists(keys))
                }

                b"INCR" => {
                    if args.len() != 1 {
                        return Err("wrong number of arguments for 'INCR' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    Ok(Command::Incr(key))
                }

                b"INCRBY" => {
                    if args.len() != 2 {
                        return Err("wrong number of arguments for 'INCRBY' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let delta = extract_integer(&args[1])?;
                    Ok(Command::IncrBy { key, delta })
                }

                b"DECR" => {
                    if args.len() != 1 {
                        return Err("wrong number of arguments for 'DECR' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    Ok(Command::Decr(key))
                }

                b"DECRBY" => {
                    if args.len() != 2 {
                        return Err("wrong number of arguments for 'DECRBY' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let delta = extract_integer(&args[1])?;
                    Ok(Command::DecrBy { key, delta })
                }

                b"EXPIRE" => {
                    if args.len() != 2 {
                        return Err("wrong number of arguments for 'EXPIRE' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let seconds = extract_integer(&args[1])? as u64;
                    Ok(Command::Expire { key, seconds })
                }

                b"PEXPIRE" => {
                    if args.len() != 2 {
                        return Err("wrong number of arguments for 'PEXPIRE' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let milliseconds = extract_integer(&args[1])? as u64;
                    Ok(Command::PExpire { key, milliseconds })
                }

                b"TTL" => {
                    if args.len() != 1 {
                        return Err("wrong number of arguments for 'TTL' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    Ok(Command::Ttl(key))
                }

                b"PTTL" => {
                    if args.len() != 1 {
                        return Err("wrong number of arguments for 'PTTL' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    Ok(Command::PTtl(key))
                }

                b"PERSIST" => {
                    if args.len() != 1 {
                        return Err("wrong number of arguments for 'PERSIST' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    Ok(Command::Persist(key))
                }

                b"MGET" => {
                    if args.is_empty() {
                        return Err("wrong number of arguments for 'MGET' command".to_string());
                    }
                    let keys = args
                        .into_iter()
                        .map(|arg| extract_bytes(&arg).map(|b| b.to_vec()))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Command::MGet(keys))
                }

                b"MSET" => {
                    if args.is_empty() || args.len() % 2 != 0 {
                        return Err("wrong number of arguments for 'MSET' command".to_string());
                    }
                    let mut pairs = Vec::with_capacity(args.len() / 2);
                    let mut i = 0;
                    while i < args.len() {
                        let key = extract_bytes(&args[i])?.to_vec();
                        let value = extract_bytes(&args[i + 1])?;
                        pairs.push((key, value));
                        i += 2;
                    }
                    Ok(Command::MSet(pairs))
                }

                b"PING" => {
                    let msg = if args.is_empty() {
                        None
                    } else {
                        Some(extract_bytes(&args[0])?)
                    };
                    Ok(Command::Ping(msg))
                }

                b"ECHO" => {
                    if args.len() != 1 {
                        return Err("wrong number of arguments for 'ECHO' command".to_string());
                    }
                    let msg = extract_bytes(&args[0])?;
                    Ok(Command::Echo(msg))
                }

                b"INFO" => {
                    let section = if args.is_empty() {
                        None
                    } else {
                        Some(String::from_utf8_lossy(&extract_bytes(&args[0])?).to_string())
                    };
                    Ok(Command::Info(section))
                }

                b"CONFIG" => {
                    if args.is_empty() {
                        return Err("wrong number of arguments for 'CONFIG' command".to_string());
                    }
                    let action = String::from_utf8_lossy(&extract_bytes(&args[0])?).to_string();
                    let config_args = args
                        .into_iter()
                        .skip(1)
                        .map(|arg| extract_bytes(&arg))
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Command::Config {
                        action,
                        args: config_args,
                    })
                }

                b"COMMAND" => Ok(Command::Command),
                b"QUIT" => Ok(Command::Quit),
                b"FLUSHDB" => Ok(Command::FlushDb),

                b"KEYS" => {
                    if args.len() != 1 {
                        return Err("wrong number of arguments for 'KEYS' command".to_string());
                    }
                    let pattern = String::from_utf8_lossy(&extract_bytes(&args[0])?).to_string();
                    Ok(Command::Keys(pattern))
                }

                b"SCAN" => {
                    // SCAN cursor [MATCH pattern] [COUNT count]
                    if args.is_empty() {
                        return Err("wrong number of arguments for 'SCAN' command".to_string());
                    }

                    let cursor = extract_bytes(&args[0])?.to_vec();
                    let mut count = 10; // Default count
                    let mut pattern = None;

                    let mut i = 1;
                    while i < args.len() {
                        let opt = extract_bytes(&args[i])?;
                        let opt_upper = opt.to_ascii_uppercase();

                        match &opt_upper[..] {
                            b"MATCH" if i + 1 < args.len() => {
                                pattern = Some(
                                    String::from_utf8_lossy(&extract_bytes(&args[i + 1])?)
                                        .to_string(),
                                );
                                i += 2;
                            }
                            b"COUNT" if i + 1 < args.len() => {
                                count = extract_integer(&args[i + 1])? as usize;
                                i += 2;
                            }
                            _ => {
                                return Err("syntax error in SCAN".to_string());
                            }
                        }
                    }

                    Ok(Command::Scan {
                        cursor,
                        count,
                        pattern,
                    })
                }

                b"JSONPATCH" => {
                    if args.len() != 2 {
                        return Err("wrong number of arguments for 'JSONPATCH' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let patch = extract_bytes(&args[1])?;
                    Ok(Command::JsonPatch { key, patch })
                }

                b"CAS" => {
                    if args.len() != 3 {
                        return Err("wrong number of arguments for 'CAS' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let expected = extract_bytes(&args[1])?;
                    let new_value = extract_bytes(&args[2])?;
                    Ok(Command::Cas {
                        key,
                        expected,
                        new_value,
                    })
                }

                b"LPUSH" => {
                    if args.len() < 2 {
                        return Err("wrong number of arguments for 'LPUSH' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let values = args[1..]
                        .iter()
                        .map(extract_bytes)
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Command::LPush { key, values })
                }

                b"RPUSH" => {
                    if args.len() < 2 {
                        return Err("wrong number of arguments for 'RPUSH' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let values = args[1..]
                        .iter()
                        .map(extract_bytes)
                        .collect::<Result<Vec<_>, _>>()?;
                    Ok(Command::RPush { key, values })
                }

                b"LPOP" => {
                    if args.is_empty() || args.len() > 2 {
                        return Err("wrong number of arguments for 'LPOP' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let count = if args.len() == 2 {
                        Some(extract_integer(&args[1])? as usize)
                    } else {
                        None
                    };
                    Ok(Command::LPop { key, count })
                }

                b"RPOP" => {
                    if args.is_empty() || args.len() > 2 {
                        return Err("wrong number of arguments for 'RPOP' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let count = if args.len() == 2 {
                        Some(extract_integer(&args[1])? as usize)
                    } else {
                        None
                    };
                    Ok(Command::RPop { key, count })
                }

                b"LLEN" => {
                    if args.len() != 1 {
                        return Err("wrong number of arguments for 'LLEN' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    Ok(Command::LLen(key))
                }

                b"LRANGE" => {
                    if args.len() != 3 {
                        return Err("wrong number of arguments for 'LRANGE' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let start = extract_integer(&args[1])?;
                    let stop = extract_integer(&args[2])?;
                    Ok(Command::LRange { key, start, stop })
                }

                b"LINDEX" => {
                    if args.len() != 2 {
                        return Err("wrong number of arguments for 'LINDEX' command".to_string());
                    }
                    let key = extract_bytes(&args[0])?.to_vec();
                    let index = extract_integer(&args[1])?;
                    Ok(Command::LIndex { key, index })
                }

                b"AUTH" => {
                    if args.len() != 1 {
                        return Err("wrong number of arguments for 'AUTH' command".to_string());
                    }
                    let password = extract_bytes(&args[0])?.to_vec();
                    Ok(Command::Auth(password))
                }

                _ => Err(format!(
                    "unknown command '{}'",
                    String::from_utf8_lossy(&cmd_name)
                )),
            }
        }
        _ => Err("Commands must be arrays".to_string()),
    }
}

/// Extract Bytes from RESP value
#[inline]
fn extract_bytes(value: &RespValue) -> Result<Bytes, String> {
    match value {
        RespValue::BulkString(Some(s)) => Ok(s.clone()),
        RespValue::SimpleString(s) => Ok(s.clone()),
        _ => Err("Expected string value".to_string()),
    }
}

/// Extract integer from RESP value
#[inline]
fn extract_integer(value: &RespValue) -> Result<i64, String> {
    match value {
        RespValue::Integer(n) => Ok(*n),
        RespValue::BulkString(Some(s)) => std::str::from_utf8(s)
            .map_err(|_| "Invalid UTF-8".to_string())?
            .parse()
            .map_err(|_| "Invalid integer".to_string()),
        _ => Err("Expected integer value".to_string()),
    }
}
