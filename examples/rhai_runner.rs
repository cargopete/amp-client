//! Run a Rhai script against a local ampd Flight SQL server.
//!
//! Usage:
//!   cargo run --example rhai_runner                        # built-in demo
//!   cargo run --example rhai_runner examples/test.rhai     # custom script
//!
//! Start ampd first:
//!   ampd solo --flight-server

use std::{
    env,
    sync::{Arc, Mutex},
};

use amp_client::{Client, RecordBatch};
use futures::StreamExt;
use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use rhai::{Dynamic, Engine};

// ── Default script (runs when no file is given) ──────────────────────────────

const DEFAULT_SCRIPT: &str = r#"
print("=== amp-client rhai smoke test ===");

// Scalar query — works without any dataset deployed
let rows = query("SELECT current_date AS today, 42 AS answer");
print(`rows: ${rows.len()}`);
for row in rows {
    print(row);
}

// Arithmetic sanity check
let math = query("SELECT 1 + 1 AS two, 6 * 7 AS forty_two");
print(`math check: ${math[0]}`);

print("=== all done ===");
"#;

// ── Arrow → Rhai conversion ───────────────────────────────────────────────────

fn batches_to_rhai(batches: &[RecordBatch]) -> Dynamic {
    let rows: Vec<Dynamic> = batches.iter().flat_map(batch_rows).collect();
    Dynamic::from(rows)
}

fn batch_rows(batch: &RecordBatch) -> Vec<Dynamic> {
    let schema = batch.schema();
    (0..batch.num_rows())
        .map(|row_idx| {
            let mut map = rhai::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = batch.column(col_idx);
                let val = if col.is_null(row_idx) {
                    Dynamic::UNIT
                } else {
                    col_value(col.as_ref(), row_idx)
                };
                map.insert(field.name().as_str().into(), val);
            }
            Dynamic::from(map)
        })
        .collect()
}

fn col_value(col: &dyn Array, row: usize) -> Dynamic {
    macro_rules! cast {
        ($arr_ty:ty => $out:ty) => {
            if let Some(a) = col.as_any().downcast_ref::<$arr_ty>() {
                return Dynamic::from(a.value(row) as $out);
            }
        };
    }

    cast!(Int8Array   => i64);
    cast!(Int16Array  => i64);
    cast!(Int32Array  => i64);
    cast!(Int64Array  => i64);
    cast!(UInt8Array  => i64);
    cast!(UInt16Array => i64);
    cast!(UInt32Array => i64);
    cast!(UInt64Array => i64);
    cast!(Float32Array => f64);
    cast!(Float64Array => f64);

    if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
        return Dynamic::from(a.value(row));
    }
    if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        return Dynamic::from(a.value(row).to_string());
    }
    if let Some(a) = col.as_any().downcast_ref::<Date32Array>() {
        // days since Unix epoch -> YYYY-MM-DD
        let days = a.value(row) as i64;
        let date = chrono::DateTime::UNIX_EPOCH + chrono::Duration::days(days);
        return Dynamic::from(date.format("%Y-%m-%d").to_string());
    }
    if let Some(a) = col.as_any().downcast_ref::<Date64Array>() {
        // milliseconds since Unix epoch -> YYYY-MM-DD
        let ms = a.value(row);
        let date = chrono::DateTime::UNIX_EPOCH + chrono::Duration::milliseconds(ms);
        return Dynamic::from(date.format("%Y-%m-%d").to_string());
    }

    // fallback: data-type name
    Dynamic::from(format!("<{}>", col.data_type()))
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    println!("Connecting to grpc://localhost:1602 …");
    let client = rt
        .block_on(Client::connect("grpc://localhost:1602"))
        .expect("could not connect — is ampd running? (ampd solo --flight-server)");
    let client = Arc::new(Mutex::new(client));

    let mut engine = Engine::new();

    engine.register_fn("assert", |cond: bool, msg: String| {
        if !cond {
            panic!("assertion failed: {msg}");
        }
    });

    // query_stream(sql: String) -> Array of row maps (uses streaming API under the hood)
    let client_clone = Arc::clone(&client);
    let rt_handle = rt.handle().clone();
    engine.register_fn("query_stream", move |sql: String| -> Vec<Dynamic> {
        let mut c = client_clone.lock().unwrap();
        let stream = std::pin::pin!(c.query_stream(&sql));
        let batches = rt_handle
            .block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("query_stream failed");
        match batches_to_rhai(&batches) {
            d if d.is::<Vec<Dynamic>>() => d.cast::<Vec<Dynamic>>(),
            _ => vec![],
        }
    });

    // list_datasets() -> Array of dataset name strings
    let client_clone = Arc::clone(&client);
    let rt_handle = rt.handle().clone();
    engine.register_fn("list_datasets", move || -> Vec<Dynamic> {
        let mut c = client_clone.lock().unwrap();
        match rt_handle.block_on(c.list_datasets()) {
            Ok(names) => names.into_iter().map(Dynamic::from).collect(),
            Err(e) => {
                eprintln!("list_datasets: {e} (no datasets deployed?)");
                vec![]
            }
        }
    });

    // describe(dataset: String) -> Map of field_name -> data_type string
    let client_clone = Arc::clone(&client);
    let rt_handle = rt.handle().clone();
    engine.register_fn("describe", move |dataset: String| -> rhai::Map {
        let mut c = client_clone.lock().unwrap();
        let schema = rt_handle
            .block_on(c.describe(&dataset))
            .expect("describe failed");
        schema
            .fields()
            .iter()
            .map(|f| (f.name().as_str().into(), Dynamic::from(f.data_type().to_string())))
            .collect()
    });

    // query(sql: String) -> Array of row maps
    let client_clone = Arc::clone(&client);
    let rt_handle = rt.handle().clone();
    engine.register_fn("query", move |sql: String| -> Vec<Dynamic> {
        let mut c = client_clone.lock().unwrap();
        let batches = rt_handle.block_on(c.query(&sql)).expect("query failed");
        match batches_to_rhai(&batches) {
            d if d.is::<Vec<Dynamic>>() => d.cast::<Vec<Dynamic>>(),
            _ => vec![],
        }
    });

    let script = match env::args().nth(1) {
        Some(path) => std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read '{path}': {e}")),
        None => DEFAULT_SCRIPT.to_owned(),
    };

    if let Err(e) = engine.run(&script) {
        eprintln!("script error: {e}");
        std::process::exit(1);
    }
}
