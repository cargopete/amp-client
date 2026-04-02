// examples/test.rhai
// Run with: cargo run --example rhai_runner examples/test.rhai

print("── scalar queries ──────────────────────────────");

let rows = query("SELECT current_date AS today, 42 AS answer");
assert(rows.len() == 1, "expected 1 row");
print(`today=${rows[0].today}  answer=${rows[0].answer}`);
assert(rows[0].answer == 42, "answer should be 42");

print("── arithmetic ──────────────────────────────────");

let math = query("SELECT 1 + 1 AS two, 6 * 7 AS forty_two, 2.5 * 4.0 AS ten");
assert(math[0].two == 2,        "1+1 should be 2");
assert(math[0].forty_two == 42, "6*7 should be 42");
assert(math[0].ten == 10.0,     "2.5*4.0 should be 10");
print(`two=${math[0].two}  forty_two=${math[0].forty_two}  ten=${math[0].ten}`);

print("── string functions ────────────────────────────");

let str_rows = query("SELECT upper('hello') AS up, lower('WORLD') AS down");
assert(str_rows[0].up == "HELLO", "upper failed");
assert(str_rows[0].down == "world", "lower failed");
print(`up=${str_rows[0].up}  down=${str_rows[0].down}`);

print("── multi-row result ────────────────────────────");

let multi = query("SELECT v FROM (VALUES (1),(2),(3)) t(v)");
assert(multi.len() == 3, `expected 3 rows, got ${multi.len()}`);
print(`row count: ${multi.len()}`);

print("── streaming API ───────────────────────────────");

let streamed = query_stream("SELECT v FROM (VALUES (10),(20),(30)) t(v)");
assert(streamed.len() == 3, `expected 3 streamed rows, got ${streamed.len()}`);
assert(streamed[0].v == 10, "first streamed value should be 10");
assert(streamed[2].v == 30, "last streamed value should be 30");
print(`streamed ${streamed.len()} rows via query_stream`);

print("── list_datasets ───────────────────────────────");

let datasets = list_datasets();
print(`found ${datasets.len()} dataset(s): ${datasets}`);
assert(type_of(datasets) == "array", "list_datasets should return an array");

print("── describe ────────────────────────────────────");

// Use a subquery so this works even with no deployed datasets
let schema = describe("(SELECT 1 AS id, 'hello' AS label, 3.14 AS score)");
print(`describe subquery: ${schema.len()} field(s): ${schema}`);
assert(schema.len() == 3, `expected 3 fields, got ${schema.len()}`);
assert("id"    in schema, "expected field: id");
assert("label" in schema, "expected field: label");
assert("score" in schema, "expected field: score");

print("");
print("all assertions passed ✓");
