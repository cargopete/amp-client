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

print("");
print("all assertions passed ✓");
