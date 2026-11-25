# `changed_since_2025b.json`

Frozen record of where tzdata 2025b (what faucet shipped through chrono-tz
0.10.4) and tzdata 2026f disagree on a zone's UTC offset, 1970–2100 (#1116).
Rolling upgrades and backfill resume use it to reason about state written by
releases on 2025b. It never changes: later tzdata releases are handled by the
`tzdata:` capability, not by this file.

How it was made (2026-11-25): a standalone crate **outside this workspace**, so
the vendored chrono-tz `[patch]` does not apply and the crates.io chrono-tz
0.10.4 (2025b) is compared with jiff 1.0.3 + jiff-tzdb 1.0.0 (2026f):

```toml
[dependencies]
chrono = "0.4"
chrono-tz = "=0.10.4"
jiff = { version = "=1.0.3", features = ["tzdb-bundle-always"] }
jiff-tzdb = "=1.0.0"
[workspace]
```

`src/main.rs` walks every chrono-tz zone on a 15-minute grid from 1970 to 2100,
bisects each edge to the second and prints one `JSON\t"<zone>": [...]` line per
zone that differs (`null` end = still differing in 2100):

```rust
use chrono::{TimeZone, Offset};
fn main(){
 let db=jiff::tz::TimeZoneDatabase::bundled();
 let until=4102444800i64; // 2100
 for tz in chrono_tz::TZ_VARIANTS { let name=tz.name(); let Ok(j)=db.get(name) else {continue};
  let off=|t:i64|->(i32,i32){ (tz.offset_from_utc_datetime(&chrono::DateTime::from_timestamp(t,0).unwrap().naive_utc()).fix().local_minus_utc(), j.to_offset(jiff::Timestamp::from_second(t).unwrap()).seconds()) };
  let mut t=0i64; let mut start:Option<i64>=None; let mut ivs=vec![]; let mut maxd=0;
  while t<until { let (a,b)=off(t); let d=a!=b; if d { maxd=maxd.max((a-b).abs()); }
    match (d,start) { (true,None)=>{ let mut lo=t-900; let mut hi=t; while hi-lo>1 {let m=(lo+hi)/2; let (x,y)=off(m); if x!=y {hi=m} else {lo=m}} start=Some(hi);} 
      (false,Some(s))=>{ let mut lo=t-900; let mut hi=t; while hi-lo>1 {let m=(lo+hi)/2; let (x,y)=off(m); if x!=y {lo=m} else {hi=m}} ivs.push((s,hi)); start=None;} _=>{} }
    t+=900; }
  if let Some(s)=start { ivs.push((s,i64::MAX)); }
  if !ivs.is_empty(){ let js: Vec<String>=ivs.iter().map(|(a,b)| format!("[{},{}]",a, if *b==i64::MAX {"null".to_string()} else {b.to_string()})).collect(); eprintln!("JSON\t\"{name}\": [{}],", js.join(",")); let fmt=|x:i64| if x==i64::MAX {"open".to_string()} else {jiff::Timestamp::from_second(x).unwrap().to_string()};
    let n=ivs.len(); let show: Vec<String> = ivs.iter().take(3).map(|(a,b)|format!("[{},{})",fmt(*a),fmt(*b))).collect();
    let last=ivs.last().unwrap(); println!("{name}\tmaxdelta={}s\tintervals={n}\tfirst={}\tlast=[{},{})", maxd, show.join(" "), fmt(last.0), fmt(last.1)); }
 }
}
```

Running it twice gave byte-identical intervals. Every changed zone and instant
in `news_oracle.rs` (values taken from the IANA NEWS text, not a library) lies
inside the matching interval, and every must-agree row lies outside all of them
(`tzdata::tests`).
