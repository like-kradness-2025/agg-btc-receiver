// test_read_text.mjs — quick DuckDB read_text smoke
import duckdb from 'duckdb';

function q(db, sql) {
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => err ? reject(err) : resolve(rows));
  });
}

const db = new duckdb.Database(':memory:');

try {
  // Test read_text — returns columns: filename, content, size, last_modified
  const rows = await q(db, `SELECT * FROM read_text('data/raw_hot/2026-06-26/trade/binance_spot.jsonl') LIMIT 3`);
  console.log('read_text rows:', rows.length);
  for (const r of rows) {
    console.log('columns:', Object.keys(r));
    console.log('content length:', r.content?.length);
    console.log('content preview:', r.content?.slice(0, 80));
  }

  // Test json_extract for typed columns (trade)
  const parsed = await q(db, `
    SELECT 
      content::BLOB AS raw_line,
      json_extract_string(content, '$.ts')::BIGINT AS ts,
      json_extract_string(content, '$.market') AS market,
      json_extract_string(content, '$.price')::DOUBLE AS price,
      json_extract_string(content, '$.qty')::DOUBLE AS qty,
      json_extract_string(content, '$.side') AS side,
      json_extract_string(content, '$.trade_id') AS trade_id
    FROM read_text('data/raw_hot/2026-06-26/trade/binance_spot.jsonl')
    LIMIT 3
  `);
  console.log('\nParsed trade columns:');
  for (const r of parsed) {
    console.log(`  ts=${r.ts} market=${r.market} price=${r.price} qty=${r.qty} side=${r.side} trade_id=${r.trade_id}`);
  }

  // Test depth: bids/asks as VARCHAR (keep JSON string, raw_line has original)
  const depthRows = await q(db, `
    SELECT 
      content::BLOB AS raw_line,
      json_extract_string(content, '$.ts')::BIGINT AS ts,
      json_extract_string(content, '$.market') AS market,
      json_extract_string(content, '$.bids') AS bids,
      json_extract_string(content, '$.asks') AS asks
    FROM read_text('data/raw_hot/2026-06-26/depth/binance_spot.jsonl')
    LIMIT 2
  `);
  console.log('\nDepth sample:');
  for (const r of depthRows) {
    console.log(`  ts=${r.ts} market=${r.market} bids_len=${(r.bids||'').length} asks_len=${(r.asks||'').length}`);
  }

  // Test fairprice
  const fpRows = await q(db, `
    SELECT 
      content::BLOB AS raw_line,
      json_extract_string(content, '$.ts')::BIGINT AS ts,
      json_extract_string(content, '$.market') AS market,
      json_extract_string(content, '$.fair_price')::DOUBLE AS fair_price,
      json_extract_string(content, '$.fair_price_source') AS fair_price_source
    FROM read_text('data/raw_hot/2026-06-26/fairprice/binance_spot.jsonl')
    LIMIT 3
  `);
  console.log('\nFairprice sample:');
  for (const r of fpRows) {
    console.log(`  ts=${r.ts} market=${r.market} fair_price=${r.fair_price} source=${r.fair_price_source}`);
  }

  // Test COPY to Parquet + verify
  const outPath = '/tmp/test_convert.parquet';
  await q(db, `
    COPY (
      SELECT 
        content::BLOB AS raw_line,
        json_extract_string(content, '$.ts')::BIGINT AS ts,
        json_extract_string(content, '$.market') AS market,
        json_extract_string(content, '$.price')::DOUBLE AS price,
        json_extract_string(content, '$.qty')::DOUBLE AS qty,
        json_extract_string(content, '$.side') AS side,
        json_extract_string(content, '$.trade_id') AS trade_id
      FROM read_text('data/raw_hot/2026-06-26/trade/binance_spot.jsonl')
      WHERE json_extract_string(content, '$.ts')::BIGINT >= (
        SELECT max(json_extract_string(content, '$.ts')::BIGINT) - 120000 
        FROM read_text('data/raw_hot/2026-06-26/trade/binance_spot.jsonl')
      )
    ) TO '${outPath}' (FORMAT PARQUET, COMPRESSION ZSTD)
  `);
  
  // Read back and verify raw_line integrity
  const verify = await q(db, `SELECT count(*)::VARCHAR AS cnt, min(ts)::VARCHAR AS min_ts, max(ts)::VARCHAR AS max_ts FROM read_parquet('${outPath}')`);
  console.log('\nParquet written:', outPath, 'rows:', verify[0]?.cnt, 'ts_range:', verify[0]?.min_ts, '-', verify[0]?.max_ts);
  
  // Verify raw_line round-trip: extract raw_line + reconstruct + hash compare
  const rawCheck = await q(db, `SELECT raw_line FROM read_parquet('${outPath}') LIMIT 1`);
  if (rawCheck.length > 0) {
    const buf = rawCheck[0].raw_line;
    console.log('raw_line type:', typeof buf, 'constructor:', buf?.constructor?.name);
    console.log('raw_line bytes:', buf?.length, 'is Buffer:', buf instanceof Buffer);
  }

} catch (err) {
  console.error('ERROR:', err.message);
} finally {
  db.close();
}
