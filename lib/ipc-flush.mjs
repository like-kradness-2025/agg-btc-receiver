/**
 * worker → main の IPC 送信を、観測失敗で壊さない形でまとめる。
 *
 * 事故 (2026-09-23): span の戻り値を関数と誤認して end() を呼び、本番の送信経路で
 * 例外 → 起動失敗ループ。契約は { end() } を返すことなので、ここで吸収する。
 * frame_units は frame_text の UTF-16 コード単位の合計 (ASCII では ほぼバイト数)。
 * 診断用の目安であり、バイト数そのものではない。
 */
export function frameUnits(envelopes) {
  let units = 0;
  for (const envelope of envelopes) {
    const text = envelope?.frame_text;
    if (typeof text === 'string') units += text.length;
  }
  return units;
}

function safeBegin(probe, spanName, envelopes) {
  try {
    let units = null;
    try { units = frameUnits(envelopes); } catch { units = null; }
    const details = units === null ? { envelopes: envelopes.length } : { envelopes: envelopes.length, frame_units: units };
    return probe?.begin?.(spanName, details) ?? null;
  } catch {
    return null; // 観測できないだけで送信は続ける
  }
}

function safeEnd(mark) {
  try { mark?.end?.(); } catch { /* 観測の失敗で本処理を壊さない */ }
}

/** queue を空にして port へ送る。送信件数を返す。送信自体の例外はそのまま伝える。 */
export function flushEnvelopeQueue({ queue, port, messageType, spanName, probe }) {
  if (!Array.isArray(queue) || queue.length === 0) return 0;
  const envelopes = queue.splice(0, queue.length);
  const mark = safeBegin(probe, spanName, envelopes);
  try {
    port.postMessage({ type: messageType, envelopes });
  } finally {
    safeEnd(mark);
  }
  return envelopes.length;
}
