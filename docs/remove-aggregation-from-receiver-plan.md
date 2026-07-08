# 受信側から集計処理を外す差分メモ

## 目的

受信側を軽くする。

つまり受信側では
- 受け取る
- 保存する

だけにして、

- 1秒集計
- 30秒板まとめ

は後から別の集計側で作る。

---

## いま受信側に入っている集計処理

`orderflow_monitor.mjs` には今、次が入っている。

### 1. 古い集計
- `FeatureComputer`
- `featuresWriter`
- `features.jsonl`

### 2. 新しい集計
- `FeatureAccumulator`
- `feedTrade()`
- `feedDepth()`
- `feedSecond()`
- `1s_features/...`
- `30s_book/...`

---

## 受信側に残すもの

### 残す
- `TradeAggregator`
  - 理由: 受信側で trade を1秒にまとめた生データとして残すため
- `tradeWriters`
  - `trades/<market>.jsonl`
- `rawTradeWriters`
  - `trades/<market>_raw.jsonl`
- `bookUpdateWriters`
  - `book/<market>_update.jsonl`
- `bookWriters`
  - `book/<market>.jsonl` の定期 snapshot
- `liquidationWriters`
- `HealthMonitor`
- `DerivativesHelper`
- `MarketDataCollector`

### 外す
- `FeatureComputer`
- `featuresWriter`
- `FeatureAccumulator`
- `featureAccumulator.feedTrade()`
- `featureAccumulator.feedDepth()`
- `featureAccumulator.feedSecond()`
- `featureAccumulator.close()`

---

## `orderflow_monitor.mjs` で消す場所

### import
消す。
- `FeatureComputer`
- `FeatureAccumulator`

### 初期化
消す。
- `featuresWriter`
- `featureComputer`
- `featureAccumulator`

### trade イベント
今:
```js
aggregator.addTrade(tradeEvent);
featureAccumulator.feedTrade(market, tradeEvent);
rawTradeWriters.get(market)?.write(tradeEvent);
```

変更後:
```js
aggregator.addTrade(tradeEvent);
rawTradeWriters.get(market)?.write(tradeEvent);
```

### depth イベント
今:
```js
featureAccumulator.feedDepth(market, depthEvent, connector.book.getMid());
bookUpdateWriters.get(market)?.write(depthEvent);
```

変更後:
```js
bookUpdateWriters.get(market)?.write(depthEvent);
```

### tick 内の集計
今:
```js
const feature = featureComputer.compute(market, book, aggTrade, now);
featuresWriter.write(feature);
featureAccumulator.feedSecond(market, now, book);
```

変更後:
- この3行を消す
- 受信側では aggregated trade の保存だけにする

### shutdown
今:
- `featuresWriter.close()`
- `featureAccumulator.close()`

変更後:
- この2つを消す

---

## 受信側で最終的に残る出力

### trade
- `trades/<market>.jsonl`
- `trades/<market>_raw.jsonl`

### book
- `book/<market>.jsonl`
- `book/<market>_update.jsonl`

### liquidation
- `liquidations/<market>.jsonl`

### 補助
- `health.jsonl`
- `derivatives/...`
- `market_data/...`

---

## 次に作る集計側

新しく別入口を作る。

候補:
- `scripts/aggregate-live-window.mjs`

役割:
- 30秒ごとに閉じた生データを読む
- 1秒集計を作る
- 30秒板まとめを作る
- 集計済み印を付ける

---

## 受信側を先に軽くする理由

- まず責務を分ける
- その後で後段集計を作る
- 削除は最後に足す

この順の方が事故が少ない。

---

## 変更の順番

### 第1段
- 受信側から集計を外す
- raw と snapshot だけ残す

### 第2段
- 生データを30秒区切りにする

### 第3段
- 集計側を新設する

### 第4段
- 集計済み記録
- 削除処理

---

## 今回の到達点

今回はまだコード変更ではなく、
**どの行を外すかの設計を固定する段階**。

次の実装でやることは明確。
