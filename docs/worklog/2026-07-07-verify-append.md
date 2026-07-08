# Worklog: burst-agg append 形式 データ検証 (2026-07-07)

## Goal
burst_agg の新しい出力形式（1マーケット1ファイル JSONL 追記）の正確性を確認する。

## 検証観点
1. ファイル構造（summary 22列、features 列が正しく出力されているか）
2. 追記の冪等性（同一範囲再実行で重複しないこと）
3. burst カウントの妥当性
4. burst_print_sizes の符号規則（buy=正, sell=負）
5. book state の正確性
6. 空 window の扱い
