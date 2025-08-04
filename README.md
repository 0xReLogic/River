# RIVER

*Columnar OLAP database yang bisa injest 1 juta baris/detik dari Kafka, lalu query SELECT count() GROUP BY city dalam 5 ms — satu binary, no JVM, no CGO.*

[![Go Report Card](https://goreportcard.com/badge/github.com/0xReLogic/river)](https://goreportcard.com/report/github.com/0xReLogic/river)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/0xReLogic/river/actions/workflows/ci.yml/badge.svg)](https://github.com/0xReLogic/river/actions/workflows/ci.yml)

## 🧪 Portfolio punchline

“I built a single-binary OLAP engine that injests 1 M rows/sec and serves sub-10 ms aggregations on my laptop — no ClickHouse, no Spark, no JVM.”
