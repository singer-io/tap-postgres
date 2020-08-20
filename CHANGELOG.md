# Changelog

## 0.2.0
  * Add support to discover partitioned tables [101](https://github.com/singer-io/tap-postgres/pull/101)

## 0.1.0
  * Add support for `wal2json` message format v2 via config parameter [91](https://github.com/singer-io/tap-postgres/pull/91)

## 0.0.70
  * Look up ssl status in `pg_stat_ssl` and `pg_stat_activity` tables [#84](https://github.com/singer-io/tap-postgres/pull/84)

## 0.0.69
  * Add `sslmode` log message when opening connection [#82](https://github.com/singer-io/tap-postgres/pull/82)

## 0.0.68
  * Respect `ssl` config property (bug fix) [#80](https://github.com/singer-io/tap-postgres/pull/80)

## 0.0.67
  * Make `bytea[]` fields have `"inclusion" : "unsupported"` metadata [#76](https://github.com/singer-io/tap-postgres/pull/76)

## 0.0.66
  * Fix sorting for full_table sync by xmin to use integer sorting rather than string sorting [#73](https://github.com/singer-io/tap-postgres/pull/73)

## 0.0.65
  * Add support for `int8[]` (`bigint[]`) array types to log-based replication [#69](https://github.com/singer-io/tap-postgres/pull/69)

## 0.0.64
  * Pass string to `decimal.Decimal` when handling numeric data type [#67](https://github.com/singer-io/tap-postgres/pull/67)
