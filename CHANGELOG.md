# Changelog

## 0.0.66
  * Fix sorting for full_table sync by xmin to use integer sorting rather than string sorting [#73](https://github.com/singer-io/tap-postgres/pull/73)

## 0.0.65
  * Add support for `int8[]` (`bigint[]`) array types to log-based replication [#69](https://github.com/singer-io/tap-postgres/pull/69)

## 0.0.64
  * Pass string to `decimal.Decimal` when handling numeric data type [#67](https://github.com/singer-io/tap-postgres/pull/67)
