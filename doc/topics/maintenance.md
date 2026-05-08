# Maintenance Mode

Maintenance mode allows you to define a time window during which an exchange session is considered to be in maintenance. When a session is in maintenance mode, BBGO strategies and components can skip certain operations, such as placing orders or quoting, to avoid errors or unintended trades during exchange downtime.

## Configuration

Maintenance mode is configured per exchange session in your `bbgo.yaml` file.

```yaml
sessions:
  binance:
    exchange: binance
    envVarPrefix: BINANCE
    maintenance:
      startTime: "2024-05-08T10:00:00Z"
      endTime: "2024-05-08T12:00:00Z"
```

### Supported Time Formats

The `startTime` and `endTime` fields support flexible time formats and relative time aliases:

*   **RFC3339**: `2024-05-08T10:00:00Z` or `2024-05-08T10:00:00+08:00`
*   **Simple Date**: `2024-05-08` (assumes 00:00:00 UTC)
*   **Date and Time**: `2024-05-08 10:00:00`
*   **Relative Aliases**:
    *   `now`: The current time.
    *   `yesterday`: 24 hours ago from now.
    *   `today`: 00:00:00 of the current day.
    *   `tomorrow`: 00:00:00 of the next day.

## How it Works

When `now` is between `startTime` and `endTime`, the session's `IsInMaintenance()` method returns `true`.

### xmaker Strategy

In the `xmaker` strategy, if the hedge session (defined by `sourceExchange`) is in maintenance mode, the strategy will skip the `updateQuote` process. This means it will not place or update any maker orders on the maker exchange.

A warning message will be logged:
`source session is in maintenance, skipping update quote`

### SplitHedge Component

If you are using the `SplitHedge` component, it will automatically skip any hedge markets whose exchange session is in maintenance mode.

*   **Hedge Execution**: When hedging a position, `SplitHedge` will only distribute the hedge quantity among sessions that are NOT in maintenance.
*   **Quote Price Calculation**: When calculating the weighted quote price for making (e.g., `GetBalanceWeightedQuotePrice`), it will ignore the balances and prices from sessions that are in maintenance.

## Example: Using Relative Aliases for Testing

You can easily test how your strategy behaves during maintenance by using the `now` alias.

```yaml
sessions:
  binance:
    exchange: binance
    maintenance:
      startTime: "yesterday"
      endTime: "tomorrow" # This session will be in maintenance mode
```
