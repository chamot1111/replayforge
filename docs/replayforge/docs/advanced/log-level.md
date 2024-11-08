---
sidebar_position: 8
---
# Log level

The log level is a way to control the verbosity of the logs. The log level can be set to one of the following values using var env LOG_LEVEL:

- `trace`: The most verbose log level. It is used for debugging purposes.
- `debug`: It is used to log debug information.
- `info`: The default log level. It is used to log information about the application.
- `warn`: It is used to log warnings.
- `error`: It is used to log errors.

By default, the log level is set to `info`.

:::warning
As replay forge is a tools to manage logs, it will not flood itself with logs. Before logging there is a filter that prevents sending the same message multiple times within a 30 second window.
:::
