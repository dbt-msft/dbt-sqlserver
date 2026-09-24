Fix the `sqlserver__openquery` macro to quote linked-server names through `adapter.quote()`, keeping its generated identifier style consistent with the rest of the adapter.
