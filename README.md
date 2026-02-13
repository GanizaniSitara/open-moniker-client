# Moniker Client

Python client for the Moniker Service.

## Install

```bash
pip install moniker-client
pip install moniker-client[oracle]  # with Oracle support
```

## Usage

```python
from moniker_client import read, describe

data = read("prices.equity/AAPL")           # today's data
data = read("prices.equity/AAPL@20260115")  # point-in-time
data = read("prices.equity/ALL@latest")     # all symbols

info = describe("prices.equity")            # metadata & ownership
```

## Configuration

Config file (`~/.moniker/client.yaml` or `.moniker.yaml`):

```yaml
service_url: http://localhost:8050
app_id: my-app
team: my-team
```

Or environment variables:

```bash
export MONIKER_SERVICE_URL=http://localhost:8050
export MONIKER_APP_ID=my-app
```

Or in code:

```python
from moniker_client import MonikerClient, ClientConfig

# Auto-load from config files
config = ClientConfig.load()

# Or explicit
config = ClientConfig(service_url="http://localhost:8050")

client = MonikerClient(config=config)
```

## API

| Function | Description |
|----------|-------------|
| `read(moniker)` | Fetch data |
| `describe(moniker)` | Get metadata |
| `list_children(moniker)` | List child paths |
| `lineage(moniker)` | Get ownership chain |

## Adapters

Built-in: `rest`, `static`
Optional: `snowflake`, `oracle`, `excel`, `bloomberg`, `refinitiv`
