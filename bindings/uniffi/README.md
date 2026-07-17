# helix-db-embedded

Native embedded runtime for the `helix-db` Python SDK.

Install both matching packages:

```sh
python -m pip install "helix-db==0.2.0b1" "helix-db-embedded==0.2.0b1"
```

Applications continue to use the public SDK:

```python
from helixdb import Client, Disk

client = Client.embedded(Disk("./data", "app"))
```

The SDK imports the native `helixdb_uniffi` module internally. Wheels are
published for macOS universal2 and manylinux x86_64/aarch64.
