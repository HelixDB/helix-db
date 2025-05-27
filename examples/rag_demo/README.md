# Setup

## HelixCLI installation

```bash
curl -sSL https://install.helix-db.com | bash
```

## Install Helix

```bash
helix install
```

# Run Queries

Take a look at the queries in `./helixdb-cfg` to see what is being used.

> cd into `helix-demo` if you haven't already

```bash
helix deploy --local
```

## Now you're ready to use the notebook!

If you are using VSCode, Cursor etc, you can just install the `jupyter` extension and open the notebook.

If you are using a terminal, you need to install and run `jupyter` to start the notebook server.

- To setup and install `jupyter`:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install notebook
    ```
- To run the notebook:
    ```bash
    jupyter notebook
    ```
