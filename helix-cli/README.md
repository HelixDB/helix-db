# Helix CLI

```
check
    - (<cluster_id|alias>)
    - (--path/--p)
build
    - (<cluster_id|alias>)
    - (--release/--r)
    - (--path/--p)
    - (--out/--o)
    - (--bin)
push
    - (<cluster_id|alias>)
    - (--force/--f)
pull
    - (<cluster_id|alias>)
branch
    - (<instance>)
    - (--output/--o)
    - (--name)
    - (--port)
init
    - (--template/--t): template to use e.g. ts, py, fli.io, docker etc.
    - (--alias/--a): name to call cluster
    - (--cloud/--c): eventually whether cluster is cloud instance or not
prune
    - (--hard/--h)
    - (--soft/--s)
delete
    - (--all)
metrics
    - (off/on)
cloud
    - login
    - logout
    - create-key
    - sync (leave for now; instance metadata syncing)
    - something to sync data like cloning a git repo
```
