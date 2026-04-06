# @helix-db/migrate

White-glove migration CLI for moving Supabase data to HelixDB.

## Quick start

```bash
npx @helix-db/migrate supabase \
  --connection-string "<SUPABASE_DB_URL>" \
  --schemas "public" \
  --helix-url "http://localhost:6969" \
  --reset-instance --yes --non-interactive
```

See `BLANK_SLATE_SUPABASE_TO_LOCAL_HELIX_GUIDE.md` for full setup details.

If you already have an existing Supabase database and existing Helix instance, use `EXISTING_SUPABASE_EXISTING_HELIX_GUIDE.md`.
