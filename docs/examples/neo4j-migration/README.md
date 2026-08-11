# Neo4j to HelixDB migration example

This is the tested companion project for the Neo4j migration guide. It exports a
small Movie Graph through Neo4j's JavaScript driver, validates a versioned JSON
snapshot, and loads it into HelixDB with replay-safe TypeScript SDK batches.

Requirements: Node.js 20 or newer and Docker with Compose.

```bash
npm install
docker compose up -d
npm run seed
npm run export -- movie-graph.v1.json
npm run load -- movie-graph.v1.json
npm run verify -- movie-graph.v1.json
docker compose down --volumes
```

Pause application writes before `npm run export`; the exporter reads all source
records in one transaction but assumes that the source is quiescent. Configure a
different source or target with the variables in `.env.example`.

Run the automated checks with:

```bash
npm run test:coverage
npm run test:e2e
```
