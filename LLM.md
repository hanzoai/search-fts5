# Hanzo Search FTS5

A **Meilisearch-API-compatible HTTP shim backed by SQLite FTS5**. It is the
native, Base/SQLite replacement for the bundled Meilisearch containers that
Hanzo Chat (LibreChat fork) used for conversation/message full-text search.

**Repo**: `github.com/hanzoai/search-fts5`
**Image**: `ghcr.io/hanzoai/search-fts5`
**Language**: Go (pure-Go `modernc.org/sqlite`, FTS5 compiled in, `CGO_ENABLED=0`)
**Listens**: `:7700` (Meili's port — drop-in for `MEILI_HOST`)

## Why this exists

LibreChat hardwires the Meilisearch REST API for conversation search via the
`meilisearch@0.38` JS client and the `mongoMeili` Mongoose plugin
(`packages/data-schemas/src/models/plugins/mongoMeili.ts`). Switching full-text
to SQLite-native is therefore NOT a config flip — the engine must keep speaking
Meili's API. This service implements exactly the API subset that chat drives,
storing documents in SQLite with an FTS5 virtual table for the searchable text.
No Meilisearch process, no Rust engine.

This is the "ONE way" full-text engine for the Hanzo native stack: chat (and any
other Meili consumer) keeps the same API; the engine is SQLite FTS5.

## Implemented Meili API surface (the only endpoints chat calls)

| Method | Path | Used by |
|--------|------|---------|
| GET  | `/health` | search route `/enable` (`client.health()`) |
| GET  | `/version` | client handshake |
| POST | `/indexes` `{uid,primaryKey}` | `client.createIndex` |
| GET  | `/indexes/{uid}` | `index.getRawInfo` (404 `index_not_found` before create) |
| GET/PATCH | `/indexes/{uid}/settings` | `index.updateSettings({filterableAttributes})` |
| POST | `/indexes/{uid}/documents` | `addDocuments` / `addDocumentsInBatches` |
| PUT  | `/indexes/{uid}/documents` | `updateDocuments` |
| GET  | `/indexes/{uid}/documents?limit&offset` | `getDocuments` (cleanup) |
| GET/DELETE | `/indexes/{uid}/documents/{id}` | `getDocument` / `deleteDocument` |
| POST | `/indexes/{uid}/documents/delete-batch` | `deleteDocuments` |
| POST | `/indexes/{uid}/search` `{q,filter,limit,offset}` | `meiliSearch` (getConvosByCursor) |
| GET  | `/tasks/{uid}` | `waitForTask` (always `succeeded`) |

Writes are applied synchronously and return a well-formed `EnqueuedTask`;
`mongoMeili` never awaits tasks, and `GET /tasks/{uid}` reports `succeeded` so
any `waitForTask` caller resolves immediately.

## Search semantics

- Indexes: `convos` (pk `conversationId`), `messages` (pk `messageId`).
- Each index = a row store (`pk`, `user`, full doc JSON) + an FTS5 table over the
  searchable text (`title` for convos, `text` for messages).
- `POST /search` parses the Meili filter `user = "<id>"` (also `user IN [...]`)
  into a SQL `WHERE user IN (...)`, and the query `q` into a prefix FTS5 MATCH
  (`"term"*` OR-joined, metachars stripped). Per-user tenant isolation is
  enforced in SQL.

## Config (env)

| Var | Default | Meaning |
|-----|---------|---------|
| `FTS5_DB_PATH` | `/data/search.db` | SQLite file (PVC-mounted in k8s) |
| `FTS5_ADDR` | `:7700` | listen address |
| `MEILI_MASTER_KEY` | (unset) | if set, require `Authorization: Bearer <key>` or `X-Meili-Api-Key` |

## Build & test

```bash
go test ./...        # unit tests (SQLite temp file, FTS5)
go build ./cmd/search-fts5
```

Compatibility is also proven against the **real `meilisearch@0.38` JS client**
exercising the full `mongoMeili` lifecycle (see commit notes / migration log in
`hanzoai/chat` LLM.md).

## Deploy (operator CR, no GHA)

- Image built on-cluster via arcd (`ghcr.io/hanzoai/search-fts5:<semver>`).
- Operator CR `hanzoai/universe/infra/k8s/operator/crs/search-fts5.yaml`
  (kind `hanzo.ai/v1 Service`), service name `search-fts5`, port 7700, with a
  small PVC at `/data`.
- Chat points `MEILI_HOST=http://search-fts5.hanzo.svc.cluster.local:7700`.

## Persistence

SQLite WAL on a PVC. Per the operator persistence model this can later be
fronted by the `replicate` (Litestream fork) sidecar → SeaweedFS for
hot-SQLite durability; not required for first cutover.
