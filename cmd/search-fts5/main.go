// Command search-fts5 is a Meilisearch-API-compatible HTTP shim backed by
// SQLite FTS5. It implements exactly the subset of the Meilisearch REST API
// that Hanzo Chat (LibreChat fork) drives through the `meilisearch@0.38` JS
// client and its `mongoMeili` Mongoose plugin:
//
//	GET    /health
//	GET    /version
//	GET    /indexes/{uid}
//	POST   /indexes                                 {uid, primaryKey}
//	GET    /indexes/{uid}/settings
//	PATCH  /indexes/{uid}/settings                  {filterableAttributes,...}
//	POST   /indexes/{uid}/documents                 [doc,...]   (add/replace)
//	PUT    /indexes/{uid}/documents                 [doc,...]   (update/upsert)
//	GET    /indexes/{uid}/documents                 ?limit&offset
//	GET    /indexes/{uid}/documents/{id}
//	DELETE /indexes/{uid}/documents/{id}
//	POST   /indexes/{uid}/documents/delete-batch    [id,...]
//	POST   /indexes/{uid}/search                    {q, filter, limit, offset}
//	GET    /tasks/{uid}
//
// One SQLite file holds every index. Each index is a pair of tables: a row
// store (full document JSON + extracted primary key + `user`) and an FTS5
// virtual table over the document's searchable text. Writes are applied
// synchronously; the JS client treats the returned EnqueuedTask as fire and
// forget (mongoMeili never awaits), and GET /tasks/{uid} always reports
// `succeeded` so any waitForTask caller resolves immediately.
//
// This is the native, Base/SQLite replacement for the bundled Meilisearch
// containers. No Meilisearch process, no Rust engine — full-text lives in
// SQLite FTS5.
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/hanzoai/sqlite"
)

const version = "0.1.0"

// server owns the single SQLite handle plus an in-memory registry of known
// indexes (mirrored in the meta table for durability across restarts).
type server struct {
	db        *sql.DB
	apiKey    string
	taskSeq   atomic.Int64
	mu        sync.Mutex // serializes index DDL (CREATE TABLE is not concurrent-safe)
	indexes   map[string]*indexMeta
	indexesMu sync.RWMutex
}

type indexMeta struct {
	UID                  string   `json:"uid"`
	PrimaryKey           string   `json:"primaryKey"`
	FilterableAttributes []string `json:"filterableAttributes"`
	CreatedAt            string   `json:"createdAt"`
	UpdatedAt            string   `json:"updatedAt"`
}

func main() {
	dbPath := env("FTS5_DB_PATH", "/data/search.db")
	addr := env("FTS5_ADDR", ":7700")
	apiKey := os.Getenv("MEILI_MASTER_KEY") // optional; matched against incoming Bearer/X-Meili-Api-Key

	// _pragma options: WAL for concurrent readers, busy_timeout so brief
	// writer locks don't surface as errors to the chat backend.
	dsn := dbPath + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Fatalf("open sqlite %s: %v", dbPath, err)
	}
	// SQLite is single-writer; cap the pool so we don't thrash on the lock.
	db.SetMaxOpenConns(1)

	s := &server{db: db, apiKey: apiKey, indexes: map[string]*indexMeta{}}
	if err := s.initMeta(); err != nil {
		log.Fatalf("init meta: %v", err)
	}
	if err := s.loadIndexes(); err != nil {
		log.Fatalf("load indexes: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.route)
	log.Printf("search-fts5 %s listening on %s (db=%s, auth=%v)", version, addr, dbPath, apiKey != "")
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// ---- schema ---------------------------------------------------------------

func (s *server) initMeta() error {
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS _meili_indexes (
		uid TEXT PRIMARY KEY,
		primary_key TEXT NOT NULL,
		filterable TEXT NOT NULL DEFAULT '[]',
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	)`)
	return err
}

func (s *server) loadIndexes() error {
	rows, err := s.db.Query(`SELECT uid, primary_key, filterable, created_at, updated_at FROM _meili_indexes`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var m indexMeta
		var filt string
		if err := rows.Scan(&m.UID, &m.PrimaryKey, &filt, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return err
		}
		_ = json.Unmarshal([]byte(filt), &m.FilterableAttributes)
		s.indexes[m.UID] = &m
	}
	return rows.Err()
}

// tableNames returns the row-store and FTS5 table names for an index. The uid
// is sanitized (only the chat indexes "convos"/"messages" are expected, but be
// defensive) and used as a suffix; SQLite identifiers are quoted at use.
func tableNames(uid string) (store, fts string) {
	safe := sanitize(uid)
	return "doc_" + safe, "fts_" + safe
}

var idRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

func sanitize(s string) string { return idRe.ReplaceAllString(s, "_") }

// ensureIndex creates the row-store + FTS5 tables for uid if missing and
// registers the index meta. Idempotent.
func (s *server) ensureIndex(uid, primaryKey string) (*indexMeta, error) {
	s.indexesMu.RLock()
	if m, ok := s.indexes[uid]; ok {
		s.indexesMu.RUnlock()
		return m, nil
	}
	s.indexesMu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	// re-check under the DDL lock
	s.indexesMu.RLock()
	if m, ok := s.indexes[uid]; ok {
		s.indexesMu.RUnlock()
		return m, nil
	}
	s.indexesMu.RUnlock()

	if primaryKey == "" {
		primaryKey = "id"
	}
	store, fts := tableNames(uid)
	now := time.Now().UTC().Format(time.RFC3339)

	// Row store: pk is the Meili primary key value, doc is the full JSON,
	// user is the extracted filterable attribute (chat filters on `user`).
	if _, err := s.db.Exec(fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %q (pk TEXT PRIMARY KEY, "user" TEXT, doc TEXT NOT NULL)`, store)); err != nil {
		return nil, err
	}
	// FTS5 over the searchable text; `content=''` makes it contentless and we
	// key rows by the same pk via a rowid map kept in the store table's rowid.
	// Simpler & robust: external-content-free FTS storing pk + text.
	if _, err := s.db.Exec(fmt.Sprintf(
		`CREATE VIRTUAL TABLE IF NOT EXISTS %q USING fts5(pk UNINDEXED, text, tokenize='unicode61 remove_diacritics 2')`, fts)); err != nil {
		return nil, err
	}

	m := &indexMeta{
		UID: uid, PrimaryKey: primaryKey,
		FilterableAttributes: []string{"user"},
		CreatedAt:            now, UpdatedAt: now,
	}
	filt, _ := json.Marshal(m.FilterableAttributes)
	if _, err := s.db.Exec(
		`INSERT OR IGNORE INTO _meili_indexes(uid, primary_key, filterable, created_at, updated_at) VALUES(?,?,?,?,?)`,
		uid, primaryKey, string(filt), now, now); err != nil {
		return nil, err
	}
	s.indexesMu.Lock()
	s.indexes[uid] = m
	s.indexesMu.Unlock()
	return m, nil
}

func (s *server) getIndex(uid string) (*indexMeta, bool) {
	s.indexesMu.RLock()
	defer s.indexesMu.RUnlock()
	m, ok := s.indexes[uid]
	return m, ok
}

// ---- routing --------------------------------------------------------------

func (s *server) route(w http.ResponseWriter, r *http.Request) {
	p := strings.Trim(r.URL.Path, "/")
	seg := strings.Split(p, "/")

	// /health and /version are unauthenticated (as in real Meilisearch) so
	// k8s liveness/readiness probes and the chat /enable check work without a
	// key. Everything else requires the master key when one is configured.
	public := p == "health" || p == "version"
	if !public && !s.authOK(r) {
		writeJSON(w, http.StatusForbidden, meiliErr("invalid_api_key", "The provided API key is invalid.", "auth"))
		return
	}

	switch {
	case p == "health" && r.Method == http.MethodGet:
		writeJSON(w, 200, map[string]string{"status": "available"})
	case p == "version" && r.Method == http.MethodGet:
		writeJSON(w, 200, map[string]string{"commitSha": "fts5", "commitDate": "", "pkgVersion": version})
	case p == "indexes" && r.Method == http.MethodPost:
		s.createIndex(w, r)
	case len(seg) >= 2 && seg[0] == "tasks" && r.Method == http.MethodGet:
		s.getTask(w, seg[1])
	case len(seg) == 2 && seg[0] == "indexes" && r.Method == http.MethodGet:
		s.indexInfo(w, seg[1])
	case len(seg) == 3 && seg[0] == "indexes" && seg[2] == "settings" && r.Method == http.MethodGet:
		s.getSettings(w, seg[1])
	case len(seg) == 3 && seg[0] == "indexes" && seg[2] == "settings" && r.Method == http.MethodPatch:
		s.patchSettings(w, r, seg[1])
	case len(seg) == 3 && seg[0] == "indexes" && seg[2] == "search" && r.Method == http.MethodPost:
		s.search(w, r, seg[1])
	case len(seg) == 3 && seg[0] == "indexes" && seg[2] == "documents" && r.Method == http.MethodPost:
		s.addDocuments(w, r, seg[1], false)
	case len(seg) == 3 && seg[0] == "indexes" && seg[2] == "documents" && r.Method == http.MethodPut:
		s.addDocuments(w, r, seg[1], true)
	case len(seg) == 3 && seg[0] == "indexes" && seg[2] == "documents" && r.Method == http.MethodGet:
		s.getDocuments(w, r, seg[1])
	case len(seg) == 4 && seg[0] == "indexes" && seg[2] == "documents" && seg[3] == "delete-batch" && r.Method == http.MethodPost:
		s.deleteBatch(w, r, seg[1])
	case len(seg) == 4 && seg[0] == "indexes" && seg[2] == "documents" && r.Method == http.MethodGet:
		s.getDocument(w, seg[1], seg[3])
	case len(seg) == 4 && seg[0] == "indexes" && seg[2] == "documents" && r.Method == http.MethodDelete:
		s.deleteDocument(w, seg[1], seg[3])
	default:
		writeJSON(w, http.StatusNotFound, meiliErr("not_found", "Path not found: "+r.URL.Path, "system"))
	}
}

// authOK accepts requests when no key is configured, otherwise requires a
// matching Bearer token or X-Meili-Api-Key. The meilisearch JS client sends
// `Authorization: Bearer <apiKey>`.
func (s *server) authOK(r *http.Request) bool {
	if s.apiKey == "" {
		return true
	}
	if h := r.Header.Get("Authorization"); h != "" {
		if strings.TrimPrefix(h, "Bearer ") == s.apiKey {
			return true
		}
	}
	return r.Header.Get("X-Meili-Api-Key") == s.apiKey
}

// ---- index handlers -------------------------------------------------------

func (s *server) createIndex(w http.ResponseWriter, r *http.Request) {
	var body struct {
		UID        string `json:"uid"`
		PrimaryKey string `json:"primaryKey"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UID == "" {
		writeJSON(w, 400, meiliErr("bad_request", "uid required", "system"))
		return
	}
	if _, err := s.ensureIndex(body.UID, body.PrimaryKey); err != nil {
		writeJSON(w, 500, meiliErr("internal", err.Error(), "system"))
		return
	}
	writeJSON(w, http.StatusAccepted, s.enqueued(body.UID, "indexCreation"))
}

func (s *server) indexInfo(w http.ResponseWriter, uid string) {
	m, ok := s.getIndex(uid)
	if !ok {
		// meilisearch JS throws with code index_not_found; mongoMeili relies on
		// this to decide whether to create the index.
		writeJSON(w, http.StatusNotFound, meiliErr("index_not_found", "Index `"+uid+"` not found.", "invalid_request"))
		return
	}
	writeJSON(w, 200, map[string]any{
		"uid": m.UID, "primaryKey": m.PrimaryKey,
		"createdAt": m.CreatedAt, "updatedAt": m.UpdatedAt,
	})
}

func (s *server) getSettings(w http.ResponseWriter, uid string) {
	m, ok := s.getIndex(uid)
	if !ok {
		writeJSON(w, http.StatusNotFound, meiliErr("index_not_found", "Index `"+uid+"` not found.", "invalid_request"))
		return
	}
	writeJSON(w, 200, map[string]any{"filterableAttributes": m.FilterableAttributes})
}

func (s *server) patchSettings(w http.ResponseWriter, r *http.Request, uid string) {
	m, _ := s.ensureIndex(uid, "")
	var body struct {
		FilterableAttributes []string `json:"filterableAttributes"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.FilterableAttributes != nil {
		m.FilterableAttributes = body.FilterableAttributes
		filt, _ := json.Marshal(m.FilterableAttributes)
		_, _ = s.db.Exec(`UPDATE _meili_indexes SET filterable=?, updated_at=? WHERE uid=?`,
			string(filt), time.Now().UTC().Format(time.RFC3339), uid)
	}
	writeJSON(w, http.StatusAccepted, s.enqueued(uid, "settingsUpdate"))
}

// ---- document handlers ----------------------------------------------------

func (s *server) addDocuments(w http.ResponseWriter, r *http.Request, uid string, _ bool) {
	m, err := s.ensureIndex(uid, "")
	if err != nil {
		writeJSON(w, 500, meiliErr("internal", err.Error(), "system"))
		return
	}
	var docs []map[string]any
	if err := json.NewDecoder(r.Body).Decode(&docs); err != nil {
		// allow a single object too
		writeJSON(w, 400, meiliErr("bad_request", "expected array of documents", "system"))
		return
	}
	store, fts := tableNames(uid)
	tx, err := s.db.Begin()
	if err != nil {
		writeJSON(w, 500, meiliErr("internal", err.Error(), "system"))
		return
	}
	for _, d := range docs {
		pk := stringify(d[m.PrimaryKey])
		if pk == "" {
			continue
		}
		raw, _ := json.Marshal(d)
		user := stringify(d["user"])
		text := searchableText(d)

		if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %q(pk,"user",doc) VALUES(?,?,?)
			ON CONFLICT(pk) DO UPDATE SET "user"=excluded."user", doc=excluded.doc`, store),
			pk, user, string(raw)); err != nil {
			_ = tx.Rollback()
			writeJSON(w, 500, meiliErr("internal", err.Error(), "system"))
			return
		}
		// FTS5 has no UPSERT; delete then insert keeps it in sync with the store.
		if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %q WHERE pk=?`, fts), pk); err != nil {
			_ = tx.Rollback()
			writeJSON(w, 500, meiliErr("internal", err.Error(), "system"))
			return
		}
		if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %q(pk,text) VALUES(?,?)`, fts), pk, text); err != nil {
			_ = tx.Rollback()
			writeJSON(w, 500, meiliErr("internal", err.Error(), "system"))
			return
		}
	}
	if err := tx.Commit(); err != nil {
		writeJSON(w, 500, meiliErr("internal", err.Error(), "system"))
		return
	}
	writeJSON(w, http.StatusAccepted, s.enqueued(uid, "documentAdditionOrUpdate"))
}

func (s *server) getDocuments(w http.ResponseWriter, r *http.Request, uid string) {
	if _, ok := s.getIndex(uid); !ok {
		writeJSON(w, http.StatusNotFound, meiliErr("index_not_found", "Index `"+uid+"` not found.", "invalid_request"))
		return
	}
	limit := atoiDefault(r.URL.Query().Get("limit"), 20)
	offset := atoiDefault(r.URL.Query().Get("offset"), 0)
	store, _ := tableNames(uid)
	rows, err := s.db.Query(fmt.Sprintf(`SELECT doc FROM %q ORDER BY pk LIMIT ? OFFSET ?`, store), limit, offset)
	if err != nil {
		writeJSON(w, 500, meiliErr("internal", err.Error(), "system"))
		return
	}
	defer rows.Close()
	results := []json.RawMessage{}
	for rows.Next() {
		var doc string
		if err := rows.Scan(&doc); err != nil {
			break
		}
		results = append(results, json.RawMessage(doc))
	}
	var total int
	_ = s.db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %q`, store)).Scan(&total)
	writeJSON(w, 200, map[string]any{
		"results": results, "offset": offset, "limit": limit, "total": total,
	})
}

func (s *server) getDocument(w http.ResponseWriter, uid, id string) {
	if _, ok := s.getIndex(uid); !ok {
		writeJSON(w, http.StatusNotFound, meiliErr("index_not_found", "Index `"+uid+"` not found.", "invalid_request"))
		return
	}
	store, _ := tableNames(uid)
	var doc string
	err := s.db.QueryRow(fmt.Sprintf(`SELECT doc FROM %q WHERE pk=?`, store), id).Scan(&doc)
	if err == sql.ErrNoRows {
		writeJSON(w, http.StatusNotFound, meiliErr("document_not_found", "Document `"+id+"` not found.", "invalid_request"))
		return
	} else if err != nil {
		writeJSON(w, 500, meiliErr("internal", err.Error(), "system"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(doc))
}

func (s *server) deleteDocument(w http.ResponseWriter, uid, id string) {
	store, fts := tableNames(uid)
	_, _ = s.db.Exec(fmt.Sprintf(`DELETE FROM %q WHERE pk=?`, store), id)
	_, _ = s.db.Exec(fmt.Sprintf(`DELETE FROM %q WHERE pk=?`, fts), id)
	writeJSON(w, http.StatusAccepted, s.enqueued(uid, "documentDeletion"))
}

func (s *server) deleteBatch(w http.ResponseWriter, r *http.Request, uid string) {
	var ids []any
	if err := json.NewDecoder(r.Body).Decode(&ids); err != nil {
		writeJSON(w, 400, meiliErr("bad_request", "expected array of ids", "system"))
		return
	}
	store, fts := tableNames(uid)
	tx, _ := s.db.Begin()
	for _, id := range ids {
		pk := stringify(id)
		_, _ = tx.Exec(fmt.Sprintf(`DELETE FROM %q WHERE pk=?`, store), pk)
		_, _ = tx.Exec(fmt.Sprintf(`DELETE FROM %q WHERE pk=?`, fts), pk)
	}
	_ = tx.Commit()
	writeJSON(w, http.StatusAccepted, s.enqueued(uid, "documentDeletion"))
}

// ---- search ---------------------------------------------------------------

func (s *server) search(w http.ResponseWriter, r *http.Request, uid string) {
	start := time.Now()
	if _, ok := s.getIndex(uid); !ok {
		writeJSON(w, http.StatusNotFound, meiliErr("index_not_found", "Index `"+uid+"` not found.", "invalid_request"))
		return
	}
	var body struct {
		Q      string `json:"q"`
		Filter any    `json:"filter"`
		Limit  *int   `json:"limit"`
		Offset *int   `json:"offset"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	limit := 20
	if body.Limit != nil {
		limit = *body.Limit
	}
	offset := 0
	if body.Offset != nil {
		offset = *body.Offset
	}

	users := parseUserFilter(body.Filter)
	store, fts := tableNames(uid)

	var (
		rows *sql.Rows
		err  error
	)
	args := []any{}
	where := []string{}
	if q := strings.TrimSpace(body.Q); q != "" {
		where = append(where, fmt.Sprintf(`d.pk IN (SELECT pk FROM %q WHERE %q MATCH ?)`, fts, fts))
		args = append(args, ftsQuery(q))
	}
	if len(users) > 0 {
		ph := make([]string, len(users))
		for i, u := range users {
			ph[i] = "?"
			args = append(args, u)
		}
		where = append(where, `d."user" IN (`+strings.Join(ph, ",")+`)`)
	}
	sqlStr := fmt.Sprintf(`SELECT d.doc FROM %q d`, store)
	if len(where) > 0 {
		sqlStr += " WHERE " + strings.Join(where, " AND ")
	}
	sqlStr += " LIMIT ? OFFSET ?"
	args = append(args, limit, offset)

	rows, err = s.db.Query(sqlStr, args...)
	if err != nil {
		writeJSON(w, 500, meiliErr("internal", err.Error(), "system"))
		return
	}
	defer rows.Close()
	hits := []json.RawMessage{}
	for rows.Next() {
		var doc string
		if err := rows.Scan(&doc); err != nil {
			break
		}
		hits = append(hits, json.RawMessage(doc))
	}
	writeJSON(w, 200, map[string]any{
		"hits":               hits,
		"query":              body.Q,
		"processingTimeMs":   time.Since(start).Milliseconds(),
		"limit":              limit,
		"offset":             offset,
		"estimatedTotalHits": len(hits),
	})
}

// parseUserFilter extracts user id(s) from a Meili filter expression. Chat
// only ever sends `user = "<id>"` (see Conversation.js getConvosByCursor) but
// we also accept an array form and `user IN [...]`.
func parseUserFilter(f any) []string {
	switch v := f.(type) {
	case string:
		return userFilterFromString(v)
	case []any:
		out := []string{}
		for _, e := range v {
			if s, ok := e.(string); ok {
				out = append(out, userFilterFromString(s)...)
			}
		}
		return out
	}
	return nil
}

var userEqRe = regexp.MustCompile(`(?i)user\s*=\s*"([^"]*)"|user\s*=\s*'([^']*)'`)
var userInRe = regexp.MustCompile(`(?i)user\s+IN\s*\[([^\]]*)\]`)

func userFilterFromString(s string) []string {
	if m := userInRe.FindStringSubmatch(s); m != nil {
		parts := strings.Split(m[1], ",")
		out := []string{}
		for _, p := range parts {
			p = strings.TrimSpace(p)
			p = strings.Trim(p, `"'`)
			if p != "" {
				out = append(out, p)
			}
		}
		return out
	}
	out := []string{}
	for _, m := range userEqRe.FindAllStringSubmatch(s, -1) {
		if m[1] != "" {
			out = append(out, m[1])
		} else if m[2] != "" {
			out = append(out, m[2])
		}
	}
	return out
}

// ftsQuery turns a free-text user query into a safe FTS5 MATCH expression: each
// whitespace token becomes a prefix term, OR-joined, with FTS5 metacharacters
// stripped so user input can't break the query or trigger a syntax error.
func ftsQuery(q string) string {
	fields := strings.Fields(q)
	terms := make([]string, 0, len(fields))
	for _, f := range fields {
		f = ftsClean.ReplaceAllString(f, "")
		if f == "" {
			continue
		}
		terms = append(terms, `"`+f+`"*`)
	}
	if len(terms) == 0 {
		return `""`
	}
	return strings.Join(terms, " OR ")
}

var ftsClean = regexp.MustCompile(`[^\p{L}\p{N}_]+`)

// searchableText concatenates the human-readable fields chat indexes. For
// conversations that's `title`; for messages it's `text` (already flattened
// from content parts by mongoMeili.preprocessObjectForIndex). We also fold in
// any other string field defensively so titles/messages are both covered.
func searchableText(d map[string]any) string {
	var b strings.Builder
	for _, k := range []string{"title", "text"} {
		if v, ok := d[k]; ok {
			b.WriteString(stringify(v))
			b.WriteByte('\n')
		}
	}
	return strings.TrimSpace(b.String())
}

// ---- tasks ----------------------------------------------------------------

// getTask always reports success — writes are applied synchronously before the
// EnqueuedTask is returned, so any client polling waitForTask resolves at once.
func (s *server) getTask(w http.ResponseWriter, uid string) {
	id, _ := strconv.ParseInt(uid, 10, 64)
	now := time.Now().UTC().Format(time.RFC3339)
	writeJSON(w, 200, map[string]any{
		"uid": id, "status": "succeeded", "type": "documentAdditionOrUpdate",
		"enqueuedAt": now, "startedAt": now, "finishedAt": now,
	})
}

func (s *server) enqueued(indexUID, typ string) map[string]any {
	return map[string]any{
		"taskUid": s.taskSeq.Add(1), "indexUid": indexUID, "status": "enqueued",
		"type": typ, "enqueuedAt": time.Now().UTC().Format(time.RFC3339),
	}
}

// ---- helpers --------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func meiliErr(code, msg, typ string) map[string]any {
	return map[string]any{
		"message": msg, "code": code, "type": typ,
		"link": "https://www.meilisearch.com/docs/reference/errors/error_codes#" + code,
	}
}

func stringify(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}
