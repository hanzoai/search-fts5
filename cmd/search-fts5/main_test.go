package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// newTestServer spins the shim against a temp SQLite file and returns an
// httptest server plus a helper to make requests, mirroring how the
// meilisearch@0.38 JS client drives the API.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	db := openTestDB(t)
	s := &server{db: db, indexes: map[string]*indexMeta{}}
	if err := s.initMeta(); err != nil {
		t.Fatal(err)
	}
	if err := s.loadIndexes(); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.route)
	return httptest.NewServer(mux)
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "search.db")
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func do(t *testing.T, method, url string, body any) (*http.Response, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, rdr)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, out
}

func TestHealth(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	resp, body := do(t, "GET", ts.URL+"/health", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("health status=%d", resp.StatusCode)
	}
	var m map[string]string
	_ = json.Unmarshal(body, &m)
	if m["status"] != "available" {
		t.Fatalf("health=%v", m)
	}
}

func TestIndexNotFoundThenCreate(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	// mongoMeili first does getRawInfo -> expects 404 index_not_found
	resp, body := do(t, "GET", ts.URL+"/indexes/convos", nil)
	if resp.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
	var e map[string]any
	_ = json.Unmarshal(body, &e)
	if e["code"] != "index_not_found" {
		t.Fatalf("expected index_not_found, got %v", e["code"])
	}
	// then createIndex
	resp, _ = do(t, "POST", ts.URL+"/indexes", map[string]string{"uid": "convos", "primaryKey": "conversationId"})
	if resp.StatusCode != 202 {
		t.Fatalf("createIndex status=%d", resp.StatusCode)
	}
	// now it exists
	resp, _ = do(t, "GET", ts.URL+"/indexes/convos", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("indexInfo status=%d", resp.StatusCode)
	}
}

// TestFullTextSearchWithUserFilter is the core acceptance test: index two
// users' conversations, then search with `user = "<id>"` and assert only that
// user's matching docs come back — exactly the call chat makes in
// Conversation.js getConvosByCursor.
func TestFullTextSearchWithUserFilter(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	_, _ = do(t, "POST", ts.URL+"/indexes", map[string]string{"uid": "convos", "primaryKey": "conversationId"})
	_, _ = do(t, "PATCH", ts.URL+"/indexes/convos/settings", map[string]any{"filterableAttributes": []string{"user"}})

	docs := []map[string]any{
		{"conversationId": "c1", "user": "alice", "title": "Kubernetes deployment notes"},
		{"conversationId": "c2", "user": "alice", "title": "Grocery list for dinner"},
		{"conversationId": "c3", "user": "bob", "title": "Kubernetes cluster upgrade"},
	}
	resp, _ := do(t, "POST", ts.URL+"/indexes/convos/documents", docs)
	if resp.StatusCode != 202 {
		t.Fatalf("addDocuments status=%d", resp.StatusCode)
	}

	// alice searches "kubernetes" -> only c1
	resp, body := do(t, "POST", ts.URL+"/indexes/convos/search", map[string]any{
		"q": "kubernetes", "filter": `user = "alice"`,
	})
	if resp.StatusCode != 200 {
		t.Fatalf("search status=%d body=%s", resp.StatusCode, body)
	}
	var sr struct {
		Hits []map[string]any `json:"hits"`
	}
	_ = json.Unmarshal(body, &sr)
	if len(sr.Hits) != 1 {
		t.Fatalf("expected 1 hit for alice/kubernetes, got %d: %s", len(sr.Hits), body)
	}
	if sr.Hits[0]["conversationId"] != "c1" {
		t.Fatalf("expected c1, got %v", sr.Hits[0]["conversationId"])
	}

	// prefix match: "kube" should still match c1 for alice
	_, body = do(t, "POST", ts.URL+"/indexes/convos/search", map[string]any{
		"q": "kube", "filter": `user = "alice"`,
	})
	_ = json.Unmarshal(body, &sr)
	if len(sr.Hits) != 1 {
		t.Fatalf("expected 1 prefix hit, got %d", len(sr.Hits))
	}

	// bob must never see alice's docs
	_, body = do(t, "POST", ts.URL+"/indexes/convos/search", map[string]any{
		"q": "grocery", "filter": `user = "bob"`,
	})
	_ = json.Unmarshal(body, &sr)
	if len(sr.Hits) != 0 {
		t.Fatalf("tenant isolation broken: bob saw %d alice docs", len(sr.Hits))
	}
}

func TestUpdateAndDelete(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	_, _ = do(t, "POST", ts.URL+"/indexes", map[string]string{"uid": "messages", "primaryKey": "messageId"})

	_, _ = do(t, "POST", ts.URL+"/indexes/messages/documents", []map[string]any{
		{"messageId": "m1", "user": "alice", "text": "hello world"},
	})
	// update text via PUT (mongoMeili updateObjectToMeili)
	_, _ = do(t, "PUT", ts.URL+"/indexes/messages/documents", []map[string]any{
		{"messageId": "m1", "user": "alice", "text": "completely different content"},
	})
	_, body := do(t, "POST", ts.URL+"/indexes/messages/search", map[string]any{"q": "different", "filter": `user = "alice"`})
	var sr struct {
		Hits []map[string]any `json:"hits"`
	}
	_ = json.Unmarshal(body, &sr)
	if len(sr.Hits) != 1 {
		t.Fatalf("expected updated doc to match 'different', got %d", len(sr.Hits))
	}
	// old text must no longer match
	_, body = do(t, "POST", ts.URL+"/indexes/messages/search", map[string]any{"q": "hello", "filter": `user = "alice"`})
	_ = json.Unmarshal(body, &sr)
	if len(sr.Hits) != 0 {
		t.Fatalf("stale FTS row: 'hello' still matches after update")
	}

	// delete-batch
	_, _ = do(t, "POST", ts.URL+"/indexes/messages/documents/delete-batch", []string{"m1"})
	_, body = do(t, "POST", ts.URL+"/indexes/messages/search", map[string]any{"q": "different", "filter": `user = "alice"`})
	_ = json.Unmarshal(body, &sr)
	if len(sr.Hits) != 0 {
		t.Fatalf("doc not deleted")
	}
}

func TestGetDocumentsPagination(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()
	_, _ = do(t, "POST", ts.URL+"/indexes", map[string]string{"uid": "convos", "primaryKey": "conversationId"})
	docs := []map[string]any{}
	for i := 0; i < 5; i++ {
		docs = append(docs, map[string]any{"conversationId": "c" + string(rune('0'+i)), "user": "u", "title": "t"})
	}
	_, _ = do(t, "POST", ts.URL+"/indexes/convos/documents", docs)
	_, body := do(t, "GET", ts.URL+"/indexes/convos/documents?limit=2&offset=0", nil)
	var gr struct {
		Results []json.RawMessage `json:"results"`
		Total   int               `json:"total"`
	}
	_ = json.Unmarshal(body, &gr)
	if gr.Total != 5 {
		t.Fatalf("expected total 5, got %d", gr.Total)
	}
	if len(gr.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(gr.Results))
	}
}
