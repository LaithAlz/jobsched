package api

import (
	_ "embed"
	"net/http"
)

// The demo UI is compiled into the binary so ./bin/scheduler is self-sufficient and does not
// depend on its working directory. See NOTES.md D-064.
//
//go:embed ui/index.html
var indexHTML []byte

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// The page is embedded and changes only on rebuild, but no-store keeps a stale copy from
	// surviving a recompile during a demo.
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(indexHTML)
}
