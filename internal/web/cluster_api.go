package web

import (
	"encoding/json"
	"net/http"

	"github.com/Hoverhuang-er/godis/internal/config"
	"github.com/Hoverhuang-er/godis/internal/lib/utils"
)

// clusterJoinRequest is the payload for POST /api/cluster/join.
// A joining node (or an operator acting for it) sends its advertised redis
// and raft addresses; the web server forwards the handshake to the local
// godis node which performs the actual raft join.
type clusterJoinRequest struct {
	ClusterID string `json:"cluster_id"`
	RedisAddr string `json:"redis_addr"`
	RaftAddr  string `json:"raft_addr"`
	Master    string `json:"master,omitempty"`
}

// handleClusterJoin forwards a cluster-join handshake to the local godis
// node. It mirrors what a TCP-joining node sends on the seed's redis port,
// so both the classic TCP channel and the HTTP API can establish clusters.
func (s *ApiServer) handleClusterJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "only POST allowed"})
		return
	}
	var req clusterJoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid JSON body"})
		return
	}
	if req.RedisAddr == "" || req.RaftAddr == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "redis_addr and raft_addr are required"})
		return
	}
	client := s.getClient()
	if client == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "redis connection not ready"})
		return
	}
	cliArgs := utils.ToCmdLine("cluster.join")
	// A cluster id is required on seeds that have one; keep the legacy
	// 2-arg form for seeds without a configured cluster id.
	if config.Properties.ClusterID != "" {
		cliArgs = append(cliArgs, utils.ToCmdLine(config.Properties.ClusterID)...)
	}
	cliArgs = append(cliArgs, utils.ToCmdLine(req.RedisAddr, req.RaftAddr)...)
	if req.Master != "" {
		cliArgs = append(cliArgs, utils.ToCmdLine(req.Master)...)
	}
	reply := client.Send(cliArgs)
	if isErrorReply(reply) {
		writeJSON(w, http.StatusOK, commandResult{
			Success: false,
			Error:   extractError(reply),
		})
		return
	}
	writeJSON(w, http.StatusOK, commandResult{
		Success: true,
		Result:  "node joined cluster",
	})
}

// clusterStatusFn reports live cluster state for GET /api/cluster/status.
// It is wired up by main via RegisterClusterStatusFn so the web package
// stays decoupled from the cluster package.
var clusterStatusFn func() (map[string]interface{}, bool)

// RegisterClusterStatusFn registers the callback that reports cluster state
// on GET /api/cluster/status.
func RegisterClusterStatusFn(fn func() (map[string]interface{}, bool)) {
	clusterStatusFn = fn
}

// handleClusterStatus reports cluster state. When the node is not running in
// cluster mode it returns a minimal standalone answer.
func (s *ApiServer) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "only GET allowed"})
		return
	}
	status := map[string]interface{}{
		"enabled":    config.Properties.ClusterEnable,
		"cluster_id": config.Properties.ClusterID,
		"mode":       "standalone",
	}
	if clusterStatusFn != nil {
		if live, ok := clusterStatusFn(); ok {
			status = live
		}
	}
	writeJSON(w, http.StatusOK, status)
}
