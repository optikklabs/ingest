package servicegraph

import spansschema "github.com/optikklabs/ingest/internal/ingestion/spans/schema"

// Connection types classify a virtual edge by the peer attribute that named it.
const (
	connDatabase  = "database"
	connMessaging = "messaging"

	// virtualNodeServer marks an edge whose server side is a synthesized,
	// uninstrumented peer rather than an instrumented service.
	virtualNodeServer = "server"
)

// EdgeKey identifies one directed service-graph edge. VirtualNode and
// ConnectionType are empty for ordinary instrumented edges and populated only
// for synthesized edges to uninstrumented peers (databases, queues, ...).
// status.code is intentionally not part of the key: request/error counts live
// in dedicated counters, and the latency histograms carry no status dimension.
type EdgeKey struct {
	TenantId       uint32
	Client         string
	Server         string
	VirtualNode    string
	ConnectionType string
}

// isErrorStatus reports whether a span status code denotes a failed request.
// Matches query's seriesattr.StatusErrorPred so both sides agree on failures.
func isErrorStatus(code string) bool {
	return code == "STATUS_CODE_ERROR" || code == "ERROR"
}

// resolvePeer derives a virtual server-node name for a client span that has no
// instrumented peer. It follows a fixed attribute priority (mirroring the OTel
// servicegraph connector) and returns the connection type implied by the match.
// Promoted keys (peer.service, db.system, db.name) are read via getters because
// the ingest mapper strips them from the generic attribute map.
func resolvePeer(row *spansschema.Row) (name, connType string) {
	if v := row.GetPeerService(); v != "" {
		return v, ""
	}
	if v := row.GetDbSystem(); v != "" {
		return v, connDatabase
	}
	if v := row.GetDbName(); v != "" {
		return v, connDatabase
	}
	attrs := row.GetAttributes()
	if v := attrs["server.address"]; v != "" {
		return v, ""
	}
	if v := attrs["messaging.destination.name"]; v != "" {
		return v, connMessaging
	}
	return "", ""
}
