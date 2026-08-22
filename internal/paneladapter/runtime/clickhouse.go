package runtime

import (
	"encoding/json"
	"os"
	"strings"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	E "github.com/sagernet/sing/common/exceptions"
)

const defaultClickHouseTable = "sessions"

// accessLogNodeID is the ClickHouse node column / service tag.
// Local hostname is preferred; panel node_id is the fallback.
func accessLogNodeID(fallback string) string {
	host, err := os.Hostname()
	if err == nil {
		host = strings.TrimSpace(host)
		if host != "" {
			return host
		}
	}
	if fallback != "" {
		return fallback
	}
	return "sing-box"
}

// injectClickHouseService inserts a clickhouse access-log service using
// panel-pushed address and credentials. The service tag is node (hostname).
// Existing type=clickhouse services in the template are replaced.
func injectClickHouseService(template json.RawMessage, ch *contract.ClickHouseConfig, node string) (json.RawMessage, error) {
	if ch == nil {
		return template, nil
	}
	if len(template) == 0 {
		return nil, E.New("empty sing_box_config_template")
	}
	if strings.TrimSpace(node) == "" {
		return nil, E.New("missing clickhouse node id")
	}

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(template, &obj); err != nil {
		return nil, E.Cause(err, "parse config template as object")
	}

	var services []json.RawMessage
	if raw, ok := obj["services"]; ok && len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &services); err != nil {
			return nil, E.Cause(err, "parse services array")
		}
	}

	kept := make([]json.RawMessage, 0, len(services)+1)
	for _, svcRaw := range services {
		var svc map[string]json.RawMessage
		if err := json.Unmarshal(svcRaw, &svc); err != nil {
			kept = append(kept, svcRaw)
			continue
		}
		var typ string
		if err := json.Unmarshal(svc["type"], &typ); err != nil {
			kept = append(kept, svcRaw)
			continue
		}
		if typ == C.TypeClickHouse {
			continue
		}
		kept = append(kept, svcRaw)
	}

	injected, err := json.Marshal(clickHouseServiceObject(ch, node))
	if err != nil {
		return nil, E.Cause(err, "marshal clickhouse service")
	}
	kept = append(kept, injected)

	encoded, err := json.Marshal(kept)
	if err != nil {
		return nil, E.Cause(err, "re-marshal services array")
	}
	obj["services"] = encoded

	result, err := json.Marshal(obj)
	if err != nil {
		return nil, E.Cause(err, "re-marshal config template")
	}
	return result, nil
}

func clickHouseServiceObject(ch *contract.ClickHouseConfig, node string) map[string]any {
	table := strings.TrimSpace(ch.Table)
	if table == "" {
		table = defaultClickHouseTable
	}
	svc := map[string]any{
		"type":   C.TypeClickHouse,
		"tag":    node,
		"server": ch.Server,
		"table":  table,
	}
	if ch.ServerPort != 0 {
		svc["server_port"] = ch.ServerPort
	}
	if ch.Database != "" {
		svc["database"] = ch.Database
	}
	if ch.Username != "" {
		svc["username"] = ch.Username
	}
	if ch.Password != "" {
		svc["password"] = ch.Password
	}
	if ch.Protocol != "" {
		svc["protocol"] = ch.Protocol
	}
	if ch.TLS != nil {
		tls := map[string]any{}
		if ch.TLS.Enabled {
			tls["enabled"] = true
		}
		if ch.TLS.ServerName != "" {
			tls["server_name"] = ch.TLS.ServerName
		}
		if ch.TLS.Insecure {
			tls["insecure"] = true
		}
		svc["tls"] = tls
	}
	return svc
}
