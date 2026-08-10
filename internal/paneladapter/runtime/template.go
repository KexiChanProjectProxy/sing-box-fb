package runtime

import (
	"encoding/json"

	"github.com/sagernet/sing-box/internal/paneladapter/contract"
	E "github.com/sagernet/sing/common/exceptions"
)

func stripManagedInboundUsers(configuration *contract.ConfigurationResponse) (json.RawMessage, error) {
	if len(configuration.SingBoxConfigTemplate) == 0 {
		return nil, E.New("empty sing_box_config_template")
	}
	var template map[string]json.RawMessage
	if err := json.Unmarshal(configuration.SingBoxConfigTemplate, &template); err != nil {
		return nil, E.Cause(err, "parse config template as object")
	}
	managedTags := make(map[string]contract.ManagedInbound, len(configuration.ManagedInbounds))
	for _, inbound := range configuration.ManagedInbounds {
		managedTags[inbound.Tag] = inbound
	}
	inboundsRaw, exists := template["inbounds"]
	if !exists {
		return configuration.SingBoxConfigTemplate, nil
	}
	var inbounds []json.RawMessage
	if err := json.Unmarshal(inboundsRaw, &inbounds); err != nil {
		return nil, E.Cause(err, "parse inbounds array")
	}
	emptyUsers := json.RawMessage(`[]`)
	modified := false
	for index, inboundRaw := range inbounds {
		var inbound map[string]json.RawMessage
		if err := json.Unmarshal(inboundRaw, &inbound); err != nil {
			continue
		}
		tagRaw, hasTag := inbound["tag"]
		if !hasTag {
			continue
		}
		var tag string
		if err := json.Unmarshal(tagRaw, &tag); err != nil {
			continue
		}
		managedInbound, managed := managedTags[tag]
		if !managed {
			continue
		}
		if managedInbound.UserApplyPolicy == contract.ApplyOnUserNone || managedInbound.Protocol == contract.ProtocolShadowsocks {
			delete(inbound, "managed")
			delete(inbound, "users")
		} else if _, hasUsers := inbound["users"]; hasUsers {
			inbound["users"] = emptyUsers
		}
		modifiedInbound, err := json.Marshal(inbound)
		if err != nil {
			return nil, E.Cause(err, "re-marshal inbound with stripped users")
		}
		inbounds[index] = modifiedInbound
		modified = true
	}
	if !modified {
		return configuration.SingBoxConfigTemplate, nil
	}
	newInbounds, err := json.Marshal(inbounds)
	if err != nil {
		return nil, E.Cause(err, "re-marshal inbounds array")
	}
	template["inbounds"] = newInbounds
	result, err := json.Marshal(template)
	if err != nil {
		return nil, E.Cause(err, "re-marshal config template")
	}
	return result, nil
}
