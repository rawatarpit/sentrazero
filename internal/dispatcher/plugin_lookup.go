package dispatcher

import (
	"sync"

	"sentra-agent/internal/plugin"
)

var pluginIDToName sync.Map

func PopulatePluginIDMap(plugins []plugin.DBPlugin) {
	for _, p := range plugins {
		if p.ID != "" && p.Name != "" {
			pluginIDToName.Store(p.ID, p.Name)
			pluginIDToName.Store(p.Name, p.Name)
			if len(p.Name) > 7 && p.Name[:7] == "plugin_" {
				pluginIDToName.Store(p.Name[7:], p.Name)
			}
		}
	}
}

func ResolvePluginName(pluginID, pluginName string) string {
	if pluginName != "" {
		return pluginName
	}
	if pluginID != "" {
		if name, ok := pluginIDToName.Load(pluginID); ok {
			return name.(string)
		}
	}
	return ""
}

func GetPluginIDToNameMap() *sync.Map {
	return &pluginIDToName
}
