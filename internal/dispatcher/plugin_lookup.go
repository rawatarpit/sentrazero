package dispatcher

import (
	"context"
	"log"
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

func ResolvePluginName(ctx context.Context, pluginID, pluginName string) string {
	if pluginName != "" {
		return pluginName
	}
	if pluginID != "" {
		if name, ok := pluginIDToName.Load(pluginID); ok {
			return name.(string)
		}

		// Plugin UUID not in local map — try on-demand fetch from API
		log.Printf("🔍 Plugin UUID %s not in local map, fetching from API...", pluginID)
		p, err := plugin.FetchAndInstallPluginByID(ctx, pluginID)
		if err != nil {
			log.Printf("⚠️ On-demand plugin fetch failed for %s: %v", pluginID, err)
			return ""
		}
		if p != nil {
			pluginIDToName.Store(p.ID, p.Name)
			pluginIDToName.Store(p.Name, p.Name)
			return p.Name
		}
	}
	return ""
}

func GetPluginIDToNameMap() *sync.Map {
	return &pluginIDToName
}
