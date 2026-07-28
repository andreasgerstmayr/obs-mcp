package metrics

import (
	"github.com/containers/kubernetes-mcp-server/pkg/api"
)

// Toolset implements the observability toolset for advanced Prometheus monitoring.
type Toolset struct{}

var _ api.Toolset = (*Toolset)(nil)

func (t *Toolset) GetName() string {
	return ToolsetName
}

func (t *Toolset) GetDescription() string {
	return "Toolset for querying Prometheus and Alertmanager endpoints in efficient ways."
}

func (t *Toolset) GetTools(_ api.FilteringProvider) []api.ServerTool {
	return []api.ServerTool{
		initListMetrics(),
		initExecuteInstantQuery(),
		initExecuteRangeQuery(),
		initShowTimeseries(),
		initGetLabelNames(),
		initGetLabelValues(),
		initGetSeries(),
		initGetAlerts(),
		initGetSilences(),
	}
}

func (t *Toolset) GetPrompts() []api.ServerPrompt {
	return nil
}

func (t *Toolset) GetResources() []api.ServerResource {
	return nil
}

func (t *Toolset) GetResourceTemplates() []api.ServerResourceTemplate {
	return nil
}
