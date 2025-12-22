package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
)

type noopPlugin struct{}

func (n *noopPlugin) CreateDB(
	_ sqlplugin.DbKind,
	_ *config.SQL,
	_ resolver.ServiceResolver,
	_ log.Logger,
	_ metrics.Handler,
) (sqlplugin.GenericDB, error) {
	return nil, nil
}

func (n *noopPlugin) GetVisibilityQueryConverter() sqlplugin.VisibilityQueryConverter {
	return nil
}

func TestRegisterPluginAlias(t *testing.T) {
	// preserve existing plugin registrations
	orig := make(map[string]sqlplugin.Plugin, len(supportedPlugins))
	for k, v := range supportedPlugins {
		orig[k] = v
	}
	t.Cleanup(func() {
		supportedPlugins = orig
	})

	RegisterPlugin("base-plugin", &noopPlugin{})
	RegisterPluginAlias("alias-plugin", "base-plugin")

	p, err := getPlugin("alias-plugin")
	require.NoError(t, err)
	require.Equal(t, supportedPlugins["base-plugin"], p)
}
