package sql

import (
	"errors"
	"fmt"
	"slices"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/resolver"
	expmaps "golang.org/x/exp/maps"
)

var ErrPluginNotSupported = errors.New("plugin not supported")

var supportedPlugins = map[string]sqlplugin.Plugin{}

// RegisterPlugin will register a SQL plugin
func RegisterPlugin(pluginName string, plugin sqlplugin.Plugin) {
	if _, ok := supportedPlugins[pluginName]; ok {
		panic("plugin " + pluginName + " already registered")
	}
	supportedPlugins[pluginName] = plugin
}

// RegisterPluginAlias registers an alias for an already-registered plugin.
// Panics if the alias already exists or the target plugin is not registered.
func RegisterPluginAlias(alias string, target string) {
	if supportedPlugins == nil {
		supportedPlugins = make(map[string]sqlplugin.Plugin)
	}
	if _, ok := supportedPlugins[alias]; ok {
		panic("plugin " + alias + " already registered")
	}
	targetPlugin, ok := supportedPlugins[target]
	if !ok {
		panic("plugin " + target + " not registered")
	}
	supportedPlugins[alias] = targetPlugin
}

// NewSQLDB creates a returns a reference to a logical connection to the
// underlying SQL database. The returned object is tied to a single
// SQL database and the object can be used to perform CRUD operations on
// the tables in the database.
func NewSQLDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
	logger log.Logger,
	mh metrics.Handler,
) (sqlplugin.DB, error) {
	return createDB[sqlplugin.DB](dbKind, cfg, r, logger, mh)
}

// NewSQLAdminDB returns a AdminDB.
func NewSQLAdminDB(
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
	logger log.Logger,
	mh metrics.Handler,
) (sqlplugin.AdminDB, error) {
	return createDB[sqlplugin.AdminDB](dbKind, cfg, r, logger, mh)
}

func createDB[T any](
	dbKind sqlplugin.DbKind,
	cfg *config.SQL,
	r resolver.ServiceResolver,
	logger log.Logger,
	mh metrics.Handler,
) (T, error) {
	var res T
	plugin, err := getPlugin(cfg.PluginName)
	if err != nil {
		return res, err
	}
	db, err := plugin.CreateDB(dbKind, cfg, r, logger, mh)
	if err != nil {
		return res, err
	}
	//revive:disable-next-line:unchecked-type-assertion
	res = db.(T)
	return res, err
}

func getPlugin(pluginName string) (sqlplugin.Plugin, error) {
	plugin, ok := supportedPlugins[pluginName]
	if !ok {
		keys := expmaps.Keys(supportedPlugins)
		slices.Sort(keys)
		return nil, fmt.Errorf(
			"%w: unknown plugin %q, supported plugins: %v",
			ErrPluginNotSupported,
			pluginName,
			keys,
		)
	}
	return plugin, nil
}

func GetPluginVisibilityQueryConverter(pluginName string) (sqlplugin.VisibilityQueryConverter, error) {
	plugin, err := getPlugin(pluginName)
	if err != nil {
		return nil, err
	}
	return plugin.GetVisibilityQueryConverter(), nil
}
