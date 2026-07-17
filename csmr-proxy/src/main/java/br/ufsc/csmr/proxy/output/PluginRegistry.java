/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.proxy.output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Registry for OutputFunctionPlugin instances.
 *
 * Plugins are discovered at startup via Java's ServiceLoader mechanism.
 * This allows the addition of new f(D) implementations without modifying
 * the proxy core — simply drop a new JAR on the classpath and register it
 * in META-INF/services/br.ufsc.csmr.proxy.output.OutputFunctionPlugin.
 *
 * Usage (Bonatto 2026, slide 15):
 *   The YAML `output_function` field references a plugin by its class simple name.
 *   The PluginRegistry resolves it at dispatch time.
 */
@Component
public class PluginRegistry {

    private static final Logger log = LoggerFactory.getLogger(PluginRegistry.class);

    private final Map<String, OutputFunctionPlugin> plugins = new HashMap<>();
    private OutputFunctionPlugin defaultPlugin;

    public PluginRegistry() {
        // Discover all plugins on the classpath
        ServiceLoader<OutputFunctionPlugin> loader =
                ServiceLoader.load(OutputFunctionPlugin.class);

        for (OutputFunctionPlugin plugin : loader) {
            String op = plugin.handlesOperation();
            plugins.put(op, plugin);
            log.info("Registered OutputFunctionPlugin '{}' for operation '{}'.",
                    plugin.getClass().getSimpleName(), op);
        }

        // Fallback: if no ServiceLoader plugins found, register the built-in KvsOutputFunction
        if (plugins.isEmpty()) {
            log.warn("No ServiceLoader plugins found — registering built-in KvsOutputFunction.");
            KvsOutputFunction builtin = new KvsOutputFunction();
            plugins.put(builtin.handlesOperation(), builtin);
        }

        defaultPlugin = plugins.getOrDefault("*", plugins.values().iterator().next());
    }

    /**
     * Returns the plugin registered for the given operation, or empty if none specific.
     */
    public Optional<OutputFunctionPlugin> getPlugin(String operation) {
        OutputFunctionPlugin specific = plugins.get(operation.toLowerCase());
        return Optional.ofNullable(specific);
    }

    public OutputFunctionPlugin getDefaultPlugin() {
        return defaultPlugin;
    }

    /** Programmatic registration (useful for testing). */
    public void register(OutputFunctionPlugin plugin) {
        plugins.put(plugin.handlesOperation(), plugin);
    }
}
