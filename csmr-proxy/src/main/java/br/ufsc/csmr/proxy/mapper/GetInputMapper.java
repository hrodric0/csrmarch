/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.proxy.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * InputMapper for the "GetWithLogging" composition (Scenario 2 - variant).
 *
 * Transforms client KVS get(key) requests:
 *   - Target: kv_store.Get → Passthrough
 *   - Target: logger.Append → Formats as "get(key)" for audit trail
 *
 * This demonstrates that read operations can also be audited transparently.
 */
public class GetInputMapper implements InputMapper {

    private static final Logger log = LoggerFactory.getLogger(GetInputMapper.class);

    @Override
    public String handlesComposition() {
        return "GetWithLogging";
    }

    @Override
    public Map<String, String> map(
            Map<String, String> clientInput,
            String targetService,
            String targetMethod,
            String entryFormat) {

        String key = clientInput.get("key");

        if (key == null) {
            throw new IllegalArgumentException("Missing required parameter: key");
        }

        log.debug("Mapping input for target {}/{}: key={}", targetService, targetMethod, key);

        switch (targetService) {
            case "kv_store":
                // Passthrough for KVS
                return new LinkedHashMap<>(clientInput);

            case "logger":
                // Format as single entry string
                String formattedEntry = formatEntry(entryFormat, key);
                return Map.of("entry", formattedEntry);

            default:
                throw new IllegalArgumentException("Unknown target service: " + targetService);
        }
    }

    private String formatEntry(String template, String key) {
        if (template == null || template.isEmpty()) {
            return String.format("get(\"%s\")", key);
        }

        String result = template.replace("{key}", key);
        log.debug("Formatted entry: {}", result);
        return result;
    }
}