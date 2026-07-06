package br.ufsc.csmr.proxy.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * InputMapper for the "PutWithLogging" composition (Scenario 2).
 *
 * Transforms client KVS put({key,value}) requests:
 *   - Target: kv_store.Put → No transformation, passthrough
 *   - Target: logger.Append → Formats as "put(key,value)" string
 *
 * Example transformation:
 *   Client input: {"key":"hello","value":"world"}
 *   logger target: {"entry":"put(hello,world)"}
 */
public class PutInputMapper implements InputMapper {

    private static final Logger log = LoggerFactory.getLogger(PutInputMapper.class);

    @Override
    public String handlesComposition() {
        return "PutWithLogging";
    }

    @Override
    public Map<String, String> map(
            Map<String, String> clientInput,
            String targetService,
            String targetMethod,
            String entryFormat) {

        String key = clientInput.get("key");
        String value = clientInput.get("value");

        if (key == null) {
            throw new IllegalArgumentException("Missing required parameter: key");
        }
        if (value == null) {
            throw new IllegalArgumentException("Missing required parameter: value");
        }

        log.debug("Mapping input for target {}/{}: key={}, value={}", targetService, targetMethod, key, value);

        switch (targetService) {
            case "kv_store":
                // Passthrough for KVS
                return new LinkedHashMap<>(clientInput);

            case "logger":
                // Format as single entry string
                String formattedEntry = formatEntry(entryFormat, key, value);
                return Map.of("entry", formattedEntry);

            default:
                throw new IllegalArgumentException("Unknown target service: " + targetService);
        }
    }

    /**
     * Formats the entry string using the template.
     * Supports placeholders like {key}, {value}.
     */
    private String formatEntry(String template, String key, String value) {
        if (template == null || template.isEmpty()) {
            // Default format if not specified
            return String.format("put(\"%s\",\"%s\")", key, value);
        }

        String result = template
            .replace("{key}", key)
            .replace("{value}", value);

        log.debug("Formatted entry: {}", result);
        return result;
    }
}