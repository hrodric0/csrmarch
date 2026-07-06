package br.ufsc.csmr.proxy.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Registry for InputMapper instances.
 *
 * Mappers are discovered at startup via Java's ServiceLoader mechanism.
 * This allows dynamic transformation of input arguments without recompiling
 * the proxy core — simply drop a new JAR on the classpath and register it
 * in META-INF/services/br.ufsc.csmr.proxy.mapper.InputMapper.
 *
 * Usage:
 *   The YAML `input_mapper` field references a mapper by its composition name
 *   (e.g., "PutInputMapper" handles "PutWithLogging" composition).
 *   The InputMapperRegistry resolves it at dispatch time.
 */
@Component
public class InputMapperRegistry {

    private static final Logger log = LoggerFactory.getLogger(InputMapperRegistry.class);

    private final Map<String, InputMapper> mappers = new HashMap<>();

    public InputMapperRegistry() {
        // Discover all mappers on the classpath
        ServiceLoader<InputMapper> loader = ServiceLoader.load(InputMapper.class);

        for (InputMapper mapper : loader) {
            String compositionName = mapper.handlesComposition();
            mappers.put(compositionName, mapper);
            log.info("Registered InputMapper '{}' for composition '{}'.",
                    mapper.getClass().getSimpleName(), compositionName);
        }

        log.info("InputMapperRegistry initialized with {} mapper(s).", mappers.size());
    }

    /**
     * Returns the mapper registered for the given composition name.
     */
    public Optional<InputMapper> getMapper(String compositionName) {
        InputMapper mapper = mappers.get(compositionName);
        return Optional.ofNullable(mapper);
    }

    /** Programmatic registration (useful for testing). */
    public void register(InputMapper mapper) {
        mappers.put(mapper.handlesComposition(), mapper);
        log.info("Programmatically registered InputMapper '{}' for composition '{}'.",
                mapper.getClass().getSimpleName(), mapper.handlesComposition());
    }

    public Map<String, InputMapper> getAllMappers() {
        return new HashMap<>(mappers);
    }
}