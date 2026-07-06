package br.ufsc.csmr.proxy.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * InputMapper for the ChainingSMR PRNG → Counter workflow.
 *
 * Stage 1: Extracts min/max from client params for PRNG generation.
 */
public class PrngInputMapper implements InputMapper {

    private static final Logger log = LoggerFactory.getLogger(PrngInputMapper.class);

    @Override
    public String handlesComposition() {
        return "PrngInputMapper";
    }

    @Override
    public Map<String, String> map(
            Map<String, String> clientInput,
            String targetService,
            String targetMethod,
            String entryFormat) {

        if (!"prng_service".equals(targetService)) {
            throw new IllegalArgumentException("PrngInputMapper only handles prng_service");
        }

        log.debug("Mapping input for prng_service.Generate: {}", clientInput);

        // Extract min/max from client input
        String min = clientInput.getOrDefault("min", "0");
        String max = clientInput.getOrDefault("max", "100");

        return Map.of(
            "min", min,
            "max", max
        );
    }
}