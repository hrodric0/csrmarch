package br.ufsc.csmr.proxy.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * InputMapper for setting counter value from previous stage output.
 *
 * Extracts the "value" from the previous stage's JSON output and
 * formats it as the "value" parameter for the Counter SetValue operation.
 */
public class CounterValueMapper implements InputMapper {

    private static final Logger log = LoggerFactory.getLogger(CounterValueMapper.class);

    @Override
    public String handlesComposition() {
        return "CounterValueMapper";
    }

    @Override
    public Map<String, String> map(
            Map<String, String> clientInput,
            String targetService,
            String targetMethod,
            String entryFormat) {

        if (!"counter_service".equals(targetService)) {
            throw new IllegalArgumentException("CounterValueMapper only handles counter_service");
        }

        log.debug("Mapping input for counter_service.SetValue: {}", clientInput);

        // Extract the "value" from the previous stage's output
        String value = clientInput.get("input");

        if (value == null) {
            throw new IllegalArgumentException("Missing 'input' field for CounterValueMapper");
        }

        return Map.of("value", value);
    }
}