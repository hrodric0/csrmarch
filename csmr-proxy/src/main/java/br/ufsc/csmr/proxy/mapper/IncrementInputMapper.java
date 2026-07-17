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
 * InputMapper for incrementing counter from previous stage output.
 *
 * Extracts the counter value from the previous stage's JSON output and
 * formats it as the "amount" parameter for the Counter Increment operation.
 */
public class IncrementInputMapper implements InputMapper {

    private static final Logger log = LoggerFactory.getLogger(IncrementInputMapper.class);

    @Override
    public String handlesComposition() {
        return "IncrementInputMapper";
    }

    @Override
    public Map<String, String> map(
            Map<String, String> clientInput,
            String targetService,
            String targetMethod,
            String entryFormat) {

        if (!"counter_service".equals(targetService)) {
            throw new IllegalArgumentException("IncrementInputMapper only handles counter_service");
        }

        log.debug("Mapping input for counter_service.Increment: {}", clientInput);

        // Extract the "input" which contains the counter value to increment by
        String amount = clientInput.getOrDefault("input", "1");

        return Map.of("amount", amount);
    }
}