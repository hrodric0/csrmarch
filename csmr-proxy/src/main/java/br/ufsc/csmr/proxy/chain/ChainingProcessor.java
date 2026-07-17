/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.proxy.chain;

import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec.ChainingStage;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Processor for chained SMR workflows (PoC 2).
 *
 * Implements the "ChainingSMR" scenario where the output of one operation
 * becomes the input to a subsequent operation, creating an automated
 * feedback loop.
 *
 * Example workflow: PRNG → Counter
 *   Stage 1: prng.generate() → {result: 12345}
 *   Stage 2: counter.setValue(12345) → {result: true}
 *
 * The processor:
 *   1. Executes stages in order
 *   2. Extracts output_field from each stage
 *   3. Formats as input for next stage via InputMapper
 *   4. Optionally returns intermediate results
 */
@Component
public class ChainingProcessor {

    private static final Logger log = LoggerFactory.getLogger(ChainingProcessor.class);

    private final ObjectMapper objectMapper;

    public ChainingProcessor() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Executes a chain of SMR operations.
     *
     * @param stages      List of chaining stages in execution order
     * @param clientInput Original client input parameters
     * @param dispatchFn  Function to dispatch to a target service
     * @return            ChainResult with final output and optionally intermediate results
     */
    public ChainResult execute(
            List<ChainingStage> stages,
            Map<String, String> clientInput,
            DispatchFunction dispatchFn) throws Exception {

        if (stages == null || stages.isEmpty()) {
            throw new IllegalArgumentException("Chaining requires at least one stage");
        }

        // Sort stages by stage_order
        stages = new ArrayList<>(stages);
        stages.sort(Comparator.comparingInt(ChainingStage::getStageOrder));

        log.info("Executing chaining workflow with {} stages", stages.size());

        List<StageResult> stageResults = new ArrayList<>();
        Map<String, String> currentInput = new LinkedHashMap<>(clientInput);

        for (int i = 0; i < stages.size(); i++) {
            ChainingStage stage = stages.get(i);

            log.info("Executing stage {}/{}: {}/{}",
                    i + 1, stages.size(), stage.getTargetService(), stage.getTargetMethod());

            // Dispatch to the target service (applying the stage's input_mapper)
            String output = dispatchFn.dispatch(
                    stage.getTargetService(),
                    stage.getTargetMethod(),
                    currentInput,
                    stage.getInputMapper()
            );

            // Parse output to JSON map
            Map<String, Object> outputMap = parseOutput(output);

            // Store stage result
            stageResults.add(new StageResult(
                    stage.getTargetService(),
                    stage.getTargetMethod(),
                    output
            ));

            // Prepare input for next stage
            if (i < stages.size() - 1) {
                String outputField = stage.getOutputField();
                if (outputField != null && !outputField.isEmpty()) {
                    Object extractedValue = extractOutputField(outputMap, outputField);
                    if (extractedValue != null) {
                        // Pass extracted value to next stage
                        // In production, this would use an InputMapper for transformation
                        currentInput = Map.of("input", extractedValue.toString());
                        log.debug("Extracted '{}' = {} for next stage", outputField, extractedValue);
                    } else {
                        log.warn("Output field '{}' not found in stage output", outputField);
                    }
                }
            }

            // Wait for completion if specified
            if (stage.isWaitForCompletion()) {
                // In production, this would ensure the Paxos proposal is committed
                log.debug("Stage {}/{} completed (wait for completion)", i + 1, stages.size());
            }
        }

        // Final output is the last stage's output
        String finalOutput = stageResults.isEmpty() ? null : stageResults.get(stageResults.size() - 1).output();

        log.info("Chaining workflow completed. Final output: {}", finalOutput);

        return new ChainResult(finalOutput, stageResults);
    }

    /**
     * Extracts a named output field from a stage's proxy payload.
     *
     * The proxy payload from a sidecar has the shape:
     *   {"group":..., "op":..., "params":..., "sidecar_response":
     *       {"instance":N, "status":"decided", "ring":..., "app_response":"{...}"}, "address":...}
     *
     * The application's actual return value lives inside the embedded
     * {@code app_response} JSON string (surfaced by the sidecar after synchronous
     * delivery). This extracts {@code outputField} from there, falling back to a
     * top-level field if present.
     */
    @SuppressWarnings("unchecked")
    private Object extractOutputField(Map<String, Object> outputMap, String field) {
        // 1. Top-level field (e.g. if already flattened by the proxy)
        if (outputMap.containsKey(field)) {
            return outputMap.get(field);
        }

        // 2. Inside sidecar_response.app_response (the real location)
        Object sidecarResp = outputMap.get("sidecar_response");
        if (sidecarResp instanceof Map) {
            Map<String, Object> sr = (Map<String, Object>) sidecarResp;
            Object appResp = sr.get("app_response");
            Map<String, Object> appMap = null;
            if (appResp instanceof Map) {
                appMap = (Map<String, Object>) appResp;
            } else if (appResp instanceof String && !((String) appResp).isBlank()) {
                // app_response is embedded as a JSON string by the sidecar
                try {
                    appMap = objectMapper.readValue((String) appResp,
                            new TypeReference<Map<String, Object>>() {});
                } catch (Exception e) {
                    log.warn("Could not parse embedded app_response for field '{}': {}", field, appResp);
                }
            }
            if (appMap != null && appMap.containsKey(field)) {
                return appMap.get(field);
            }
        }
        return null;
    }

    /**
     * Parses output string to JSON map.
     */
    private Map<String, Object> parseOutput(String output) throws Exception {
        try {
            return objectMapper.readValue(output, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            log.error("Failed to parse output as JSON: {}", output, e);
            // Return raw string wrapped in map
            return Map.of("raw", output);
        }
    }

    /**
     * Result of a single stage execution.
     */
    public record StageResult(
            String targetService,
            String targetMethod,
            String output
    ) {}

    /**
     * Result of the entire chain execution.
     */
    public record ChainResult(
            String finalOutput,
            List<StageResult> stageResults
    ) {}

    /**
     * Function interface for dispatching to target services.
     * {@code inputMapperName} is the stage's configured input_mapper (may be null),
     * used to transform the chained input before it reaches the target ring.
     */
    @FunctionalInterface
    public interface DispatchFunction {
        String dispatch(String service, String method, Map<String, String> input, String inputMapperName) throws Exception;
    }
}