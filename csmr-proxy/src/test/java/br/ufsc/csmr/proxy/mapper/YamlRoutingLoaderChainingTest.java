/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.proxy.mapper;

import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec;
import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec.ChainingStage;
import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec.CompositionRule;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that {@code chaining_stages} (PoC 2 feedback loops) are parsed from
 * the composition YAML into the spec DTO. Before the fix the parser silently
 * dropped these stages, so a Chaining composition loaded with zero stages.
 *
 * The private {@code convertYamlToSpec} is invoked via reflection so the test
 * does not depend on classpath resource resolution or Spring context startup.
 */
class YamlRoutingLoaderChainingTest {

    private static final String CHAINING_YAML = """
            spec:
              toleratedFailures: 1
              compositions:
                - name: "InitCounterWithRandom"
                  type: "Chaining"
                  method: "init_counter"
                  return_intermediate: true
                  chaining_stages:
                    - stage_order: 1
                      target_service: "prng_service"
                      target_method: "Generate"
                      output_field: "result"
                      input_mapper: "PrngInputMapper"
                    - stage_order: 2
                      target_service: "counter_service"
                      target_method: "SetValue"
                      output_field: "result"
                      input_mapper: "CounterValueMapper"
                      wait_for_completion: false
            """;

    @SuppressWarnings("unchecked")
    private CsmrCompositionSpec parse(String yamlText) throws Exception {
        Map<String, Object> yamlData = new Yaml().load(yamlText);
        YamlRoutingLoader loader = new YamlRoutingLoader();
        Method m = YamlRoutingLoader.class.getDeclaredMethod("convertYamlToSpec", Map.class);
        m.setAccessible(true);
        return (CsmrCompositionSpec) m.invoke(loader, yamlData);
    }

    @Test
    void chainingStagesAreParsedInOrder() throws Exception {
        CsmrCompositionSpec spec = parse(CHAINING_YAML);

        List<CompositionRule> comps = spec.getCompositions();
        assertNotNull(comps);
        assertEquals(1, comps.size());

        CompositionRule rule = comps.get(0);
        assertEquals("InitCounterWithRandom", rule.getName());
        assertTrue(rule.isReturnIntermediate(), "return_intermediate not parsed");

        List<ChainingStage> stages = rule.getChainingStages();
        assertNotNull(stages, "chaining_stages dropped during parse");
        assertEquals(2, stages.size());

        ChainingStage first = stages.get(0);
        assertEquals(1, first.getStageOrder());
        assertEquals("prng_service", first.getTargetService());
        assertEquals("Generate", first.getTargetMethod());
        assertEquals("result", first.getOutputField());
        assertEquals("PrngInputMapper", first.getInputMapper());
        assertTrue(first.isWaitForCompletion(), "default wait_for_completion should be true");

        ChainingStage second = stages.get(1);
        assertEquals(2, second.getStageOrder());
        assertEquals("counter_service", second.getTargetService());
        assertEquals("CounterValueMapper", second.getInputMapper());
        assertFalse(second.isWaitForCompletion(), "explicit wait_for_completion=false not parsed");
    }

    @Test
    void compositionWithoutChainingStagesHasNullStages() throws Exception {
        String yaml = """
                spec:
                  toleratedFailures: 1
                  compositions:
                    - name: "PlainPut"
                      type: "Simple"
                      method: "put"
                """;
        CompositionRule rule = parse(yaml).getCompositions().get(0);
        assertNull(rule.getChainingStages());
        assertFalse(rule.isReturnIntermediate());
    }
}
