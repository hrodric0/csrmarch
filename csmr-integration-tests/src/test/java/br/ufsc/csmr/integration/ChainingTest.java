/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test PoC 2: Chaining SMRs (PRNG → Counter) — Alves 2026, Future Work / ChainingSMRs
 * Mirrors test_chaining.sh.
 *
 * Purpose: Verify an SMR operation's output becomes another SMR operation's input
 * through an automated feedback loop:
 *   stage 1: prng_service.Generate(min,max)  → deterministic number N
 *   stage 2: counter_service.SetValue(N)      → counter set to N
 * With return_intermediate: true, BOTH stage outputs are returned.
 *
 * What we are testing:
 *   1. The chain response contains intermediate stage results ("chain")
 *   2. Both stages (prng_service.Generate + counter_service.SetValue) are present
 *   3. The counter value EQUALS the PRNG-generated value  →  the chain actually propagated
 *   4. The PRNG value is within the declared [min,max] range (deterministic, valid)
 *
 * Why this matters:
 *   - Demonstrates automated feedback loops across independent SMR groups.
 *   - Fails if the stages do not chain, or the counter value != PRNG value (broken loop).
 */
@ExtendWith(CsmrDockerComposeExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ChainingTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CsmrProxyClient client;

    @BeforeAll
    void setup(CsmrDockerComposeExtension extension) {
        client = new CsmrProxyClient(extension.getProxyUrl());
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
    }

    /**
     * Unwrap the proxy envelope: outer result is a stringified JSON object carrying
     * "chain":[{"stage","output":<wrapped>}, ...]. Returns the parsed inner object.
     */
    private JsonNode unwrapChain(String response) throws Exception {
        assertNotNull(response, "proxy returned null body");
        JsonNode root = MAPPER.readTree(response);
        JsonNode resultNode = root.get("result");
        assertNotNull(resultNode, "response missing top-level 'result'. Body: " + response);
        String innerJson = resultNode.isTextual() ? resultNode.asText() : resultNode.toString();
        return MAPPER.readTree(innerJson);
    }

    /** Reads the app_response.result int from a chain stage's wrapped output. */
    private Integer extractAppResult(JsonNode stageOutputNode, String stageName) {
        JsonNode inner = stageOutputNode.path("sidecar_response").path("app_response");
        if (inner.isMissingNode() || inner.isNull()) {
            fail("Stage " + stageName + " missing sidecar_response.app_response: " + stageOutputNode);
        }
        try {
            JsonNode appNode = inner.isTextual() ? MAPPER.readTree(inner.asText()) : inner;
            return appNode.path("result").asInt();
        } catch (Exception e) {
            fail("Could not parse app_response for stage " + stageName + ": " + inner + " — " + e.getMessage());
            return null;
        }
    }

    @Test
    @Order(1)
    @DisplayName("PoC2: chaining response carries intermediate stage results")
    void testChainIntermediate() throws Exception {
        String response = client.sendCommand("init_counter", Map.of("min", "0", "max", "100"));
        assertTrue(client.isDecided(response),
            "init_counter chain must be decided. Response: " + response);

        JsonNode inner = unwrapChain(response);
        JsonNode chain = inner.path("chain");
        assertTrue(chain.isArray() && chain.size() >= 2,
            "chain must carry >=2 intermediate stage results. Body: " + response);
    }

    @Test
    @Order(2)
    @DisplayName("PoC2: both PRNG.Generate and Counter.SetValue stages present")
    void testBothStagesPresent() throws Exception {
        String response = client.sendCommand("init_counter", Map.of("min", "0", "max", "100"));
        JsonNode inner = unwrapChain(response);

        boolean sawPrng = false, sawCounter = false;
        for (JsonNode stage : inner.path("chain")) {
            String name = stage.path("stage").asText();
            if ("prng_service.Generate".equals(name)) sawPrng = true;
            if ("counter_service.SetValue".equals(name)) sawCounter = true;
        }
        assertTrue(sawPrng, "PRNG stage (prng_service.Generate) missing from chain. Body: " + response);
        assertTrue(sawCounter, "Counter stage (counter_service.SetValue) missing from chain. Body: " + response);
    }

    @Test
    @Order(3)
    @DisplayName("PoC2: counter value equals PRNG-generated value (chain propagated)")
    void testValuePropagates() throws Exception {
        String response = client.sendCommand("init_counter", Map.of("min", "0", "max", "100"));
        JsonNode inner = unwrapChain(response);

        Integer prngVal = null, counterVal = null;
        for (JsonNode stage : inner.path("chain")) {
            String name = stage.path("stage").asText();
            if ("prng_service.Generate".equals(name)) {
                prngVal = extractAppResult(stage.path("output"), name);
            } else if ("counter_service.SetValue".equals(name)) {
                counterVal = extractAppResult(stage.path("output"), name);
            }
        }

        assertNotNull(prngVal, "PRNG stage result value was null");
        assertNotNull(counterVal, "Counter stage result value was null");
        assertEquals(prngVal, counterVal,
            "Counter value must equal the PRNG-generated value (chain propagated). "
                + "PRNG=" + prngVal + " Counter=" + counterVal);
    }

    @Test
    @Order(4)
    @DisplayName("PoC2: generated value within declared range [0,100]")
    void testRangeSanity() throws Exception {
        String response = client.sendCommand("init_counter", Map.of("min", "0", "max", "100"));
        JsonNode inner = unwrapChain(response);

        Integer prngVal = null;
        for (JsonNode stage : inner.path("chain")) {
            if ("prng_service.Generate".equals(stage.path("stage").asText())) {
                prngVal = extractAppResult(stage.path("output"), "prng_service.Generate");
                break;
            }
        }
        assertNotNull(prngVal, "PRNG value was null");
        assertTrue(prngVal >= 0 && prngVal <= 100,
            "Generated value must be within [0,100]. Got: " + prngVal);
    }
}
