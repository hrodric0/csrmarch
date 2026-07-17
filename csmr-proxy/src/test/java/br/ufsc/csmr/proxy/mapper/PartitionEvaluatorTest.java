/**
 * CSMR component.
 *
 * @author Rodrigo W. Bonatto (UFSC, 2026)
 * @see <a href="https://github.com/hrodric0/csmrarch">CSMR Project</a>
 */

package br.ufsc.csmr.proxy.mapper;

import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec.PartitionRule;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for hash-based sharding (Scenario 3).
 *
 * The regression guard: {@code Math.floorMod} must be used instead of
 * {@code Math.abs(hash) % mod}, because {@code Math.abs(Integer.MIN_VALUE)} is
 * still negative and would produce a negative shard index — mis-routing the
 * command. We verify every value lands on exactly one of the mod shards.
 */
class PartitionEvaluatorTest {

    private static final int MOD = 3;

    /** One rule per shard 0..MOD-1, each hashing on "key". */
    private static List<PartitionRule> shardRules() {
        List<PartitionRule> rules = new ArrayList<>();
        for (int shard = 0; shard < MOD; shard++) {
            PartitionRule r = new PartitionRule();
            r.setHashMod(MOD);
            r.setHashValue(shard);
            r.setTargetService("svc");
            r.setTargetMethod("shard" + shard);
            rules.add(r);
        }
        return rules;
    }

    @Test
    void everyValueMapsToExactlyOneShard() {
        PartitionEvaluator evaluator = new PartitionEvaluator();
        List<PartitionRule> rules = shardRules();

        for (int i = 0; i < 1_000; i++) {
            String value = "object-" + i;
            PartitionRule match = evaluator.findMatchingRule(rules, "key", Map.of("key", value));
            assertNotNull(match, "no shard matched value '" + value + "' — negative index suspected");
            int shard = Math.floorMod(value.hashCode(), MOD);
            assertEquals("shard" + shard, match.getTargetMethod());
        }
    }

    @Test
    void valueWithIntegerMinValueHashStillShardsNonNegative() {
        // "polygenelubricants" is a well-known String whose hashCode() is
        // Integer.MIN_VALUE — the exact case Math.abs(...) % mod mishandles.
        String pathological = "polygenelubricants";
        assertEquals(Integer.MIN_VALUE, pathological.hashCode());

        PartitionEvaluator evaluator = new PartitionEvaluator();
        PartitionRule match =
                evaluator.findMatchingRule(shardRules(), "key", Map.of("key", pathological));

        assertNotNull(match, "Integer.MIN_VALUE hash produced no match — negative shard index");
        int shard = Math.floorMod(Integer.MIN_VALUE, MOD);
        assertTrue(shard >= 0 && shard < MOD);
        assertEquals("shard" + shard, match.getTargetMethod());
    }

    @Test
    void missingPartitionParameterReturnsNull() {
        PartitionEvaluator evaluator = new PartitionEvaluator();
        assertNull(evaluator.findMatchingRule(shardRules(), "key", Map.of("other", "x")));
    }
}
