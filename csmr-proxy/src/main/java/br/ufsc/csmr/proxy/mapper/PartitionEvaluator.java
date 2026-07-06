package br.ufsc.csmr.proxy.mapper;

import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec.PartitionRule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Evaluates partition rules for Scenario 3 (Argument Partition / Sharding).
 *
 * Supports multiple partitioning strategies:
 *   1. key_range: Alphabetic range (e.g., "a-m", "n-z")
 *   2. hash_mod: Consistent hashing (e.g., hash(key) % 3 == 0)
 *   3. regex: Pattern matching on the partition parameter
 *
 * Formal definition (Alves 2026, Section 5.2):
 *   Given operation x with parameter p, partition evaluation selects the ring:
 *     R(x) = { ring ∈ {R₁, R₂, ..., Rₙ} : partition_rule(ring, p) }
 *
 * The first matching rule determines the target ring.
 */
@Component
public class PartitionEvaluator {

    private static final Logger log = LoggerFactory.getLogger(PartitionEvaluator.class);

    /**
     * Finds the matching partition rule for a given operation and parameters.
     *
     * @param partitionRules List of partition rules from the YAML composition
     * @param partitionBy    Parameter name to partition on (e.g., "key")
     * @param params         Client input parameters
     * @return               First matching PartitionRule, or null if no match
     */
    public PartitionRule findMatchingRule(
            List<PartitionRule> partitionRules,
            String partitionBy,
            Map<String, String> params) {

        if (partitionRules == null || partitionRules.isEmpty()) {
            log.warn("No partition rules provided for operation");
            return null;
        }

        String partitionValue = params.get(partitionBy);
        if (partitionValue == null) {
            log.warn("Partition parameter '{}' not found in request params", partitionBy);
            return null;
        }

        for (PartitionRule rule : partitionRules) {
            if (matchesRule(rule, partitionValue)) {
                log.info("Partition match: value '{}' matched rule targeting {}/{}",
                        partitionValue, rule.getTargetService(), rule.getTargetMethod());
                return rule;
            }
        }

        log.warn("No partition rule matched for value '{}'", partitionValue);
        return null;
    }

    /**
     * Evaluates whether a partition value matches a rule.
     */
    private boolean matchesRule(PartitionRule rule, String value) {
        // Priority 1: Hash-based partitioning
        if (rule.getHashMod() != null && rule.getHashValue() != null) {
            return matchesHash(rule, value);
        }

        // Priority 2: Regex matching
        if (rule.getRegex() != null && !rule.getRegex().isEmpty()) {
            return matchesRegex(rule, value);
        }

        // Priority 3: Key range (alphabetic)
        if (rule.getKeyRange() != null && !rule.getKeyRange().isEmpty()) {
            return matchesKeyRange(rule, value);
        }

        return false;
    }

    /**
     * Hash-based partitioning: hash(key) % mod == value
     */
    private boolean matchesHash(PartitionRule rule, String value) {
        int hash = Objects.hashCode(value);
        int mod = rule.getHashMod();
        int expected = rule.getHashValue();

        // floorMod (not Math.abs % mod): Math.abs(Integer.MIN_VALUE) is still
        // negative, which would yield a negative shard index and mis-route.
        int result = Math.floorMod(hash, mod);
        boolean matches = result == expected;

        log.debug("Hash partition: hash('{}')={} % {} = {}, expected={}, matches={}",
                value, hash, mod, result, expected, matches);

        return matches;
    }

    /**
     * Regex-based partitioning.
     */
    private boolean matchesRegex(PartitionRule rule, String value) {
        try {
            boolean matches = value.matches(rule.getRegex());
            log.debug("Regex partition: '{}' matches pattern '{}' = {}", value, rule.getRegex(), matches);
            return matches;
        } catch (Exception e) {
            log.error("Invalid regex pattern: {}", rule.getRegex(), e);
            return false;
        }
    }

    /**
     * Alphabetic key range partitioning (e.g., "a-m", "n-z").
     * Supports both lowercase and uppercase.
     */
    private boolean matchesKeyRange(PartitionRule rule, String value) {
        if (value.isEmpty()) {
            return false;
        }

        char firstChar = Character.toLowerCase(value.charAt(0));

        // Parse range like "a-m" or "n-z"
        String range = rule.getKeyRange().toLowerCase();
        String[] bounds = range.split("-");

        if (bounds.length != 2) {
            log.warn("Invalid key range format: {}", rule.getKeyRange());
            return false;
        }

        char lower = bounds[0].charAt(0);
        char upper = bounds[1].charAt(0);

        boolean matches = firstChar >= lower && firstChar <= upper;

        log.debug("Key range partition: '{}' ({}) in range '{}' ({}-{}) = {}",
                value, firstChar, range, lower, upper, matches);

        return matches;
    }
}