package br.ufsc.csmr.controlplane.operator;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 * Java representation of the CsmrComposition CRD spec.
 *
 * Maps to the YAML declarative API supporting all 4 dissertation scenarios:
 *   1. Adding Operations (ExposeLockAcquire)
 *   2. Extending Semantics (GetWithLogging - requires input_mapper + output_function)
 *   3. Argument Partition (PartitionedKVS - key_range routing)
 *   4. Removing Operations (RemoveRSA)
 *
 * Example YAML:
 *
 * apiVersion: csmr.ufsc.br/v1alpha1
 * kind: CsmrComposition
 * spec:
 *   toleratedFailures: 1
 *   services:
 *     kv_store:
 *       addresses: ["node3:4000", "node4:4001", "node5:4002"]
 *       operations:
 *         - method: "Get"
 *           params: [{name: "key", type: "string"}]
 *           returns: "string"
 *         - method: "Put"
 *           params: [{name: "key", type: "string"}, {name: "value", type: "string"}]
 *           returns: "void"
 *     lock_service:
 *       addresses: ["node6:6000", "node7:6001"]
 *       operations:
 *         - method: "Acquire"
 *           params: [{name: "lockName", type: "string"}]
 *           returns: "bool"
 *   compositions:
 *     - name: "PutWithLogging"
 *       type: "Composition"
 *       method: "Put"
 *       input_mapper: "PutInputMapper"
 *       output_function: "KvsOutputFunction"
 *       routing:
 *         - target_service: "kv_store"
 *           target_method: "Put"
 *         - target_service: "logger"
 *           target_method: "Append"
 *           entry_format: "put({key},{value})"
 *     - name: "PartitionedKVS"
 *       type: "Partition"
 *       method: "Put"
 *       partition_by: "key"
 *       partition_rules:
 *         - key_range: "a-m"
 *           target_service: "kv_store_ring1"
 *           target_method: "Put"
 *         - key_range: "n-z"
 *           target_service: "kv_store_ring2"
 *           target_method: "Put"
 */
public class CsmrCompositionSpec {

    /** f in the formal model: number of crash failures each service must tolerate. */
    @JsonProperty("toleratedFailures")
    private int toleratedFailures = 1;

    /** Map of service name → service definition. */
    @JsonProperty("services")
    private Map<String, ServiceDefinition> services;

    /** List of composition rules supporting all 4 scenarios. */
    @JsonProperty("compositions")
    private List<CompositionRule> compositions;

    // ─── Getters / Setters ────────────────────────────────────────────────────

    public int getToleratedFailures() { return toleratedFailures; }
    public void setToleratedFailures(int toleratedFailures) { this.toleratedFailures = toleratedFailures; }

    public Map<String, ServiceDefinition> getServices() { return services; }
    public void setServices(Map<String, ServiceDefinition> services) { this.services = services; }

    public List<CompositionRule> getCompositions() { return compositions; }
    public void setCompositions(List<CompositionRule> compositions) { this.compositions = compositions; }

    // ─── Nested DTOs ──────────────────────────────────────────────────────────

    /**
     * Service definition with detailed operation signatures.
     * Supports Scenario 1 (Adding Operations) by defining new services.
     * Supports Scenario 4 (Removing Operations) by excluding operations from compositions.
     */
    public static class ServiceDefinition {
        @JsonProperty("addresses")
        private List<String> addresses;

        @JsonProperty("operations")
        private List<OperationDefinition> operations;

        public List<String> getAddresses() { return addresses; }
        public void setAddresses(List<String> addresses) { this.addresses = addresses; }

        public List<OperationDefinition> getOperations() { return operations; }
        public void setOperations(List<OperationDefinition> operations) { this.operations = operations; }
    }

    /**
     * Detailed operation definition with parameter types and return type.
     * Required for InputMapper validation and argument-based routing.
     */
    public static class OperationDefinition {
        @JsonProperty("method")
        private String method;

        @JsonProperty("params")
        private List<ParameterDefinition> params;

        @JsonProperty("returns")
        private String returns;

        public String getMethod() { return method; }
        public void setMethod(String method) { this.method = method; }

        public List<ParameterDefinition> getParams() { return params; }
        public void setParams(List<ParameterDefinition> params) { this.params = params; }

        public String getReturns() { return returns; }
        public void setReturns(String returns) { this.returns = returns; }
    }

    /** Parameter definition for operation signature validation. */
    public static class ParameterDefinition {
        @JsonProperty("name")
        private String name;

        @JsonProperty("type")
        private String type;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
    }

    /**
     * Composition rule supporting all 4 dissertation scenarios plus Chaining:
     *   - Addition:      Direct exposure of new operation (Scenario 1)
     *   - Composition:   Multi-ring execution with input_mapper + output_function (Scenario 2)
     *   - Partition:     Argument-based routing across rings (Scenario 3)
     *   - Removal:       Explicit exclusion via deprecated flag (Scenario 4)
     *   - Chaining:      Output of one operation becomes input to another (PoC 2)
     */
    public static class CompositionRule {
        @JsonProperty("name")
        private String name;

        /** Type: "Addition", "Composition", "Partition" */
        @JsonProperty("type")
        private String type;

        @JsonProperty("method")
        private String method;

        /** For Scenario 2: Java class implementing InputMapper interface */
        @JsonProperty("input_mapper")
        private String inputMapper;

        /** For Scenario 2: Java class implementing OutputFunctionPlugin interface */
        @JsonProperty("output_function")
        private String outputFunction;

        /** For Scenario 3: Parameter to partition by (e.g., "key") */
        @JsonProperty("partition_by")
        private String partitionBy;

        /** For Scenario 3: Argument-based routing rules */
        @JsonProperty("partition_rules")
        private List<PartitionRule> partitionRules;

        /** For Scenario 2: Routing targets with entry format transformation */
        @JsonProperty("routing")
        private List<RoutingTarget> routing;

        /** For Scenario 4: Flag to deprecate/remove operation */
        @JsonProperty("deprecated")
        private boolean deprecated = false;

        /** For Scenario 4: Reason for removal */
        @JsonProperty("removal_reason")
        private String removalReason;

        /** For Scenario 1: Service operation to expose directly (e.g., "lock_service.Acquire") */
        @JsonProperty("service_operation")
        private String serviceOperation;

        /** For PoC 2: ChainingSMR - list of chained stages */
        @JsonProperty("chaining_stages")
        private List<ChainingStage> chainingStages;

        /** For PoC 2: Whether the chain returns intermediate results */
        @JsonProperty("return_intermediate")
        private boolean returnIntermediate = false;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public String getMethod() { return method; }
        public void setMethod(String method) { this.method = method; }

        public String getInputMapper() { return inputMapper; }
        public void setInputMapper(String inputMapper) { this.inputMapper = inputMapper; }

        public String getOutputFunction() { return outputFunction; }
        public void setOutputFunction(String outputFunction) { this.outputFunction = outputFunction; }

        public String getPartitionBy() { return partitionBy; }
        public void setPartitionBy(String partitionBy) { this.partitionBy = partitionBy; }

        public List<PartitionRule> getPartitionRules() { return partitionRules; }
        public void setPartitionRules(List<PartitionRule> partitionRules) { this.partitionRules = partitionRules; }

        public List<RoutingTarget> getRouting() { return routing; }
        public void setRouting(List<RoutingTarget> routing) { this.routing = routing; }

        public boolean isDeprecated() { return deprecated; }
        public void setDeprecated(boolean deprecated) { this.deprecated = deprecated; }

        public String getRemovalReason() { return removalReason; }
        public void setRemovalReason(String removalReason) { this.removalReason = removalReason; }

        public String getServiceOperation() { return serviceOperation; }
        public void setServiceOperation(String serviceOperation) { this.serviceOperation = serviceOperation; }

        public List<ChainingStage> getChainingStages() { return chainingStages; }
        public void setChainingStages(List<ChainingStage> chainingStages) { this.chainingStages = chainingStages; }

        public boolean isReturnIntermediate() { return returnIntermediate; }
        public void setReturnIntermediate(boolean returnIntermediate) { this.returnIntermediate = returnIntermediate; }
    }

    /**
     * Single stage in a chained SMR workflow (PoC 2).
     *
     * Represents one step where the output from the previous stage
     * becomes the input to the current stage.
     *
     * Example: PRNG → Counter
     *   Stage 1: prng.generate() → randomNumber
     *   Stage 2: counter.setValue(randomNumber)
     */
    public static class ChainingStage {
        @JsonProperty("stage_order")
        private int stageOrder;  // Execution order (1, 2, 3, ...)

        @JsonProperty("target_service")
        private String targetService;

        @JsonProperty("target_method")
        private String targetMethod;

        @JsonProperty("output_field")
        private String outputField;  // Field to extract from previous output

        @JsonProperty("input_mapper")
        private String inputMapper;  // Mapper for transforming output to input

        @JsonProperty("wait_for_completion")
        private boolean waitForCompletion = true;

        public int getStageOrder() { return stageOrder; }
        public void setStageOrder(int stageOrder) { this.stageOrder = stageOrder; }

        public String getTargetService() { return targetService; }
        public void setTargetService(String targetService) { this.targetService = targetService; }

        public String getTargetMethod() { return targetMethod; }
        public void setTargetMethod(String targetMethod) { this.targetMethod = targetMethod; }

        public String getOutputField() { return outputField; }
        public void setOutputField(String outputField) { this.outputField = outputField; }

        public String getInputMapper() { return inputMapper; }
        public void setInputMapper(String inputMapper) { this.inputMapper = inputMapper; }

        public boolean isWaitForCompletion() { return waitForCompletion; }
        public void setWaitForCompletion(boolean waitForCompletion) { this.waitForCompletion = waitForCompletion; }
    }

    /**
     * Argument-based partition rule for Scenario 3.
     * Example: key_range "a-m" routes to ring1, "n-z" routes to ring2.
     */
    public static class PartitionRule {
        @JsonProperty("key_range")
        private String keyRange;  // Supports: "a-m", "n-z", "0-9", regex patterns

        @JsonProperty("target_service")
        private String targetService;

        @JsonProperty("target_method")
        private String targetMethod;

        @JsonProperty("hash_mod")
        private Integer hashMod;  // Optional: hash(key) % mod == value routes here

        @JsonProperty("hash_value")
        private Integer hashValue;

        @JsonProperty("regex")
        private String regex;  // Optional: regex pattern matching

        public String getKeyRange() { return keyRange; }
        public void setKeyRange(String keyRange) { this.keyRange = keyRange; }

        public String getTargetService() { return targetService; }
        public void setTargetService(String targetService) { this.targetService = targetService; }

        public String getTargetMethod() { return targetMethod; }
        public void setTargetMethod(String targetMethod) { this.targetMethod = targetMethod; }

        public Integer getHashMod() { return hashMod; }
        public void setHashMod(Integer hashMod) { this.hashMod = hashMod; }

        public Integer getHashValue() { return hashValue; }
        public void setHashValue(Integer hashValue) { this.hashValue = hashValue; }

        public String getRegex() { return regex; }
        public void setRegex(String regex) { this.regex = regex; }
    }

    /**
     * Routing target for Scenario 2 (Extending Operations).
     * Defines which service/method to route to and how to format the entry.
     */
    public static class RoutingTarget {
        @JsonProperty("target_service")
        private String targetService;

        @JsonProperty("target_method")
        private String targetMethod;

        /** Entry format template, e.g., "put({key},{value})" */
        @JsonProperty("entry_format")
        private String entryFormat;

        public String getTargetService() { return targetService; }
        public void setTargetService(String targetService) { this.targetService = targetService; }

        public String getTargetMethod() { return targetMethod; }
        public void setTargetMethod(String targetMethod) { this.targetMethod = targetMethod; }

        public String getEntryFormat() { return entryFormat; }
        public void setEntryFormat(String entryFormat) { this.entryFormat = entryFormat; }
    }
}