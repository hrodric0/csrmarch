package br.ufsc.csmr.proxy.mapper;

import br.ufsc.csmr.controlplane.operator.CsmrCompositionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import jakarta.annotation.PostConstruct;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Loads routing configuration from YAML files.
 *
 * Supports loading from:
 *   1. Classpath resources (default composition.yaml)
 *   2. External file path (via csmr.routing.yaml-path property)
 *   3. ZooKeeper (future: ZooKeeperRoutingLoader)
 *
 * The loaded YAML must follow the CsmrCompositionSpec structure, supporting
 * all 4 dissertation scenarios.
 */
@Component
public class YamlRoutingLoader {

    private static final Logger log = LoggerFactory.getLogger(YamlRoutingLoader.class);

    @Value("${csmr.routing.yaml-path:classpath:csmr-composition.yaml}")
    private String yamlPath;

    private CsmrCompositionSpec loadedSpec;

    @PostConstruct
    public void load() {
        Yaml yaml = new Yaml();

        // Resolve the input source first so we can use try-with-resources for the stream.
        InputStream resolved;
        try {
            if (yamlPath.startsWith("classpath:")) {
                String resourcePath = yamlPath.substring("classpath:".length());
                resolved = getClass().getClassLoader().getResourceAsStream(resourcePath);
                if (resolved == null) {
                    log.warn("Resource not found: {}. Using default PoC routing.", resourcePath);
                    return;
                }
                log.info("Loading routing configuration from classpath: {}", resourcePath);
            } else {
                Path path = Paths.get(yamlPath);
                if (!Files.exists(path)) {
                    log.warn("File not found: {}. Using default PoC routing.", yamlPath);
                    return;
                }
                resolved = new FileInputStream(path.toFile());
                log.info("Loading routing configuration from file: {}", yamlPath);
            }
        } catch (Exception e) {
            log.error("Could not open routing configuration '{}': {}. Using default PoC routing.",
                    yamlPath, e.getMessage(), e);
            return;
        }

        // try-with-resources guarantees the stream is closed even if parsing throws.
        try (InputStream inputStream = resolved) {
            Map<String, Object> yamlData = yaml.load(inputStream);
            loadedSpec = convertYamlToSpec(yamlData);

            log.info("Successfully loaded {} service(s) and {} composition rule(s).",
                    loadedSpec.getServices() != null ? loadedSpec.getServices().size() : 0,
                    loadedSpec.getCompositions() != null ? loadedSpec.getCompositions().size() : 0);

        } catch (Exception e) {
            // Loud fallback: silent default routing previously masked real config errors.
            loadedSpec = null;
            log.error("Failed to parse YAML routing configuration '{}': {}. " +
                            "Falling back to default PoC routing — composition rules from YAML will NOT apply.",
                    yamlPath, e.getMessage(), e);
        }
    }

    /**
     * Converts raw YAML data to CsmrCompositionSpec.
     * Handles the nested structure with operation definitions and routing targets.
     */
    @SuppressWarnings("unchecked")
    private CsmrCompositionSpec convertYamlToSpec(Map<String, Object> yamlData) {
        CsmrCompositionSpec spec = new CsmrCompositionSpec();

        Map<String, Object> specData = (Map<String, Object>) yamlData.get("spec");
        if (specData == null) {
            log.warn("No 'spec' section found in YAML");
            return spec;
        }

        // toleratedFailures
        spec.setToleratedFailures(getInt(specData, "toleratedFailures", 1));

        // services
        Map<String, Object> servicesData = (Map<String, Object>) specData.get("services");
        if (servicesData != null) {
            Map<String, CsmrCompositionSpec.ServiceDefinition> services = new LinkedHashMap<>();

            for (Map.Entry<String, Object> entry : servicesData.entrySet()) {
                String serviceName = entry.getKey();
                Map<String, Object> serviceData = (Map<String, Object>) entry.getValue();

                CsmrCompositionSpec.ServiceDefinition serviceDef = new CsmrCompositionSpec.ServiceDefinition();

                // addresses
                List<String> addresses = (List<String>) serviceData.get("addresses");
                serviceDef.setAddresses(addresses);

                // operations
                List<Map<String, Object>> operationsData = (List<Map<String, Object>>) serviceData.get("operations");
                if (operationsData != null) {
                    List<CsmrCompositionSpec.OperationDefinition> operations = new ArrayList<>();

                    for (Map<String, Object> opData : operationsData) {
                        CsmrCompositionSpec.OperationDefinition opDef = new CsmrCompositionSpec.OperationDefinition();
                        opDef.setMethod((String) opData.get("method"));

                        // params
                        List<Map<String, String>> paramsData = (List<Map<String, String>>) opData.get("params");
                        if (paramsData != null) {
                            List<CsmrCompositionSpec.ParameterDefinition> params = new ArrayList<>();
                            for (Map<String, String> paramData : paramsData) {
                                CsmrCompositionSpec.ParameterDefinition paramDef = new CsmrCompositionSpec.ParameterDefinition();
                                paramDef.setName(paramData.get("name"));
                                paramDef.setType(paramData.get("type"));
                                params.add(paramDef);
                            }
                            opDef.setParams(params);
                        }

                        opDef.setReturns((String) opData.get("returns"));
                        operations.add(opDef);
                    }

                    serviceDef.setOperations(operations);
                }

                services.put(serviceName, serviceDef);
            }

            spec.setServices(services);
        }

        // compositions
        List<Map<String, Object>> compositionsData = (List<Map<String, Object>>) specData.get("compositions");
        if (compositionsData != null) {
            List<CsmrCompositionSpec.CompositionRule> compositions = new ArrayList<>();

            for (Map<String, Object> compData : compositionsData) {
                CsmrCompositionSpec.CompositionRule rule = new CsmrCompositionSpec.CompositionRule();

                rule.setName((String) compData.get("name"));
                rule.setType((String) compData.get("type"));
                rule.setMethod((String) compData.get("method"));
                rule.setServiceOperation((String) compData.get("service_operation"));
                rule.setInputMapper((String) compData.get("input_mapper"));
                rule.setOutputFunction((String) compData.get("output_function"));
                rule.setPartitionBy((String) compData.get("partition_by"));
                rule.setDeprecated(getBoolean(compData, "deprecated", false));
                rule.setRemovalReason((String) compData.get("removal_reason"));

                // partition_rules
                List<Map<String, Object>> partitionRulesData = (List<Map<String, Object>>) compData.get("partition_rules");
                if (partitionRulesData != null) {
                    List<CsmrCompositionSpec.PartitionRule> partitionRules = new ArrayList<>();

                    for (Map<String, Object> ruleData : partitionRulesData) {
                        CsmrCompositionSpec.PartitionRule partitionRule = new CsmrCompositionSpec.PartitionRule();
                        partitionRule.setKeyRange((String) ruleData.get("key_range"));
                        partitionRule.setTargetService((String) ruleData.get("target_service"));
                        partitionRule.setTargetMethod((String) ruleData.get("target_method"));

                        Object hashMod = ruleData.get("hash_mod");
                        if (hashMod instanceof Integer) {
                            partitionRule.setHashMod((Integer) hashMod);
                        } else if (hashMod instanceof String) {
                            partitionRule.setHashMod(Integer.parseInt((String) hashMod));
                        }

                        Object hashValue = ruleData.get("hash_value");
                        if (hashValue instanceof Integer) {
                            partitionRule.setHashValue((Integer) hashValue);
                        } else if (hashValue instanceof String) {
                            partitionRule.setHashValue(Integer.parseInt((String) hashValue));
                        }

                        partitionRule.setRegex((String) ruleData.get("regex"));
                        partitionRules.add(partitionRule);
                    }

                    rule.setPartitionRules(partitionRules);
                }

                // routing
                List<Map<String, Object>> routingData = (List<Map<String, Object>>) compData.get("routing");
                if (routingData != null) {
                    List<CsmrCompositionSpec.RoutingTarget> routing = new ArrayList<>();

                    for (Map<String, Object> routeData : routingData) {
                        CsmrCompositionSpec.RoutingTarget target = new CsmrCompositionSpec.RoutingTarget();
                        target.setTargetService((String) routeData.get("target_service"));
                        target.setTargetMethod((String) routeData.get("target_method"));
                        target.setEntryFormat((String) routeData.get("entry_format"));
                        routing.add(target);
                    }

                    rule.setRouting(routing);
                }

                // chaining_stages (PoC 2: automated feedback loops)
                rule.setReturnIntermediate(getBoolean(compData, "return_intermediate", false));

                List<Map<String, Object>> chainingStagesData =
                        (List<Map<String, Object>>) compData.get("chaining_stages");
                if (chainingStagesData != null) {
                    List<CsmrCompositionSpec.ChainingStage> stages = new ArrayList<>();

                    for (Map<String, Object> stageData : chainingStagesData) {
                        CsmrCompositionSpec.ChainingStage stage = new CsmrCompositionSpec.ChainingStage();
                        stage.setStageOrder(getInt(stageData, "stage_order", 0));
                        stage.setTargetService((String) stageData.get("target_service"));
                        stage.setTargetMethod((String) stageData.get("target_method"));
                        stage.setOutputField((String) stageData.get("output_field"));
                        stage.setInputMapper((String) stageData.get("input_mapper"));
                        stage.setWaitForCompletion(getBoolean(stageData, "wait_for_completion", true));
                        stages.add(stage);
                    }

                    rule.setChainingStages(stages);
                }

                compositions.add(rule);
            }

            spec.setCompositions(compositions);
        }

        return spec;
    }

    public CsmrCompositionSpec getLoadedSpec() {
        return loadedSpec;
    }

    private int getInt(Map<String, Object> map, String key, int defaultValue) {
        Object value = map.get(key);
        if (value == null) return defaultValue;
        if (value instanceof Number) return ((Number) value).intValue();
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private boolean getBoolean(Map<String, Object> map, String key, boolean defaultValue) {
        Object value = map.get(key);
        if (value == null) return defaultValue;
        if (value instanceof Boolean) return (Boolean) value;
        return Boolean.parseBoolean(value.toString());
    }
}