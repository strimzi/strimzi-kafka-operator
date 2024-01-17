/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.SystemProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.joining;


/**
 * Utility for {@link JvmOptions} object.
 */
public final class JvmOptionUtils  {
    /**
     * Default JVM -Xms setting
     */
    static final String DEFAULT_JVM_XMS = "128M";
    /**
     * Set of JVM performance options to be prioritized in sorting.
     */
    private static final Set<String> JVM_PERFORMANCE_PRIORITY_OPTIONS = Set.of("UnlockDiagnosticVMOptions", "UnlockExperimentalVMOptions");

    private static final String X_MS = "-Xms";
    private static final String X_MX = "-Xmx";
    private static final String MAX_RAM_PERCENTAGE = "MaxRAMPercentage";
    private static final String INITIAL_RAM_PERCENTAGE = "InitialRAMPercentage";
    private static final List<String> XX_HEAP_OPTIONS = List.of(MAX_RAM_PERCENTAGE, INITIAL_RAM_PERCENTAGE);
    private static final String MEMORY = "memory";

    private JvmOptionUtils() {
        // static access only
    }

    /**
     * Get the set of JVM options, bringing the Java system properties as well, and fill corresponding Strimzi environment variables
     * in order to pass them to the running application on the command line
     *
     * @param envVars environment variables list to put the JVM options and Java system properties
     * @param jvmOptions JVM options
     */
    public static void javaOptions(List<EnvVar> envVars, JvmOptions jvmOptions) {
        var strimziJavaOpts = new StringBuilder();
        appendHeapOpts(strimziJavaOpts, jvmOptions);
        parseJvmPerformanceOptions(jvmOptions)
            .ifPresent(opts -> strimziJavaOpts.append(' ').append(opts));

        var optsTrim = strimziJavaOpts.toString().trim();
        if (!optsTrim.isEmpty()) {
            envVars.add(ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_STRIMZI_JAVA_OPTS, optsTrim));
        }

        jvmSystemProperties(envVars, jvmOptions);
    }

    /**
     * Appends Heap configurations to the String builder. Heap configurations include -Xms, -Xmx,
     * -XX:InitialRAMPercentage, and -XX:MaxRAMPercentage
     *
     * @param optsBuilder   String builder to add the heap options to
     * @param jvmOptions    JVM Options configured in the CR
     */
    private static void appendHeapOpts(StringBuilder optsBuilder, JvmOptions jvmOptions) {
        if (jvmOptions != null) {
            if (jvmOptions.getXms() != null)    {
                optsBuilder.append(X_MS).append(jvmOptions.getXms());
            }

            if (jvmOptions.getXmx() != null)    {
                optsBuilder.append(' ').append(X_MX).append(jvmOptions.getXmx());
            }

            if (jvmOptions.getXx() != null) {
                if (jvmOptions.getXx().get(INITIAL_RAM_PERCENTAGE) != null)   {
                    optsBuilder.append(' ').append("-XX:").append(INITIAL_RAM_PERCENTAGE).append("=").append(jvmOptions.getXx().get(INITIAL_RAM_PERCENTAGE));
                }

                if (jvmOptions.getXx().get(MAX_RAM_PERCENTAGE) != null)   {
                    optsBuilder.append(' ').append("-XX:").append(MAX_RAM_PERCENTAGE).append("=").append(jvmOptions.getXx().get(MAX_RAM_PERCENTAGE));
                }
            }
        }
    }

    /**
     * Adds the STRIMZI_JAVA_SYSTEM_PROPERTIES variable to the EnvVar list if any system properties were specified
     * through the provided JVM options
     *
     * @param envVars list of the Environment Variables to add to
     * @param jvmOptions JVM options
     */
    public static void jvmSystemProperties(List<EnvVar> envVars, JvmOptions jvmOptions) {
        if (jvmOptions != null) {
            var jvmSystemPropertiesString = getJavaSystemPropertiesToString(jvmOptions.getJavaSystemProperties());
            if (jvmSystemPropertiesString != null && !jvmSystemPropertiesString.isEmpty()) {
                envVars.add(ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES, jvmSystemPropertiesString));
            }
        }
    }

    private static String getJavaSystemPropertiesToString(List<SystemProperty> javaSystemProperties) {
        if (javaSystemProperties == null) {
            return null;
        }
        List<String> javaSystemPropertiesList = new ArrayList<>(javaSystemProperties.size());
        for (SystemProperty property: javaSystemProperties) {
            javaSystemPropertiesList.add("-D" + property.getName() + "=" + property.getValue());
        }
        return String.join(" ", javaSystemPropertiesList);
    }

    /**
     * Adds the KAFKA_JVM_PERFORMANCE_OPTS variable to the EnvVar list if any performance related options were specified
     * through the provided JVM options
     *
     * @param envVars list of the Environment Variables to add to
     * @param jvmOptions JVM options
     */
    public static void jvmPerformanceOptions(List<EnvVar> envVars, JvmOptions jvmOptions) {
        parseJvmPerformanceOptions(jvmOptions)
            .map(envVar -> ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS, envVar))
            .ifPresent(envVars::add);
    }

    /**
     * Parses JVM performance options from {@link JvmOptions#getXx()} property into a space-separated string of JVM flags.
     * If performance options object is null or XX options are not configured then {@link Optional#empty()} will be returned.
     *
     * @param jvmOptions JVM options to parse
     * @return optional comma-separated string of JVM flags
     */
    private static Optional<String> parseJvmPerformanceOptions(JvmOptions jvmOptions) {
        return Optional.ofNullable(jvmOptions)
            .map(JvmOptions::getXx)
            .map(Map::entrySet)
            .map(entrySet -> entrySet.stream()
                    // We filter out the XX options which should be passed to HEAP options and not to Performance options in Kafka
                    .filter(entry -> !XX_HEAP_OPTIONS.contains(entry.getKey()))
                    .sorted(JvmOptionUtils::compareJvmPerformanceOption)
                    .map(JvmOptionUtils::toJvmPerformanceFlag)
                    .collect(joining(" ")))
            .filter(envVar -> !envVar.isEmpty());
    }

    /**
     * Compares two JVM option for sorting. The options are sorted by priority or if not prioritized by name.
     * The prioritized options are placed at the beginning to ensure correctness of JVM flags.
     * For example `-XX:+UnlockDiagnosticVMOptions` flag must be entered before `-XX:+PrintNMTStatistics`.
     *
     * @param jvmOption1 first JVM option to compare with key holding flag name placed in the key
     * @param jvmOption2 second JVM option to compare with key holding flag name placed in the key
     * @return comparison result [-1, 0, 1]
     */
    private static int compareJvmPerformanceOption(Map.Entry<String, String> jvmOption1, Map.Entry<String, String> jvmOption2) {
        final var isJvmOption1Prioritized = isPriorityJvmPerformanceOption(jvmOption1);
        final var isJvmOption2Prioritized = isPriorityJvmPerformanceOption(jvmOption2);
        if (isJvmOption1Prioritized && isJvmOption2Prioritized) {
            return 0;
        }
        if (isJvmOption1Prioritized) {
            return -1;
        }
        if (isJvmOption2Prioritized) {
            return 1;
        }
        return jvmOption1.getKey().compareTo(jvmOption2.getKey());
    }

    /**
     * Checks whether the given JVM option should be prioritized or not.
     *
     * @param jvmOption JVM option to check
     * @return true - JVM option should be prioritized, false - otherwise
     */
    private static boolean isPriorityJvmPerformanceOption(Map.Entry<String, String> jvmOption)  {
        return JVM_PERFORMANCE_PRIORITY_OPTIONS.contains(jvmOption.getKey());
    }

    /**
     * Converts JVM option into a JVM flag.
     *
     * @param jvmOption JVM option to convert with flag name placed in the key
     * @return JVM flag e.g. -XX+PrintNMTStatistics
     */
    private static String toJvmPerformanceFlag(Map.Entry<String, String> jvmOption) {
        if ("true".equalsIgnoreCase(jvmOption.getValue())) {
            return "-XX:+" + jvmOption.getKey();
        }
        if ("false".equalsIgnoreCase(jvmOption.getValue())) {
            return "-XX:-" + jvmOption.getKey();
        }
        return "-XX:" + jvmOption.getKey() + "=" + jvmOption.getValue();
    }

    /**
     * Adds KAFKA_HEAP_OPTS variable to the EnvVar list if any heap related options were specified through the provided JVM options
     * If Xmx Java Options are not set STRIMZI_DYNAMIC_HEAP_PERCENTAGE and STRIMZI_DYNAMIC_HEAP_MAX may also be set by using the ResourceRequirements
     *
     * @param envVars list of the Environment Variables to add to
     * @param dynamicHeapPercentage value to set for the STRIMZI_DYNAMIC_HEAP_PERCENTAGE
     * @param dynamicHeapMaxBytes value to set for the STRIMZI_DYNAMIC_HEAP_MAX
     * @param jvmOptions JVM options
     * @param resources the resource requirements
     */
    public static void heapOptions(List<EnvVar> envVars, int dynamicHeapPercentage, long dynamicHeapMaxBytes, JvmOptions jvmOptions, ResourceRequirements resources) {
        if (dynamicHeapPercentage <= 0 || dynamicHeapPercentage > 100)  {
            throw new IllegalArgumentException("The Heap percentage " + dynamicHeapPercentage + " is invalid. It has to be >0 and <= 100.");
        }

        var kafkaHeapOpts = new StringBuilder();
        appendHeapOpts(kafkaHeapOpts, jvmOptions);

        if (needsDefaultHeapConfiguration(jvmOptions)) {
            // Get the resources => if requests are set, take request. If requests are not set, try limits
            if (hasMemoryRequestOrLimit(resources)) {
                // Delegate to the container to figure out only when CGroup memory limits are defined to prevent allocating
                // too much memory on the kubelet.
                envVars.add(ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE, Integer.toString(dynamicHeapPercentage)));
                if (dynamicHeapMaxBytes > 0) {
                    envVars.add(ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX, Long.toString(dynamicHeapMaxBytes)));
                }
            } else if (needsDefaultHeapXmsConfiguration(jvmOptions)) {
                // When no memory limit, `Xms`, and `Xmx` are defined then set a default `Xms` and
                // leave `Xmx` undefined.
                kafkaHeapOpts.append(X_MS).append(DEFAULT_JVM_XMS);
            }
        }

        var kafkaHeapOptsString = kafkaHeapOpts.toString().trim();
        if (!kafkaHeapOptsString.isEmpty()) {
            envVars.add(ContainerUtils.createEnvVar(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS, kafkaHeapOptsString));
        }
    }

    /**
     * Checks whether we need to add our own default Java heap configuration. If the user defines their own Xmx or
     * MaxRAMPercentage, we leave it to the user. But if both are missing, we use our own values.
     *
     * @param jvmOptions    JVM Options as configured by the user in the CR
     *
     * @return  True if our own custom configuration should be added. False otherwise.
     */
    private static boolean needsDefaultHeapConfiguration(JvmOptions jvmOptions)    {
        return jvmOptions == null || (jvmOptions.getXmx() == null && (jvmOptions.getXx() == null || jvmOptions.getXx().get("MaxRAMPercentage") == null));
    }

    /**
     * Checks whether we need to add our own default Java Xms heap configuration. If the user defines their own Xms or
     * InitialRAMPercentage, we leave it to the user. But if both are missing, we use our own default for the Xms option.
     *
     * @param jvmOptions    JVM Options as configured by the user in the CR
     *
     * @return  True if our own custom Xms configuration should be added. False otherwise.
     */
    private static boolean needsDefaultHeapXmsConfiguration(JvmOptions jvmOptions)    {
        return jvmOptions == null || (jvmOptions.getXms() == null && (jvmOptions.getXx() == null || jvmOptions.getXx().get("InitialRAMPercentage") == null));
    }

    private static boolean hasMemoryRequestOrLimit(ResourceRequirements resources) {
        if (resources == null) {
            return false;
        }
        return Optional.ofNullable(resources.getRequests()).map(it -> it.get(MEMORY))
            .or(() -> Optional.ofNullable(resources.getLimits()).map(it -> it.get(MEMORY)))
            .isPresent();
    }
}
