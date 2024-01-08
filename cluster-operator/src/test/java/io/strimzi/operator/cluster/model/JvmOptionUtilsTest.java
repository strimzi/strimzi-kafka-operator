/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.common.JvmOptions;
import io.strimzi.api.kafka.model.common.SystemProperty;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
class JvmOptionUtilsTest {
    @ParallelTest
    void testInvalidHeapPercentage() {
        RuntimeException exception = assertThrows(IllegalArgumentException.class, () -> JvmOptionUtils.heapOptions(new ArrayList<>(), 0, 0, new JvmOptions(), new ResourceRequirements()));
        assertThat(exception.getMessage(), is("The Heap percentage 0 is invalid. It has to be >0 and <= 100."));

        exception = assertThrows(IllegalArgumentException.class, () -> JvmOptionUtils.heapOptions(new ArrayList<>(), 101, 0, new JvmOptions(), new ResourceRequirements()));
        assertThat(exception.getMessage(), is("The Heap percentage 101 is invalid. It has to be >0 and <= 100."));
    }

    @ParallelTest
    void testValidHeapPercentage() {
        Map<String, String> envVars = heapOptions(null, 1, 0, new ResourceRequirementsBuilder().withLimits(Map.of("memory", new Quantity("1Gi"))).build());
        assertThat(envVars.size(), is(1));
        assertThat(envVars.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE), is("1"));

        envVars = heapOptions(null, 100, 0, new ResourceRequirementsBuilder().withLimits(Map.of("memory", new Quantity("1Gi"))).build());
        assertThat(envVars.size(), is(1));
        assertThat(envVars.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE), is("100"));
    }

    @ParallelTest
    void testJvmMemoryOptionsExplicit() {
        Map<String, String> env = heapOptions(jvmOptions("4", "4"), 50, 4_000_000_000L, null);
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-Xms4 -Xmx4"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is(nullValue()));
    }

    @ParallelTest
    void testJvmMemoryOptionsXmsOnly() {
        Map<String, String> env = heapOptions(jvmOptions(null, "4"), 50, 5_000_000_000L, null);
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-Xms4"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is(nullValue()));
    }

    @ParallelTest
    void testJvmMemoryOptionsXmxOnly() {
        Map<String, String> env = heapOptions(jvmOptions("4", null), 50, 5_000_000_000L, null);
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-Xmx4"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is(nullValue()));
    }

    @ParallelTest
    void testJvmMemoryOptionsDefaultWithNoMemoryLimitOrJvmOptions() {
        Map<String, String> env = heapOptions(jvmOptions(null, null), 50, 5_000_000_000L, null);
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-Xms" + JvmOptionUtils.DEFAULT_JVM_XMS));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is(nullValue()));
    }

    @ParallelTest
    void testJvmMemoryOptionsDefaultWithMemoryLimit() {
        Map<String, String> env = heapOptions(jvmOptions(null, "4"), 50, 5_000_000_000L, new ResourceRequirementsBuilder().withLimits(Map.of("memory", new Quantity("1Gi"))).build());
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-Xms4"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE), is("50"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is("5000000000"));
    }

    @ParallelTest
    void testJvmMemoryOptionsMemoryRequest() {
        Map<String, String> env = heapOptions(null, 70, 10_000_000_000L, new ResourceRequirementsBuilder().withRequests(Map.of("memory", new Quantity("1Gi"))).build());
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE), is("70"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is("10000000000"));
    }

    @ParallelTest
    void testJvmMemoryOptionsDefaultWithNoMemoryLimitButXXOptionsExist() {
        Map<String, String> env = heapOptions(jvmOptions(null, null, Map.of("Something", "80")), 50, 5_000_000_000L, null);
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-Xms" + JvmOptionUtils.DEFAULT_JVM_XMS));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is(nullValue()));
    }

    @ParallelTest
    void testJvmMemoryOptionsWithMaxAndInitialRAMPercentage() {
        Map<String, String> env = heapOptions(jvmOptions(null, null, Map.of("MaxRAMPercentage", "80", "InitialRAMPercentage", "50")), 50, 5_000_000_000L, null);
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-XX:InitialRAMPercentage=50 -XX:MaxRAMPercentage=80"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is(nullValue()));
    }

    @ParallelTest
    void testJvmMemoryOptionsWithInitialRAMPercentage() {
        Map<String, String> env = heapOptions(jvmOptions(null, null, Map.of("InitialRAMPercentage", "50")), 50, 5_000_000_000L, null);
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-XX:InitialRAMPercentage=50"));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_PERCENTAGE), is(nullValue()));
        assertThat(env.get(AbstractModel.ENV_VAR_DYNAMIC_HEAP_MAX), is(nullValue()));
    }

    @ParallelTest
    void testThatJavaPerformanceOptionsAreIgnoredOnNullJvmOptions() {
        // when
        var envVars = new ArrayList<EnvVar>();

        // when
        JvmOptionUtils.jvmPerformanceOptions(envVars, null);

        // then
        assertThat(envVars, is(emptyList()));
    }

    @ParallelTest
    void testThatJavaPerformanceOptionsAreIgnoredOnEmptyJvmOptions() {
        // when
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();

        // when
        JvmOptionUtils.jvmPerformanceOptions(envVars, jvmOptions);

        // then
        assertThat(envVars, is(emptyList()));
    }

    @ParallelTest
    void testThatJavaPerformanceOptionsAreIgnoredOnEmptyXxProperty() {
        // when
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setXx(emptyMap());

        // when
        JvmOptionUtils.jvmPerformanceOptions(envVars, jvmOptions);

        // then
        assertThat(envVars, is(emptyList()));
    }

    @ParallelTest
    void testThatJavaHeapXXOptionsAreIgnored() {
        // when
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setXx(Map.of("InitialRAMPercentage", "50", "MaxRAMPercentage", "80"));

        // when
        JvmOptionUtils.jvmPerformanceOptions(envVars, jvmOptions);

        // then
        assertThat(envVars, is(emptyList()));
    }

    @ParallelTest
    void testThatJavaPerformanceOptionsAreAddedToEnvVariable() {
        // when
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setXx(Map.of(
                "MaxRAMPercentage", "80",
                "b", "2",
                "a", "3",
                "z", "false",
                "x", "true",
                "y", "FALSE",
                "v", "tRuE"));

        // when
        JvmOptionUtils.jvmPerformanceOptions(envVars, jvmOptions);

        // then
        var expectedPerformanceOpts = new EnvVarBuilder()
            .withName(AbstractModel.ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS)
            .withValue("-XX:a=3 -XX:b=2 -XX:+v -XX:+x -XX:-y -XX:-z")
            .build();
        assertThat(envVars, equalTo(List.of(expectedPerformanceOpts)));
    }

    @ParallelTest
    void testThatUnlockDiagnosticVMOptionsPerformanceOptionIsAlwaysSetFirst() {
        // when
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setXx(Map.of(
            "a", "1",
            "b", "2",
            "c", "3",
            "d", "false",
            "e", "true",
            "UnlockDiagnosticVMOptions", "true",
            "z", "anything"));

        // when
        JvmOptionUtils.jvmPerformanceOptions(envVars, jvmOptions);

        // then
        var expectedPerformanceOpts = new EnvVarBuilder()
            .withName(AbstractModel.ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS)
            .withValue("-XX:+UnlockDiagnosticVMOptions -XX:a=1 -XX:b=2 -XX:c=3 -XX:-d -XX:+e -XX:z=anything")
            .build();
        assertThat(envVars, equalTo(List.of(expectedPerformanceOpts)));
    }

    @ParallelTest
    void testThatUnlockExperimentalVMOptionsPerformanceOptionIsAlwaysSetFirst() {
        // when
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setXx(Map.of(
                "a", "1",
                "b", "false",
                "c", "true",
                "UnlockExperimentalVMOptions", "true",
                "z", "anything"));

        // when
        JvmOptionUtils.jvmPerformanceOptions(envVars, jvmOptions);

        // then
        var expectedPerformanceOpts = new EnvVarBuilder()
                .withName(AbstractModel.ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS)
                .withValue("-XX:+UnlockExperimentalVMOptions -XX:a=1 -XX:-b -XX:+c -XX:z=anything")
                .build();
        assertThat(envVars, equalTo(List.of(expectedPerformanceOpts)));
    }

    @ParallelTest
    void testThatJavaOptionsAreIgnoredOnNullJvmOptions() {
        // given
        var envVars = new ArrayList<EnvVar>();

        // when
        JvmOptionUtils.javaOptions(envVars, null);

        // then
        assertThat(envVars, is(emptyList()));
    }

    @ParallelTest
    void testThatJavaOptionsAreIgnoredOnEmptyJvmOptions() {
        // given
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();

        // when
        JvmOptionUtils.javaOptions(envVars, jvmOptions);

        // then
        assertThat(envVars, is(emptyList()));
    }

    @ParallelTest
    void testThatJavaOptionsAreIgnoredOnEmptyXxProperty() {
        // given
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setXx(emptyMap());

        // when
        JvmOptionUtils.javaOptions(envVars, jvmOptions);

        // then
        assertThat(envVars, is(emptyList()));
    }

    @ParallelTest
    void testThatJavaOptionsWithHeapMinimumSizeIsAddedToEnvVariables() {
     // given
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setXms("64m");

        // when
        JvmOptionUtils.javaOptions(envVars, jvmOptions);

        // then
        var expectedJavaOpts = new EnvVarBuilder()
            .withName(AbstractModel.ENV_VAR_STRIMZI_JAVA_OPTS)
            .withValue("-Xms64m")
            .build();
        assertThat(envVars, equalTo(List.of(expectedJavaOpts)));
    }

    @ParallelTest
    void testThatJavaOptionsWithHeapMaximumSizeIsAddedToEnvVariables() {
     // given
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setXmx("1024m");

        // when
        JvmOptionUtils.javaOptions(envVars, jvmOptions);

        // then
        var expectedJavaOpts = new EnvVarBuilder()
            .withName(AbstractModel.ENV_VAR_STRIMZI_JAVA_OPTS)
            .withValue("-Xmx1024m")
            .build();
        assertThat(envVars, equalTo(List.of(expectedJavaOpts)));
    }

    @ParallelTest
    void testThatJavaOptionsWithHeapMinimumAndMaximumSizeIsAddedToEnvVariables() {
     // given
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setXms("32m");
        jvmOptions.setXmx("777m");

        // when
        JvmOptionUtils.javaOptions(envVars, jvmOptions);

        // then
        var expectedJavaOpts = new EnvVarBuilder()
            .withName(AbstractModel.ENV_VAR_STRIMZI_JAVA_OPTS)
            .withValue("-Xms32m -Xmx777m")
            .build();
        assertThat(envVars, equalTo(List.of(expectedJavaOpts)));
    }

    @ParallelTest
    void testThatJavaOptionsWithPerformanceOptionsIsAddedToEnvVariables() {
     // given
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setXx(Map.of(
            "a", "1",
            "c", "5",
            "d", "6",
            "UnlockDiagnosticVMOptions", "true"));

        // when
        JvmOptionUtils.javaOptions(envVars, jvmOptions);

        // then
        var expectedJavaOpts = new EnvVarBuilder()
            .withName(AbstractModel.ENV_VAR_STRIMZI_JAVA_OPTS)
            .withValue("-XX:+UnlockDiagnosticVMOptions -XX:a=1 -XX:c=5 -XX:d=6")
            .build();
        assertThat(envVars, equalTo(List.of(expectedJavaOpts)));
    }

    @ParallelTest
    void testThatJavaOptionsWithHeapAndPerformanceOptionsIsAddedToEnvVariables() {
     // given
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setXms("128m");
        jvmOptions.setXmx("512m");
        jvmOptions.setXx(Map.of(
            "x", "1",
            "z", "anything",
            "y", "true",
            "UnlockDiagnosticVMOptions", "false"));

        // when
        JvmOptionUtils.javaOptions(envVars, jvmOptions);

        // then
        var expectedJavaOpts = new EnvVarBuilder()
            .withName(AbstractModel.ENV_VAR_STRIMZI_JAVA_OPTS)
            .withValue("-Xms128m -Xmx512m -XX:-UnlockDiagnosticVMOptions -XX:x=1 -XX:+y -XX:z=anything")
            .build();
        assertThat(envVars, equalTo(List.of(expectedJavaOpts)));
    }

    @ParallelTest
    void testThatJavaOptionsWithSystemPropertiesIsAddedToEnvVariables() {
     // given
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setJavaSystemProperties(List.of(
            systemProperty("prop1", "foo"),
            systemProperty("prop2", "bar")));

        // when
        JvmOptionUtils.javaOptions(envVars, jvmOptions);

        // then
        var expectedJavaOpts = new EnvVarBuilder()
            .withName(AbstractModel.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES)
            .withValue("-Dprop1=foo -Dprop2=bar")
            .build();
        assertThat(envVars, equalTo(List.of(expectedJavaOpts)));
    }

    @ParallelTest
    void testThatJavaOptionsWithEmptySystemPropertiesIsIgnored() {
     // given
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setJavaSystemProperties(emptyList());

        // when
        JvmOptionUtils.javaOptions(envVars, jvmOptions);

        // then
        assertThat(envVars, is(emptyList()));
    }

    @ParallelTest
    void testThatJavaOptionsWithHeapPerformanceAndSystemPropertiesIsAddedToEnvVariables() {
     // given
        var envVars = new ArrayList<EnvVar>();
        var jvmOptions = new JvmOptions();
        jvmOptions.setXms("128m");
        jvmOptions.setXmx("512m");
        jvmOptions.setXx(Map.of(
            "x", "1",
            "z", "anything",
            "y", "true",
            "UnlockDiagnosticVMOptions", "false"));
        jvmOptions.setJavaSystemProperties(List.of(
            systemProperty("prop1", "foo"),
            systemProperty("prop2", "bar")));

        // when
        JvmOptionUtils.javaOptions(envVars, jvmOptions);

        // then
        var expectedJavaOpts = new EnvVarBuilder()
            .withName(AbstractModel.ENV_VAR_STRIMZI_JAVA_OPTS)
            .withValue("-Xms128m -Xmx512m -XX:-UnlockDiagnosticVMOptions -XX:x=1 -XX:+y -XX:z=anything")
            .build();
        var expectedSystemProperties = new EnvVarBuilder()
            .withName(AbstractModel.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES)
            .withValue("-Dprop1=foo -Dprop2=bar")
            .build();
        assertThat(envVars, equalTo(List.of(expectedJavaOpts, expectedSystemProperties)));
    }

    /**
     * Utility methods to get the heap options environment variables based on the given options
     *
     * @param jvmOpts           The JvmOptions configuration from the Strimzi model
     * @param dynamicPercentage The dynamic heap percentage
     * @param dynamicMax        The maximal heap
     * @param resources         The configured resources
     *
     * @return                  Map with the environment variables with their names as the keys of the map
     */
    private static Map<String, String> heapOptions(JvmOptions jvmOpts, int dynamicPercentage, long dynamicMax, ResourceRequirements resources)  {
        List<EnvVar> envVars = new ArrayList<>();

        JvmOptionUtils.heapOptions(envVars, dynamicPercentage, dynamicMax, jvmOpts, resources);

        return envVars.stream().collect(Collectors.toMap(EnvVar::getName, EnvVar::getValue));
    }

    /**
     * Utility method to create JvmOptions object.
     *
     * @param xmx   Configured -Xmx
     * @param xms   Configured -Xms
     *
     * @return      New JvmOptions object
     */
    private static JvmOptions jvmOptions(String xmx, String xms) {
        JvmOptions result = new JvmOptions();
        result.setXms(xms);
        result.setXmx(xmx);
        return result;
    }

    /**
     * Utility method to create JvmOptions object.
     *
     * @param xmx   Configures -Xmx
     * @param xms   Configures -Xms
     * @param xx    Configures -XX options
     *
     * @return      New JvmOptions object
     */
    private static JvmOptions jvmOptions(String xmx, String xms, Map<String, String> xx) {
        JvmOptions result = new JvmOptions();
        result.setXms(xms);
        result.setXmx(xmx);
        result.setXx(xx);
        return result;
    }

    /**
     * Utility method to create SystemProperty with given name and value.
     *
     * @param name name of the system property
     * @param value property value
     * @return SystemProperty
     */
    private static SystemProperty systemProperty(String name, String value) {
        var systemProperty = new SystemProperty();
        systemProperty.setName(name);
        systemProperty.setValue(value);
        return systemProperty;
    }
}
