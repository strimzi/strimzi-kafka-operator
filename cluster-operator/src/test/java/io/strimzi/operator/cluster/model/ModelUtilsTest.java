/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.JvmOptions;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.StrimziPodSetBuilder;
import io.strimzi.api.kafka.model.SystemProperty;
import io.strimzi.api.kafka.model.storage.EphemeralStorageBuilder;
import io.strimzi.api.kafka.model.storage.JbodStorageBuilder;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.test.annotations.ParallelParameterizedTest;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.operator.common.Util.parseMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static java.util.Collections.emptyList;


@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling"})
@ParallelSuite
class ModelUtilsTest {
    @ParallelTest
    void testParseImageMap() {
        Map<String, String> m = parseMap(
                KafkaVersionTestUtils.LATEST_KAFKA_VERSION + "=" + KafkaVersionTestUtils.LATEST_KAFKA_IMAGE + "\n  " +
                        KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION + "=" + KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE + "\n ");
        assertThat(m.size(), is(2));
        assertThat(m.get(KafkaVersionTestUtils.LATEST_KAFKA_VERSION), is(KafkaVersionTestUtils.LATEST_KAFKA_IMAGE));
        assertThat(m.get(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION), is(KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE));

        m = parseMap(
                KafkaVersionTestUtils.LATEST_KAFKA_VERSION + "=" + KafkaVersionTestUtils.LATEST_KAFKA_IMAGE + "," +
                KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION + "=" + KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE);
        assertThat(m.size(), is(2));
        assertThat(m.get(KafkaVersionTestUtils.LATEST_KAFKA_VERSION), is(KafkaVersionTestUtils.LATEST_KAFKA_IMAGE));
        assertThat(m.get(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION), is(KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE));
        assertThat(m.get(KafkaVersionTestUtils.PREVIOUS_KAFKA_VERSION), is(KafkaVersionTestUtils.PREVIOUS_KAFKA_IMAGE));
    }

    @Test
    void testAnnotationsOrLabelsImageMap() {
        Map<String, String> m = parseMap(" discovery.3scale.net=true");
        assertThat(m.size(), is(1));
        assertThat(m.get("discovery.3scale.net"), is("true"));

        m = parseMap(" discovery.3scale.net/scheme=http\n" +
                "        discovery.3scale.net/port=8080\n" +
                "        discovery.3scale.net/path=path/\n" +
                "        discovery.3scale.net/description-path=oapi/");
        assertThat(m.size(), is(4));
        assertThat(m.get("discovery.3scale.net/scheme"), is("http"));
        assertThat(m.get("discovery.3scale.net/port"), is("8080"));
        assertThat(m.get("discovery.3scale.net/path"), is("path/"));
        assertThat(m.get("discovery.3scale.net/description-path"), is("oapi/"));
    }

    @ParallelTest
    void testStorageSerializationAndDeserialization()    {
        Storage jbod = new JbodStorageBuilder().withVolumes(
                new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build(),
                new PersistentClaimStorageBuilder().withStorageClass("gp2-st1").withDeleteClaim(true).withId(1).withSize("1000Gi").build())
                .build();

        Storage ephemeral = new EphemeralStorageBuilder().build();

        Storage persistent = new PersistentClaimStorageBuilder().withStorageClass("gp2-ssd").withDeleteClaim(false).withId(0).withSize("100Gi").build();

        assertThat(ModelUtils.decodeStorageFromJson(ModelUtils.encodeStorageToJson(jbod)), is(jbod));
        assertThat(ModelUtils.decodeStorageFromJson(ModelUtils.encodeStorageToJson(ephemeral)), is(ephemeral));
        assertThat(ModelUtils.decodeStorageFromJson(ModelUtils.encodeStorageToJson(persistent)), is(persistent));
    }

    @ParallelTest
    void testExistingCertificatesDiffer()   {
        Secret defaultSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "Key1")
                .addToData("my-cluster-kafka-2.crt", "Certificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .build();

        Secret sameAsDefaultSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "Key1")
                .addToData("my-cluster-kafka-2.crt", "Certificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .build();

        Secret scaleDownSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .build();

        Secret scaleUpSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "Key1")
                .addToData("my-cluster-kafka-2.crt", "Certificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .addToData("my-cluster-kafka-3.crt", "Certificate3")
                .addToData("my-cluster-kafka-3.key", "Key3")
                .addToData("my-cluster-kafka-4.crt", "Certificate4")
                .addToData("my-cluster-kafka-4.key", "Key4")
                .build();

        Secret changedSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "NewKey1")
                .addToData("my-cluster-kafka-2.crt", "Certificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .build();

        Secret changedScaleUpSecret = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "Certificate0")
                .addToData("my-cluster-kafka-0.key", "Key0")
                .addToData("my-cluster-kafka-1.crt", "Certificate1")
                .addToData("my-cluster-kafka-1.key", "Key1")
                .addToData("my-cluster-kafka-2.crt", "NewCertificate2")
                .addToData("my-cluster-kafka-2.key", "Key2")
                .addToData("my-cluster-kafka-3.crt", "Certificate3")
                .addToData("my-cluster-kafka-3.key", "Key3")
                .addToData("my-cluster-kafka-4.crt", "Certificate4")
                .addToData("my-cluster-kafka-4.key", "Key4")
                .build();

        Secret changedScaleDownSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("my-secret")
                .endMetadata()
                .addToData("my-cluster-kafka-0.crt", "NewCertificate0")
                .addToData("my-cluster-kafka-0.key", "NewKey0")
                .build();

        assertThat(ModelUtils.doExistingCertificatesDiffer(defaultSecret, defaultSecret), is(false));
        assertThat(ModelUtils.doExistingCertificatesDiffer(defaultSecret, sameAsDefaultSecret), is(false));
        assertThat(ModelUtils.doExistingCertificatesDiffer(defaultSecret, scaleDownSecret), is(false));
        assertThat(ModelUtils.doExistingCertificatesDiffer(defaultSecret, scaleUpSecret), is(false));
        assertThat(ModelUtils.doExistingCertificatesDiffer(defaultSecret, changedSecret), is(true));
        assertThat(ModelUtils.doExistingCertificatesDiffer(defaultSecret, changedScaleUpSecret), is(true));
        assertThat(ModelUtils.doExistingCertificatesDiffer(defaultSecret, changedScaleDownSecret), is(true));
    }

    @ParallelTest
    void testCreateOwnerReference()   {
        Kafka owner = new KafkaBuilder()
                .withNewMetadata()
                    .withName("my-kafka")
                    .withUid("some-uid")
                .endMetadata()
                .build();

        OwnerReference ref = ModelUtils.createOwnerReference(owner, false);

        assertThat(ref.getApiVersion(), is(owner.getApiVersion()));
        assertThat(ref.getKind(), is(owner.getKind()));
        assertThat(ref.getName(), is(owner.getMetadata().getName()));
        assertThat(ref.getUid(), is(owner.getMetadata().getUid()));
        assertThat(ref.getBlockOwnerDeletion(), is(false));
        assertThat(ref.getController(), is(false));
    }

    @ParallelTest
    void testCreateControllerOwnerReference()   {
        StrimziPodSet owner = new StrimziPodSetBuilder()
                .withNewMetadata()
                    .withName("my-cluster-kafka")
                    .withUid("some-uid")
                .endMetadata()
                .build();

        OwnerReference ref = ModelUtils.createOwnerReference(owner, true);

        assertThat(ref.getApiVersion(), is(owner.getApiVersion()));
        assertThat(ref.getKind(), is(owner.getKind()));
        assertThat(ref.getName(), is(owner.getMetadata().getName()));
        assertThat(ref.getUid(), is(owner.getMetadata().getUid()));
        assertThat(ref.getBlockOwnerDeletion(), is(false));
        assertThat(ref.getController(), is(true));
    }

    @ParallelTest
    void testHasOwnerReference()    {
        OwnerReference owner = new OwnerReferenceBuilder()
                .withApiVersion("my-api")
                .withKind("my-kind")
                .withName("my-owner")
                .withUid("a02c09d8-a04f-469d-97ba-920720abe9b3")
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();

        OwnerReference otherOwner = new OwnerReferenceBuilder()
                .withApiVersion("my-other-api")
                .withKind("my-other-kind")
                .withName("my-other-owner")
                .withUid("3dfcd6b9-ad05-4277-8d13-147346fe1f70")
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();

        Pod pod = new PodBuilder()
                    .withNewMetadata()
                        .withName("my-pod")
                        .withNamespace("my-namespace")
                    .endMetadata()
                    .withNewSpec()
                        .withContainers(new ContainerBuilder()
                                .withName("busybox")
                                .withImage("busybox")
                                .withCommand("sleep", "3600")
                                .withImagePullPolicy("IfNotPresent")
                                .build())
                        .withRestartPolicy("Always")
                        .withTerminationGracePeriodSeconds(0L)
                    .endSpec()
                    .build();

        // No owner reference
        assertThat(ModelUtils.hasOwnerReference(pod, owner), is(false));

        // Only our owner reference
        pod.getMetadata().setOwnerReferences(List.of(owner));
        assertThat(ModelUtils.hasOwnerReference(pod, owner), is(true));

        // Other owner reference
        pod.getMetadata().setOwnerReferences(List.of(otherOwner));
        assertThat(ModelUtils.hasOwnerReference(pod, owner), is(false));

        // Multiple owner references
        pod.getMetadata().setOwnerReferences(List.of(otherOwner, owner));
        assertThat(ModelUtils.hasOwnerReference(pod, owner), is(true));
    }

    @ParallelTest
    void testInvalidHeapPercentage() {
        RuntimeException exception = assertThrows(RuntimeException.class, () -> ModelUtils.heapOptions(new ArrayList<>(), 0, 0, new JvmOptions(), new ResourceRequirements()));
        assertThat(exception.getMessage(), is("The Heap percentage 0 is invalid. It has to be >0 and <= 100."));

        exception = assertThrows(RuntimeException.class, () -> ModelUtils.heapOptions(new ArrayList<>(), 101, 0, new JvmOptions(), new ResourceRequirements()));
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
        assertThat(env.get(AbstractModel.ENV_VAR_KAFKA_HEAP_OPTS), is("-Xms" + ModelUtils.DEFAULT_JVM_XMS));
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
    void testServiceDnsNames() {
        List<String> dnsNames = ModelUtils.generateAllServiceDnsNames("my-namespace", "my-service");

        assertThat(dnsNames.size(), is(4));
        assertThat(dnsNames, hasItems("my-service", "my-service.my-namespace", "my-service.my-namespace.svc", "my-service.my-namespace.svc.cluster.local"));
    }

    @ParallelParameterizedTest
    @CsvSource(value = {
        // XX options                                              | Expected JVM performance opts
        //---------------------------------------------------------+---------------------------------------------------------------------
        "null                                                      | null",
        "Z:false X:foo C:TRUE a:bar UnlockDiagnosticVMOptions:true | -XX:+UnlockDiagnosticVMOptions -XX:+C -XX:X=foo -XX:-Z -XX:a=bar",
        "c:1 b:2 a:3 z:False x:True                                | -XX:a=3 -XX:b=2 -XX:c=1 -XX:+x -XX:-z"
    }, delimiterString = "|", nullValues = "null")
    void testThatJavaOptionsJavaPerformanceOptsEnvVariableFromJavaOptions(String xx, String expected) {
        // given
        var jvmOptions = new JvmOptions();
        jvmOptions.setXx(toMap(xx));
        var envVars = new ArrayList<EnvVar>();

        // when
        ModelUtils.jvmPerformanceOptions(envVars, jvmOptions);

        // then
        var expectedEnvVars = expected != null ? List.of(new EnvVarBuilder()
            .withName(AbstractModel.ENV_VAR_KAFKA_JVM_PERFORMANCE_OPTS)
            .withValue(expected)
            .build()) : emptyList();
        assertThat(envVars, equalTo(expectedEnvVars));
    }

    @ParallelTest
    void testThatJavaPerformanceOptsEnvVariableIsNotAppendedOnNullJavaOptions() {
        // given
        var envVars = new ArrayList<EnvVar>();

        // when
        ModelUtils.jvmPerformanceOptions(envVars, null);

        // then
        assertThat(envVars, is(emptyList()));
    }
    
    @ParallelParameterizedTest
    @CsvSource(value = {
        // ms | mx   | xx                                      | expected Java Opts
        //----+------+-----------------------------------------+-------------------------------------------------------------
        "null | null | null                                    | null",
        "null | 512m | null                                    | -Xmx512m",
        "64m  | null | null                                    | -Xms64m",
        "64m  | 512m | null                                    | -Xms64m -Xmx512m",
        "64m  | 512m | foo:bar UnlockDiagnosticVMOptions:false | -Xms64m -Xmx512m -XX:-UnlockDiagnosticVMOptions -XX:foo=bar",
        "64m  | 512m | test:true                               | -Xms64m -Xmx512m -XX:+test",
        "null | null | null                                    | null"
    }, delimiterString = "|", nullValues = "null")
    void testThatStrimziJavaOptsEnvVariableIsAppendedFromJavaOptions(String ms, String mx,  String xx,  String expectedJavaOpts) {
        // given
        var jvmOptions = new JvmOptions();
        jvmOptions.setXms(ms);
        jvmOptions.setXmx(mx);
        jvmOptions.setXx(toMap(xx));
        var envVars = new ArrayList<EnvVar>();
        
        // when
        ModelUtils.javaOptions(envVars, jvmOptions);

        // then
        var expectedEnvVars = new ArrayList<EnvVar>();
        Optional.ofNullable(expectedJavaOpts)
            .map(it -> new EnvVarBuilder().withName(AbstractModel.ENV_VAR_STRIMZI_JAVA_OPTS).withValue(it).build())
            .ifPresent(expectedEnvVars::add);
        assertThat(envVars, equalTo(expectedEnvVars));
    }

    @ParallelParameterizedTest
    @CsvSource(value = {
        // system properties       | expected System Properties
        //-------------------------+--------------------------------
        "null                      | null",
        "prop:value                | -Dprop=value",
        "prop1:value1 prop2:value2 | -Dprop1=value1 -Dprop2=value2"
    }, delimiterString = "|", nullValues = "null")
    void testThatStrimziJavaSystemPropertiesEnvVariableIsAppendedFromJavaOptions(String systemProperties,  String expectedSystemProperties) {
        // given
        var jvmOptions = new JvmOptions();
        jvmOptions.setJavaSystemProperties(toSystemProperties(systemProperties));
        var envVars = new ArrayList<EnvVar>();

        // when
        ModelUtils.javaOptions(envVars, jvmOptions);

        // then
        var expectedEnvVars = new ArrayList<EnvVar>();
        Optional.ofNullable(expectedSystemProperties)
            .map(it -> new EnvVarBuilder().withName(AbstractModel.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES).withValue(it).build())
            .ifPresent(expectedEnvVars::add);
        assertThat(envVars, equalTo(expectedEnvVars));
    }

    @ParallelTest
    void testThatStrimziJavaOptsAndStrimziJavaSystemPropertiesEnvVariablesAreNotAppendedOnNullJavaOptions()  {
        // given
        var envVars = new ArrayList<EnvVar>();

        // when
        ModelUtils.javaOptions(envVars, null);

        // then
        assertThat(envVars, is(emptyList()));
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

        ModelUtils.heapOptions(envVars, dynamicPercentage, dynamicMax, jvmOpts, resources);

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
     * Utility parsing string input into a hash map.
     * The entries must be separated with whitespaces whereas key/values with `:` character.
     * If given string is null then null hash map will be returned.
     *
     * @param string string to convert into map
     * @return converted map
     */
    private static Map<String, String> toMap(final String string)  {
        return string != null ? Stream.of(string.split("\s"))
            .map(it -> it.split(":"))
            .collect(Collectors.toMap(it -> it[0], it -> it[1])) : null;
    }

    /**
     * Utility parsing string input into list of system properties.
     * The entries must be separated with whitespaces  whereas property name/value with `:` character.
     * If given string is null then null list will be returned.
     * The properties
     *
     * @param string string to convert into system properties list
     * @return converted list of system properties
     */
    private static List<SystemProperty> toSystemProperties(final String string) {
        if (string == null)  {
            return null;
        }
        final var systemProperties = new ArrayList<SystemProperty>();
        for (var entry: string.split("\s")) {
            var nameValue = entry.split(":");
            var systemProperty = new SystemProperty();
            systemProperty.setName(nameValue[0]);
            systemProperty.setValue(nameValue[1]);
            systemProperties.add(systemProperty);
        }
        return systemProperties;
    }
}
