/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageBuilder;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.common.Reconciliation;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:JavaNCSS"})
public class KafkaClusterResourcesTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final String NAMESPACE = "test";
    private static final String CLUSTER = "foo";
    private static final int REPLICAS = 3;
    private static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewZookeeper()
                    .withReplicas(REPLICAS)
                    .withNewPersistentClaimStorage()
                        .withSize("100Gi")
                    .endPersistentClaimStorage()
                .endZookeeper()
                .withNewKafka()
                    .withReplicas(REPLICAS)
                    .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("plain")
                                    .withPort(9092)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(false)
                                    .build(),
                            new GenericKafkaListenerBuilder()
                                    .withName("tls")
                                    .withPort(9093)
                                    .withType(KafkaListenerType.INTERNAL)
                                    .withTls(true)
                                    .build())
                    .withNewJbodStorage()
                        .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").withDeleteClaim(false).build())
                    .endJbodStorage()
                    .withConfig(Map.of("log.message.format.version", "3.0", "inter.broker.protocol.version", "3.0"))
                .endKafka()
            .endSpec()
            .build();

    private record ResourcesCombo(String cpuRequest, String cpuLimit, String memoryRequest, String memoryLimit, String error) { }
    
    private static List<ResourcesCombo> resourcesCombos() {
        return Arrays.asList(
            new ResourcesCombo("0.001", null, null, null, null),
            new ResourcesCombo("1m", null, null, null, null),
            new ResourcesCombo("1000m", "2000m", null, null, null),
            new ResourcesCombo("2000m", "2000m", null, null, null),
            new ResourcesCombo(null, "0.1", null, null, null),
            new ResourcesCombo("2000m", "1000m", null, null, "[.spec.kafka.resources cpu request must be <= limit]"),
            new ResourcesCombo("0m", "2000m", null, null, "[.spec.kafka.resources cpu request must be > zero]"),
            new ResourcesCombo("1000m", "0m", null, null, "[.spec.kafka.resources cpu limit must be > zero, .spec.kafka.resources cpu request must be <= limit]"),

            new ResourcesCombo(null, null, "128974848", null, null),
            new ResourcesCombo(null, null, "129e6", null, null),
            new ResourcesCombo(null, null, "129M", null, null),
            new ResourcesCombo(null, null, "128974848000m", null, null),
            new ResourcesCombo(null, null, "123Mi", null, null),
            new ResourcesCombo(null, null, "1Gi", "2Gi", null),
            new ResourcesCombo(null, null, "2Gi", "2Gi", null),
            new ResourcesCombo(null, null, null, "1", null),
            new ResourcesCombo(null, null, "2Gi", "1Gi", "[.spec.kafka.resources memory request must be <= limit]"),
            new ResourcesCombo(null, null, "0Gi", "2Gi", "[.spec.kafka.resources memory request must be > zero]"),
            new ResourcesCombo(null, null, "1Gi", "0Gi", "[.spec.kafka.resources memory limit must be > zero, .spec.kafka.resources memory request must be <= limit]"),

            new ResourcesCombo(null, null, null, null, null),
            new ResourcesCombo(String.valueOf(Long.MAX_VALUE), String.valueOf(Long.MAX_VALUE), String.valueOf(Long.MAX_VALUE), String.valueOf(Long.MAX_VALUE), null),
            new ResourcesCombo("0", "0", "0", "0", "[.spec.kafka.resources cpu request must be > zero, .spec.kafka.resources cpu limit must be > zero, "
                    + ".spec.kafka.resources memory request must be > zero, .spec.kafka.resources memory limit must be > zero]"),
            new ResourcesCombo("1000m", "0m", "1Gi", "0Gi", "[.spec.kafka.resources cpu limit must be > zero, .spec.kafka.resources cpu request must be <= limit, "
                    + ".spec.kafka.resources memory limit must be > zero, .spec.kafka.resources memory request must be <= limit]"),
            new ResourcesCombo("-1", "-1", "-1", "-1", "[.spec.kafka.resources cpu request must be > zero, .spec.kafka.resources cpu limit must be > zero, " 
                        + ".spec.kafka.resources memory request must be > zero, .spec.kafka.resources memory limit must be > zero]")
        );
    }

    @ParameterizedTest
    @MethodSource("resourcesCombos")
    public void withComputeResources(ResourcesCombo combo) {
        ResourceRequirementsBuilder builder = new ResourceRequirementsBuilder();
        if (combo.cpuRequest() != null) {
            builder.addToRequests("cpu", new Quantity(combo.cpuRequest()));
        }
        if (combo.cpuLimit() != null) {
            builder.addToLimits("cpu", new Quantity(combo.cpuLimit()));
        }
        if (combo.memoryRequest() != null) {
            builder.addToRequests("memory", new Quantity(combo.memoryRequest()));
        }
        if (combo.memoryLimit() != null) { 
            builder.addToLimits("memory", new Quantity(combo.memoryLimit()));
        }

        Kafka kafka = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withResources(builder.build())
                    .endKafka()
                .endSpec()
                .build();

        if (combo.error() != null) {
            InvalidResourceException ex = assertThrows(InvalidResourceException.class, () -> KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS));
            assertThat(ex.getMessage(), equalTo(combo.error()));
        } else {
            KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafka, VERSIONS);
            assertThat(kc.getContainers(null).get(0), notNullValue());
        }   
    }
}
