/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class KafkaConverterTest {

    private final KafkaConverter kafkaConverter = new KafkaConverter();

    @Test
    public void testListenersToV1Beta2() {
        convertListenersToV1beta2("kafka.strimzi.io/v1beta1");
        convertListenersToV1beta2("kafka.strimzi.io/v1alpha1");
    }

    public void convertListenersToV1beta2(String fromApiVersion) {
        Kafka converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                    .withNewKafka()
                        .withNewListeners()
                            .withNewKafkaListeners()
                                .withNewPlain().endPlain()
                                .withNewTls().endTls()
                                .withNewKafkaListenerExternalLoadBalancer().endKafkaListenerExternalLoadBalancer()
                            .endKafkaListeners()
                        .endListeners()
                    .endKafka()
                .endSpec()
                .build();
        kafkaConverter.convertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNotNull(converted.getSpec().getKafka().getListeners());
        Assertions.assertNotNull(converted.getSpec().getKafka().getListeners().getGenericKafkaListeners());
        Assertions.assertEquals(3, converted.getSpec().getKafka().getListeners().getGenericKafkaListeners().size());
    }

    @Test
    public void testListenersToV1Beta1() {
        convertListenersToV1beta1("kafka.strimzi.io/v1beta2");
    }

    public void convertListenersToV1beta1(String fromApiVersion) {
        Kafka converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                .withNewKafka()
                .withNewListeners()
                .addNewGenericKafkaListener()
                .withName("tls")
                .withTls(true)
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .endGenericKafkaListener()
                .endListeners()
                .endKafka()
                .endSpec()
                .build();
        kafkaConverter.convertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNotNull(converted.getSpec().getKafka().getListeners());
        Assertions.assertNotNull(converted.getSpec().getKafka().getListeners().getGenericKafkaListeners());
        Assertions.assertEquals(1, converted.getSpec().getKafka().getListeners().getGenericKafkaListeners().size());
        Assertions.assertTrue(converted.getSpec().getKafka().getListeners().getGenericKafkaListeners().get(0).isTls());
    }


    @Test
    public void testKafkaTolerationToV1Beta2() {
        convertKafkaTolerationsToV1beta2("kafka.strimzi.io/v1beta1");
        convertKafkaTolerationsToV1beta2("kafka.strimzi.io/v1alpha1");
    }

    public void convertKafkaTolerationsToV1beta2(String fromApiVersion) {
        Kafka converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                .withNewKafka().withTolerations(new TolerationBuilder()
                        .withKey("foo")
                        .withEffect("dbdb")
                        .build())
                .endKafka()
                .endSpec()
                .build();
        kafkaConverter.convertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNull(converted.getSpec().getKafka().getTolerations());
        Assertions.assertEquals(1, converted.getSpec().getKafka().getTemplate().getPod().getTolerations().size());
        Assertions.assertEquals("foo", converted.getSpec().getKafka().getTemplate().getPod().getTolerations().get(0).getKey());
        Assertions.assertEquals("dbdb", converted.getSpec().getKafka().getTemplate().getPod().getTolerations().get(0).getEffect());
    }

    @Test
    public void testKafkaTolerationToV1Beta1() {
        convertKafkaTolerationsToV1beta1("kafka.strimzi.io/v1beta2");
    }

    public void convertKafkaTolerationsToV1beta1(String fromApiVersion) {
        Kafka converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                .withNewKafka().withNewTemplate().withNewPod().withTolerations(new TolerationBuilder()
                        .withKey("foo")
                        .withEffect("dbdb")
                        .build())
                .endPod()
                .endTemplate()
                .endKafka()
                .endSpec()
                .build();
        kafkaConverter.convertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNull(converted.getSpec().getKafka().getTolerations());
        Assertions.assertEquals(1, converted.getSpec().getKafka().getTemplate().getPod().getTolerations().size());
        Assertions.assertEquals("foo", converted.getSpec().getKafka().getTemplate().getPod().getTolerations().get(0).getKey());
        Assertions.assertEquals("dbdb", converted.getSpec().getKafka().getTemplate().getPod().getTolerations().get(0).getEffect());
    }


    @Test
    public void testZookeeperTolerationToV1Beta1() {
        convertZookeeperTolerationsToV1beta1("kafka.strimzi.io/v1beta2");
    }

    public void convertZookeeperTolerationsToV1beta1(String fromApiVersion) {
        Kafka converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                .withNewZookeeper().withNewTemplate().withNewPod().withTolerations(new TolerationBuilder()
                        .withKey("foo")
                        .withEffect("dbdb")
                        .build())
                .endPod()
                .endTemplate()
                .endZookeeper()
                .endSpec()
                .build();
        kafkaConverter.convertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNull(converted.getSpec().getZookeeper().getTolerations());
        Assertions.assertEquals(1, converted.getSpec().getZookeeper().getTemplate().getPod().getTolerations().size());
        Assertions.assertEquals("foo", converted.getSpec().getZookeeper().getTemplate().getPod().getTolerations().get(0).getKey());
        Assertions.assertEquals("dbdb", converted.getSpec().getZookeeper().getTemplate().getPod().getTolerations().get(0).getEffect());
    }


    @Test
    public void testZookeeperTolerationToV1Beta2() {
        convertZookeeperTolerationsToV1beta2("kafka.strimzi.io/v1beta1");
    }

    public void convertZookeeperTolerationsToV1beta2(String fromVpiVersion) {
        Kafka converted = new KafkaBuilder()
                .withApiVersion(fromVpiVersion)
                .withNewSpec()
                .withNewZookeeper().withTolerations(new TolerationBuilder()
                        .withKey("foo")
                        .withEffect("dbdb")
                        .build())
                .endZookeeper()
                .endSpec()
                .build();
        kafkaConverter.convertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNull(converted.getSpec().getZookeeper().getTolerations());
        Assertions.assertEquals(1, converted.getSpec().getZookeeper().getTemplate().getPod().getTolerations().size());
        Assertions.assertEquals("foo", converted.getSpec().getZookeeper().getTemplate().getPod().getTolerations().get(0).getKey());
        Assertions.assertEquals("dbdb", converted.getSpec().getZookeeper().getTemplate().getPod().getTolerations().get(0).getEffect());
    }

    @Test
    public void testKafkaAffinityToV1Beta2() {
        convertKafkaAffinityToV1beta2("kafka.strimzi.io/v1beta1");
        convertKafkaAffinityToV1beta2("kafka.strimzi.io/v1alpha1");
    }

    public void convertKafkaAffinityToV1beta2(String fromApiVersion) {
        Kafka converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                .withNewKafka().withAffinity(new AffinityBuilder()
                        .withNewNodeAffinity()
                        .addNewPreferredDuringSchedulingIgnoredDuringExecution()
                        .withWeight(100)
                        .endPreferredDuringSchedulingIgnoredDuringExecution()
                        .endNodeAffinity()
                        .build())
                .endKafka()
                .endSpec()
                .build();
        kafkaConverter.convertTo(converted,
                ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNull(converted.getSpec().getKafka().getAffinity());
        Assertions.assertNotNull(converted.getSpec().getKafka().getTemplate().getPod().getAffinity());
        Assertions.assertEquals(100, converted.getSpec().getKafka().getTemplate().getPod().getAffinity().getNodeAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(0).getWeight());
    }

    @Test
    public void testKafkaAffinityToV1Beta1() {
        convertKafkaAffinityToV1beta1();
    }

    public void convertKafkaAffinityToV1beta1() {
        Kafka converted = new KafkaBuilder()
                .withApiVersion("kafka.strimzi.io/v1beta2")
                .withNewSpec()
                .withNewKafka().withNewTemplate().withNewPod().withAffinity(new AffinityBuilder()
                        .withNewNodeAffinity()
                        .addNewPreferredDuringSchedulingIgnoredDuringExecution()
                        .withWeight(100)
                        .endPreferredDuringSchedulingIgnoredDuringExecution()
                        .endNodeAffinity()
                        .build()).endPod().endTemplate()
                .endKafka()
                .endSpec()
                .build();
        kafkaConverter.convertTo(converted,
                ApiVersion.V1BETA1);
        Assertions.assertEquals("kafka.strimzi.io/v1beta1", converted.getApiVersion());
        Assertions.assertNull(converted.getSpec().getKafka().getAffinity());
        Assertions.assertNotNull(converted.getSpec().getKafka().getTemplate().getPod().getAffinity());
        Assertions.assertEquals(100, converted.getSpec().getKafka().getTemplate().getPod().getAffinity().getNodeAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(0).getWeight());
    }

    @Test
    public void testZookeeperAffinityToV1Beta1() {
        convertKafkaAffinityToV1beta1();
    }

    public void convertZookeeperAffinityToV1beta1() {
        Kafka converted = new KafkaBuilder()
                .withApiVersion("kafka.strimzi.io/v1beta2")
                .withNewSpec()
                .withNewZookeeper().withNewTemplate().withNewPod().withAffinity(new AffinityBuilder()
                        .withNewNodeAffinity()
                        .addNewPreferredDuringSchedulingIgnoredDuringExecution()
                        .withWeight(100)
                        .endPreferredDuringSchedulingIgnoredDuringExecution()
                        .endNodeAffinity()
                        .build()).endPod().endTemplate()
                .endZookeeper()
                .endSpec()
                .build();
        kafkaConverter.convertTo(converted,
                ApiVersion.V1BETA1);
        Assertions.assertEquals("kafka.strimzi.io/v1beta1", converted.getApiVersion());
        Assertions.assertNull(converted.getSpec().getZookeeper().getAffinity());
        Assertions.assertNotNull(converted.getSpec().getZookeeper().getTemplate().getPod().getAffinity());
        Assertions.assertEquals(100, converted.getSpec().getZookeeper().getTemplate().getPod().getAffinity().getNodeAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(0).getWeight());
    }


    @Test
    public void testZookeeperAffinityToV1Beta2() {
        convertZookeeperAffinityToV1beta2("kafka.strimzi.io/v1beta1");
        convertZookeeperAffinityToV1beta2("kafka.strimzi.io/v1alpha1");
    }

    public void convertZookeeperAffinityToV1beta2(String fromApiVersion) {
        Kafka converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                .withNewZookeeper().withAffinity(new AffinityBuilder()
                        .withNewNodeAffinity()
                        .addNewPreferredDuringSchedulingIgnoredDuringExecution()
                        .withWeight(100)
                        .endPreferredDuringSchedulingIgnoredDuringExecution()
                        .endNodeAffinity()
                        .build())
                .endZookeeper()
                .endSpec()
                .build();
        kafkaConverter.convertTo(converted,
                ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNull(converted.getSpec().getZookeeper().getAffinity());
        Assertions.assertNotNull(converted.getSpec().getZookeeper().getTemplate().getPod().getAffinity());
        Assertions.assertEquals(100, converted.getSpec().getZookeeper().getTemplate().getPod().getAffinity().getNodeAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(0).getWeight());
    }

    @Test
    public void testKafkaTlsSidecarToV1Beta2() {
        convertKafkaTlsSidecarToV1beta2("kafka.strimzi.io/v1beta1");
        convertKafkaTlsSidecarToV1beta2("kafka.strimzi.io/v1alpha1");
    }

    public void convertKafkaTlsSidecarToV1beta2(String fromApiVersion) {
        Kafka converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                    .withNewKafka()
                        .withNewTlsSidecar()
                        .endTlsSidecar()
                    .endKafka()
                .endSpec()
                .build();
        Assertions.assertNotNull(converted.getSpec().getKafka());
        Assertions.assertNotNull(converted.getSpec().getKafka().getTlsSidecar());
        kafkaConverter.convertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNotNull(converted.getSpec().getKafka());
        Assertions.assertNull(converted.getSpec().getKafka().getTlsSidecar());
    }

    @Test
    public void testZookeeperTlsSidecarToV1Beta2() {
        convertZookeeperTlsSidecarToV1beta2("kafka.strimzi.io/v1beta1");
        convertZookeeperTlsSidecarToV1beta2("kafka.strimzi.io/v1alpha1");
    }

    public void convertZookeeperTlsSidecarToV1beta2(String fromApiVersion) {
        Kafka converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                    .withNewZookeeper()
                        .withNewTlsSidecar()
                        .endTlsSidecar()
                    .endZookeeper()
                .endSpec()
                .build();
        Assertions.assertNotNull(converted.getSpec().getZookeeper());
        Assertions.assertNotNull(converted.getSpec().getZookeeper().getTlsSidecar());
        kafkaConverter.convertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNotNull(converted.getSpec().getZookeeper());
        Assertions.assertNull(converted.getSpec().getZookeeper().getTlsSidecar());
    }

    @Test
    public void testToplevelTopicOperatorToV1Beta2() {
        String fromApiVersion = "kafka.strimzi.io/v1beta1";
        convertToplevelTopicOperatorToV1Beta2(fromApiVersion);
    }

    public void convertToplevelTopicOperatorToV1Beta2(String fromApiVersion) {
        Kafka converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                    .withNewTopicOperator()
                    .endTopicOperator()
                .endSpec()
                .build();
        kafkaConverter.convertTo(converted,
                ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNotNull(converted.getSpec().getEntityOperator().getTopicOperator());
        converted = new KafkaBuilder()
            .withApiVersion(fromApiVersion)
            .withNewSpec()
                .withNewTopicOperator()
                    .withImage("foo")
                .endTopicOperator()
            .endSpec()
            .build();
        kafkaConverter.convertTo(converted,
                ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNotNull(converted.getSpec().getEntityOperator().getTopicOperator());
        Assertions.assertEquals("foo", converted.getSpec().getEntityOperator().getTopicOperator().getImage());
    }
}