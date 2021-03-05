/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.converter;

import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaClusterSpec;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.template.ExternalTrafficPolicy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

abstract class KafkaConverterTestBase {

    ExtConverters.ExtConverter<Kafka> kafkaConverter() {
        return ExtConverters.crConverter(new KafkaConverter());
    }

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
        converted = kafkaConverter().testConvertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNotNull(converted.getSpec().getKafka().getListeners());
        Assertions.assertNotNull(converted.getSpec().getKafka().getListeners().getGenericKafkaListeners());
        Assertions.assertEquals(3, converted.getSpec().getKafka().getListeners().getGenericKafkaListeners().size());
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
        converted = kafkaConverter().testConvertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNull(converted.getSpec().getKafka().getTolerations());
        Assertions.assertEquals(1, converted.getSpec().getKafka().getTemplate().getPod().getTolerations().size());
        Assertions.assertEquals("foo", converted.getSpec().getKafka().getTemplate().getPod().getTolerations().get(0).getKey());
        Assertions.assertEquals("dbdb", converted.getSpec().getKafka().getTemplate().getPod().getTolerations().get(0).getEffect());
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
        converted = kafkaConverter().testConvertTo(converted, ApiVersion.V1BETA2);
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
        converted = kafkaConverter().testConvertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNull(converted.getSpec().getKafka().getAffinity());
        Assertions.assertNotNull(converted.getSpec().getKafka().getTemplate().getPod().getAffinity());
        Assertions.assertEquals(100, converted.getSpec().getKafka().getTemplate().getPod().getAffinity().getNodeAffinity().getPreferredDuringSchedulingIgnoredDuringExecution().get(0).getWeight());
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
        converted = kafkaConverter().testConvertTo(converted, ApiVersion.V1BETA2);
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
        converted = kafkaConverter().testConvertTo(converted, ApiVersion.V1BETA2);
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
        converted = kafkaConverter().testConvertTo(converted, ApiVersion.V1BETA2);
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
                        .withNewTlsSidecar()
                        .endTlsSidecar()
                        .withAffinity(new AffinityBuilder()
                            .withNewNodeAffinity()
                            .addNewPreferredDuringSchedulingIgnoredDuringExecution()
                            .withWeight(100)
                            .endPreferredDuringSchedulingIgnoredDuringExecution()
                            .endNodeAffinity()
                        .build())
                    .endTopicOperator()
                .endSpec()
                .build();
        converted = kafkaConverter().testConvertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNotNull(converted.getSpec().getEntityOperator().getTlsSidecar());
        Assertions.assertNotNull(converted.getSpec().getEntityOperator().getTopicOperator());
        // TODO topicOperator affinity ?

        converted = new KafkaBuilder()
            .withApiVersion(fromApiVersion)
            .withNewSpec()
                .withNewTopicOperator()
                    .withImage("foo")
                .endTopicOperator()
            .endSpec()
            .build();
        converted = kafkaConverter().testConvertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        Assertions.assertNotNull(converted.getSpec().getEntityOperator().getTopicOperator());
        Assertions.assertEquals("foo", converted.getSpec().getEntityOperator().getTopicOperator().getImage());
    }

    @Test
    public void testExternalServiceTemplateToV1Beta2() {
        String fromApiVersion = "kafka.strimzi.io/v1beta1";
        convertExternalServiceTemplateToV1Beta2(fromApiVersion);
    }

    public void convertExternalServiceTemplateToV1Beta2(String fromApiVersion) {
        Kafka converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                    .withNewKafka()
                        .withNewTemplate()
                            .withNewExternalBootstrapService()
                                .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                                .withLoadBalancerSourceRanges(Collections.singletonList("foo-bar"))
                            .endExternalBootstrapService()
                            .withNewPerPodService()
                                .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                                .withLoadBalancerSourceRanges(Collections.singletonList("foo-bar"))
                            .endPerPodService()
                        .endTemplate()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName("tls")
                            .withTls(true)
                            .withPort(9093)
                            .withType(KafkaListenerType.LOADBALANCER)
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build();
        converted = kafkaConverter().testConvertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        KafkaClusterSpec kafka = converted.getSpec().getKafka();
        Assertions.assertNull(kafka.getTemplate().getExternalBootstrapService().getExternalTrafficPolicy());
        Assertions.assertTrue(kafka.getTemplate().getExternalBootstrapService().getLoadBalancerSourceRanges() == null ||
            kafka.getTemplate().getExternalBootstrapService().getLoadBalancerSourceRanges().isEmpty());
        Assertions.assertNull(kafka.getTemplate().getPerPodService().getExternalTrafficPolicy());
        Assertions.assertTrue(kafka.getTemplate().getPerPodService().getLoadBalancerSourceRanges() == null ||
            kafka.getTemplate().getPerPodService().getLoadBalancerSourceRanges().isEmpty());
        Assertions.assertEquals(ExternalTrafficPolicy.LOCAL, kafka.getListeners().getGenericKafkaListeners().get(0).getConfiguration().getExternalTrafficPolicy());
        Assertions.assertTrue(kafka.getListeners().getGenericKafkaListeners().get(0).getConfiguration().getLoadBalancerSourceRanges().size() > 0);
        Assertions.assertEquals("foo-bar", kafka.getListeners().getGenericKafkaListeners().get(0).getConfiguration().getLoadBalancerSourceRanges().get(0));

        converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                    .withNewKafka()
                        .withNewTemplate()
                            .withNewExternalBootstrapService()
                                .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                                .withLoadBalancerSourceRanges(Collections.singletonList("foo-bar"))
                            .endExternalBootstrapService()
                        .endTemplate()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName("tls")
                            .withTls(true)
                            .withPort(9093)
                            .withType(KafkaListenerType.LOADBALANCER)
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build();
        converted = kafkaConverter().testConvertTo(converted, ApiVersion.V1BETA2);
        Assertions.assertEquals("kafka.strimzi.io/v1beta2", converted.getApiVersion());
        kafka = converted.getSpec().getKafka();
        Assertions.assertNull(kafka.getTemplate().getExternalBootstrapService().getExternalTrafficPolicy());
        Assertions.assertTrue(kafka.getTemplate().getExternalBootstrapService().getLoadBalancerSourceRanges() == null ||
            kafka.getTemplate().getExternalBootstrapService().getLoadBalancerSourceRanges().isEmpty());
        Assertions.assertEquals(ExternalTrafficPolicy.LOCAL, kafka.getListeners().getGenericKafkaListeners().get(0).getConfiguration().getExternalTrafficPolicy());
        Assertions.assertTrue(kafka.getListeners().getGenericKafkaListeners().get(0).getConfiguration().getLoadBalancerSourceRanges().size() > 0);
        Assertions.assertEquals("foo-bar", kafka.getListeners().getGenericKafkaListeners().get(0).getConfiguration().getLoadBalancerSourceRanges().get(0));

        converted = new KafkaBuilder()
                .withApiVersion(fromApiVersion)
                .withNewSpec()
                    .withNewKafka()
                        .withNewTemplate()
                            .withNewExternalBootstrapService()
                                .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                                .withLoadBalancerSourceRanges(Collections.singletonList("foo-bar"))
                            .endExternalBootstrapService()
                            .withNewPerPodService()
                                .withExternalTrafficPolicy(ExternalTrafficPolicy.CLUSTER)
                                .withLoadBalancerSourceRanges(Collections.singletonList("foo-bar"))
                            .endPerPodService()
                        .endTemplate()
                    .withNewListeners()
                        .addNewGenericKafkaListener()
                            .withName("tls")
                            .withTls(true)
                            .withPort(9093)
                            .withType(KafkaListenerType.LOADBALANCER)
                        .endGenericKafkaListener()
                    .endListeners()
                .endKafka()
            .endSpec()
            .build();
        try {
            kafkaConverter().testConvertTo(converted, ApiVersion.V1BETA2);
            Assertions.fail("KafkaClusterSpec's ExternalBootstrapService and PerPodService are not equal!");
        } catch (Exception ignored) {
        }
    }
}