/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.common.GenericSecretSourceBuilder;
import io.strimzi.api.kafka.model.common.template.ExternalTrafficPolicy;
import io.strimzi.api.kafka.model.common.template.IpFamily;
import io.strimzi.api.kafka.model.common.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBootstrap;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBootstrapBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationCustomBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.NodeAddressType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:JavaNCSS"})
@ParallelSuite
public class KafkaClusterListenersTest {
    private static final KafkaVersion.Lookup VERSIONS = KafkaVersionTestUtils.getKafkaVersionLookup();
    private static final SharedEnvironmentProvider SHARED_ENV_PROVIDER = new MockSharedEnvironmentProvider();

    private final static String NAMESPACE = "test";
    private final static String CLUSTER = "foo";
    private final static Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
                .withName(CLUSTER)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewKafka()
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
                .endKafka()
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_CONTROLLERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("controllers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_MIXED = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("mixed")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(2)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.CONTROLLER, ProcessRoles.BROKER)
            .endSpec()
            .build();
    private final static KafkaNodePool POOL_BROKERS = new KafkaNodePoolBuilder()
            .withNewMetadata()
                .withName("brokers")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withReplicas(3)
                .withNewJbodStorage()
                    .withVolumes(new PersistentClaimStorageBuilder().withId(0).withSize("100Gi").build())
                .endJbodStorage()
                .withRoles(ProcessRoles.BROKER)
            .endSpec()
            .build();

    //////////
    // Utility methods
    //////////

    private Map<String, String> expectedBrokerSelectorLabels()    {
        return Map.of(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER,
                Labels.STRIMZI_NAME_LABEL, KafkaResources.kafkaComponentName(CLUSTER),
                Labels.STRIMZI_KIND_LABEL, Kafka.RESOURCE_KIND,
                Labels.STRIMZI_BROKER_ROLE_LABEL, "true");
    }

    //////////
    // Tests
    //////////

    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelTest
    public void testListenersTemplate() {
        Map<String, String> svcLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> svcAnnotations = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> hSvcLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> hSvcAnnotations = Map.of("a7", "v7", "a8", "v8");

        Map<String, String> exSvcLabels = Map.of("l9", "v9", "l10", "v10");
        Map<String, String> exSvcAnnotations = Map.of("a9", "v9", "a10", "v10");

        Map<String, String> perPodSvcLabels = Map.of("l11", "v11", "l12", "v12");
        Map<String, String> perPodSvcAnnotations = Map.of("a11", "v11", "a12", "v12");

        Map<String, String> exRouteLabels = Map.of("l13", "v13", "l14", "v14");
        Map<String, String> exRouteAnnotations = Map.of("a13", "v13", "a14", "v14");

        Map<String, String> perPodRouteLabels = Map.of("l15", "v15", "l16", "v16");
        Map<String, String> perPodRouteAnnotations = Map.of("a15", "v15", "a16", "v16");

        Map<String, String> exIngressLabels = Map.of("l17", "v17", "l18", "v18");
        Map<String, String> exIngressAnnotations = Map.of("a17", "v17", "a18", "v18");

        Map<String, String> perPodIngressLabels = Map.of("l19", "v19", "l20", "v20");
        Map<String, String> perPodIngressAnnotations = Map.of("a19", "v19", "a20", "v20");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.ROUTE)
                                    .withTls(true)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external2")
                                    .withPort(9095)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(true)
                                    .build(),
                            new GenericKafkaListenerBuilder()
                                    .withName("external3")
                                    .withPort(9096)
                                    .withType(KafkaListenerType.INGRESS)
                                    .withTls(true)
                                    .withNewConfiguration()
                                        .withNewBootstrap()
                                            .withHost("bootstrap")
                                        .endBootstrap()
                                        .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(3)
                                                        .withHost("mixed-3")
                                                        .build(),
                                                new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(4)
                                                        .withHost("mixed-4")
                                                        .build(),
                                                new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(5)
                                                        .withHost("brokers-5")
                                                        .build(),
                                                new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(6)
                                                        .withHost("brokers-6")
                                                        .build(),
                                                new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(7)
                                                        .withHost("brokers-7")
                                                        .build())
                                    .endConfiguration()
                                    .build())
                        .withNewTemplate()
                            .withNewBootstrapService()
                                .withNewMetadata()
                                    .withLabels(svcLabels)
                                    .withAnnotations(svcAnnotations)
                                .endMetadata()
                                .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                                .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                            .endBootstrapService()
                            .withNewBrokersService()
                                .withNewMetadata()
                                    .withLabels(hSvcLabels)
                                    .withAnnotations(hSvcAnnotations)
                                .endMetadata()
                                .withIpFamilyPolicy(IpFamilyPolicy.SINGLE_STACK)
                                .withIpFamilies(IpFamily.IPV6)
                            .endBrokersService()
                            .withNewExternalBootstrapService()
                                .withNewMetadata()
                                    .withLabels(exSvcLabels)
                                    .withAnnotations(exSvcAnnotations)
                                .endMetadata()
                            .endExternalBootstrapService()
                            .withNewPerPodService()
                                .withNewMetadata()
                                    .withLabels(perPodSvcLabels)
                                    .withAnnotations(perPodSvcAnnotations)
                                .endMetadata()
                            .endPerPodService()
                            .withNewExternalBootstrapRoute()
                                .withNewMetadata()
                                    .withLabels(exRouteLabels)
                                    .withAnnotations(exRouteAnnotations)
                                .endMetadata()
                            .endExternalBootstrapRoute()
                            .withNewPerPodRoute()
                                .withNewMetadata()
                                    .withLabels(perPodRouteLabels)
                                    .withAnnotations(perPodRouteAnnotations)
                                .endMetadata()
                            .endPerPodRoute()
                            .withNewExternalBootstrapIngress()
                                .withNewMetadata()
                                    .withLabels(exIngressLabels)
                                    .withAnnotations(exIngressAnnotations)
                                .endMetadata()
                            .endExternalBootstrapIngress()
                            .withNewPerPodIngress()
                                .withNewMetadata()
                                    .withLabels(perPodIngressLabels)
                                    .withAnnotations(perPodIngressAnnotations)
                                .endMetadata()
                            .endPerPodIngress()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check Service
        Service svc = kc.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnnotations.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check Headless Service
        svc = kc.generateHeadlessService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(hSvcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(hSvcAnnotations.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("SingleStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6"));

        // Check External Bootstrap service
        List<Service> services = kc.generateExternalBootstrapServices();
        assertThat(services.size(), is(3));
        assertThat(services.get(0).getMetadata().getLabels().entrySet().containsAll(exSvcLabels.entrySet()), is(true));
        assertThat(services.get(0).getMetadata().getAnnotations().entrySet().containsAll(exSvcAnnotations.entrySet()), is(true));
        assertThat(services.get(1).getMetadata().getLabels().entrySet().containsAll(exSvcLabels.entrySet()), is(true));
        assertThat(services.get(1).getMetadata().getAnnotations().entrySet().containsAll(exSvcAnnotations.entrySet()), is(true));
        assertThat(services.get(2).getMetadata().getLabels().entrySet().containsAll(exSvcLabels.entrySet()), is(true));
        assertThat(services.get(2).getMetadata().getAnnotations().entrySet().containsAll(exSvcAnnotations.entrySet()), is(true));

        // Check Bootstrap Route
        List<Route> routes = kc.generateExternalBootstrapRoutes();
        assertThat(routes.size(), is(1));
        Route rt = routes.get(0);
        assertThat(rt.getMetadata().getLabels().entrySet().containsAll(exRouteLabels.entrySet()), is(true));
        assertThat(rt.getMetadata().getAnnotations().entrySet().containsAll(exRouteAnnotations.entrySet()), is(true));

        // Check Bootstrap Ingress
        List<Ingress> ingresses = kc.generateExternalBootstrapIngresses();
        assertThat(ingresses.size(), is(1));
        Ingress ing = ingresses.get(0);
        assertThat(ing.getMetadata().getLabels().entrySet().containsAll(exIngressLabels.entrySet()), is(true));
        assertThat(ing.getMetadata().getAnnotations().entrySet().containsAll(exIngressAnnotations.entrySet()), is(true));

        // Check per pod service
        services = kc.generatePerPodServices();
        assertThat(services.size(), is(15));
        for (Service service : services) {
            assertThat(service.getMetadata().getLabels().entrySet().containsAll(perPodSvcLabels.entrySet()), is(true));
            assertThat(service.getMetadata().getAnnotations().entrySet().containsAll(perPodSvcAnnotations.entrySet()), is(true));
        }

        // Check PerPodRoute
        routes = kc.generateExternalRoutes();
        assertThat(routes.size(), is(5));
        for (Route route : routes) {
            assertThat(route.getMetadata().getLabels().entrySet().containsAll(perPodRouteLabels.entrySet()), is(true));
            assertThat(route.getMetadata().getAnnotations().entrySet().containsAll(perPodRouteAnnotations.entrySet()), is(true));
        }

        // Check PerPodIngress
        ingresses = kc.generateExternalIngresses();
        assertThat(ingresses.size(), is(5));
        for (Ingress ingress : ingresses) {
            assertThat(ingress.getMetadata().getLabels().entrySet().containsAll(perPodIngressLabels.entrySet()), is(true));
            assertThat(ingress.getMetadata().getAnnotations().entrySet().containsAll(perPodIngressAnnotations.entrySet()), is(true));
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelTest
    public void testListenersTemplateFromKafkaAndNodePools() {
        Map<String, String> svcLabels = Map.of("l5", "v5", "l6", "v6");
        Map<String, String> svcAnnotations = Map.of("a5", "v5", "a6", "v6");

        Map<String, String> hSvcLabels = Map.of("l7", "v7", "l8", "v8");
        Map<String, String> hSvcAnnotations = Map.of("a7", "v7", "a8", "v8");

        Map<String, String> exSvcLabels = Map.of("l9", "v9", "l10", "v10");
        Map<String, String> exSvcAnnotations = Map.of("a9", "v9", "a10", "v10");

        Map<String, String> perPodSvcLabels = Map.of("l11", "v11", "l12", "v12");
        Map<String, String> perPodSvcAnnotations = Map.of("a11", "v11", "a12", "v12");

        Map<String, String> exRouteLabels = Map.of("l13", "v13", "l14", "v14");
        Map<String, String> exRouteAnnotations = Map.of("a13", "v13", "a14", "v14");

        Map<String, String> perPodRouteLabels = Map.of("l15", "v15", "l16", "v16");
        Map<String, String> perPodRouteAnnotations = Map.of("a15", "v15", "a16", "v16");

        Map<String, String> exIngressLabels = Map.of("l17", "v17", "l18", "v18");
        Map<String, String> exIngressAnnotations = Map.of("a17", "v17", "a18", "v18");

        Map<String, String> perPodIngressLabels = Map.of("l19", "v19", "l20", "v20");
        Map<String, String> perPodIngressAnnotations = Map.of("a19", "v19", "a20", "v20");

        // Node pool values
        Map<String, String> perPodSvcLabels2 = Map.of("l21", "v21", "l22", "v22");
        Map<String, String> perPodSvcAnnotations2 = Map.of("a21", "v21", "a22", "v22");

        Map<String, String> perPodRouteLabels2 = Map.of("l25", "v25", "l26", "v26");
        Map<String, String> perPodRouteAnnotations2 = Map.of("a25", "v25", "a26", "v26");

        Map<String, String> perPodIngressLabels2 = Map.of("l29", "v29", "l30", "v30");
        Map<String, String> perPodIngressAnnotations2 = Map.of("a29", "v29", "a30", "v30");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.ROUTE)
                                    .withTls(true)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external2")
                                    .withPort(9095)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(true)
                                    .build(),
                            new GenericKafkaListenerBuilder()
                                    .withName("external3")
                                    .withPort(9096)
                                    .withType(KafkaListenerType.INGRESS)
                                    .withTls(true)
                                    .withNewConfiguration()
                                        .withNewBootstrap()
                                            .withHost("bootstrap")
                                        .endBootstrap()
                                        .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(3)
                                                        .withHost("mixed-3")
                                                        .build(),
                                                new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(4)
                                                        .withHost("mixed-4")
                                                        .build(),
                                                new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(5)
                                                        .withHost("brokers-5")
                                                        .build(),
                                                new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(6)
                                                        .withHost("brokers-6")
                                                        .build(),
                                                new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(7)
                                                        .withHost("brokers-7")
                                                        .build())
                                    .endConfiguration()
                                    .build())
                        .withNewTemplate()
                            .withNewBootstrapService()
                                .withNewMetadata()
                                    .withLabels(svcLabels)
                                    .withAnnotations(svcAnnotations)
                                .endMetadata()
                                .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                                .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                            .endBootstrapService()
                            .withNewBrokersService()
                                .withNewMetadata()
                                    .withLabels(hSvcLabels)
                                    .withAnnotations(hSvcAnnotations)
                                .endMetadata()
                                .withIpFamilyPolicy(IpFamilyPolicy.SINGLE_STACK)
                                .withIpFamilies(IpFamily.IPV6)
                            .endBrokersService()
                            .withNewExternalBootstrapService()
                                .withNewMetadata()
                                    .withLabels(exSvcLabels)
                                    .withAnnotations(exSvcAnnotations)
                                .endMetadata()
                            .endExternalBootstrapService()
                            .withNewPerPodService()
                                .withNewMetadata()
                                    .withLabels(perPodSvcLabels)
                                    .withAnnotations(perPodSvcAnnotations)
                                .endMetadata()
                            .endPerPodService()
                            .withNewExternalBootstrapRoute()
                                .withNewMetadata()
                                    .withLabels(exRouteLabels)
                                    .withAnnotations(exRouteAnnotations)
                                .endMetadata()
                            .endExternalBootstrapRoute()
                            .withNewPerPodRoute()
                                .withNewMetadata()
                                    .withLabels(perPodRouteLabels)
                                    .withAnnotations(perPodRouteAnnotations)
                                .endMetadata()
                            .endPerPodRoute()
                            .withNewExternalBootstrapIngress()
                                .withNewMetadata()
                                    .withLabels(exIngressLabels)
                                    .withAnnotations(exIngressAnnotations)
                                .endMetadata()
                            .endExternalBootstrapIngress()
                            .withNewPerPodIngress()
                                .withNewMetadata()
                                    .withLabels(perPodIngressLabels)
                                    .withAnnotations(perPodIngressAnnotations)
                                .endMetadata()
                            .endPerPodIngress()
                        .endTemplate()
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPerPodService()
                            .withNewMetadata()
                                .withLabels(perPodSvcLabels2)
                                .withAnnotations(perPodSvcAnnotations2)
                            .endMetadata()
                        .endPerPodService()
                        .withNewPerPodRoute()
                            .withNewMetadata()
                                .withLabels(perPodRouteLabels2)
                                .withAnnotations(perPodRouteAnnotations2)
                            .endMetadata()
                        .endPerPodRoute()
                        .withNewPerPodIngress()
                            .withNewMetadata()
                                .withLabels(perPodIngressLabels2)
                                .withAnnotations(perPodIngressAnnotations2)
                            .endMetadata()
                        .endPerPodIngress()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check Service
        Service svc = kc.generateService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(svcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(svcAnnotations.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));

        // Check Headless Service
        svc = kc.generateHeadlessService();
        assertThat(svc.getMetadata().getLabels().entrySet().containsAll(hSvcLabels.entrySet()), is(true));
        assertThat(svc.getMetadata().getAnnotations().entrySet().containsAll(hSvcAnnotations.entrySet()), is(true));
        assertThat(svc.getSpec().getIpFamilyPolicy(), is("SingleStack"));
        assertThat(svc.getSpec().getIpFamilies(), contains("IPv6"));

        // Check External Bootstrap service
        List<Service> services = kc.generateExternalBootstrapServices();
        assertThat(services.size(), is(3));
        assertThat(services.get(0).getMetadata().getLabels().entrySet().containsAll(exSvcLabels.entrySet()), is(true));
        assertThat(services.get(0).getMetadata().getAnnotations().entrySet().containsAll(exSvcAnnotations.entrySet()), is(true));
        assertThat(services.get(1).getMetadata().getLabels().entrySet().containsAll(exSvcLabels.entrySet()), is(true));
        assertThat(services.get(1).getMetadata().getAnnotations().entrySet().containsAll(exSvcAnnotations.entrySet()), is(true));
        assertThat(services.get(2).getMetadata().getLabels().entrySet().containsAll(exSvcLabels.entrySet()), is(true));
        assertThat(services.get(2).getMetadata().getAnnotations().entrySet().containsAll(exSvcAnnotations.entrySet()), is(true));

        // Check Bootstrap Route
        List<Route> routes = kc.generateExternalBootstrapRoutes();
        assertThat(routes.size(), is(1));
        Route rt = routes.get(0);
        assertThat(rt.getMetadata().getLabels().entrySet().containsAll(exRouteLabels.entrySet()), is(true));
        assertThat(rt.getMetadata().getAnnotations().entrySet().containsAll(exRouteAnnotations.entrySet()), is(true));

        // Check Bootstrap Ingress
        List<Ingress> ingresses = kc.generateExternalBootstrapIngresses();
        assertThat(ingresses.size(), is(1));
        Ingress ing = ingresses.get(0);
        assertThat(ing.getMetadata().getLabels().entrySet().containsAll(exIngressLabels.entrySet()), is(true));
        assertThat(ing.getMetadata().getAnnotations().entrySet().containsAll(exIngressAnnotations.entrySet()), is(true));

        // Check per pod service
        services = kc.generatePerPodServices();
        assertThat(services.size(), is(15));
        for (Service service : services) {
            if (service.getMetadata().getName().contains("brokers")) {
                assertThat(service.getMetadata().getLabels().entrySet().containsAll(perPodSvcLabels2.entrySet()), is(true));
                assertThat(service.getMetadata().getAnnotations().entrySet().containsAll(perPodSvcAnnotations2.entrySet()), is(true));
                assertThat(service.getMetadata().getLabels().entrySet().containsAll(perPodSvcLabels.entrySet()), is(false));
                assertThat(service.getMetadata().getAnnotations().entrySet().containsAll(perPodSvcAnnotations.entrySet()), is(false));
            } else {
                assertThat(service.getMetadata().getLabels().entrySet().containsAll(perPodSvcLabels2.entrySet()), is(false));
                assertThat(service.getMetadata().getAnnotations().entrySet().containsAll(perPodSvcAnnotations2.entrySet()), is(false));
                assertThat(service.getMetadata().getLabels().entrySet().containsAll(perPodSvcLabels.entrySet()), is(true));
                assertThat(service.getMetadata().getAnnotations().entrySet().containsAll(perPodSvcAnnotations.entrySet()), is(true));
            }
        }

        // Check PerPodRoute
        routes = kc.generateExternalRoutes();
        assertThat(routes.size(), is(5));
        for (Route route : routes) {
            if (route.getMetadata().getName().contains("brokers")) {
                assertThat(route.getMetadata().getLabels().entrySet().containsAll(perPodRouteLabels2.entrySet()), is(true));
                assertThat(route.getMetadata().getAnnotations().entrySet().containsAll(perPodRouteAnnotations2.entrySet()), is(true));
                assertThat(route.getMetadata().getLabels().entrySet().containsAll(perPodRouteLabels.entrySet()), is(false));
                assertThat(route.getMetadata().getAnnotations().entrySet().containsAll(perPodRouteAnnotations.entrySet()), is(false));
            } else {
                assertThat(route.getMetadata().getLabels().entrySet().containsAll(perPodRouteLabels2.entrySet()), is(false));
                assertThat(route.getMetadata().getAnnotations().entrySet().containsAll(perPodRouteAnnotations2.entrySet()), is(false));
                assertThat(route.getMetadata().getLabels().entrySet().containsAll(perPodRouteLabels.entrySet()), is(true));
                assertThat(route.getMetadata().getAnnotations().entrySet().containsAll(perPodRouteAnnotations.entrySet()), is(true));
            }
        }

        // Check PerPodIngress
        ingresses = kc.generateExternalIngresses();
        assertThat(ingresses.size(), is(5));
        for (Ingress ingress : ingresses) {
            if (ingress.getMetadata().getName().contains("brokers")) {
                assertThat(ingress.getMetadata().getLabels().entrySet().containsAll(perPodIngressLabels2.entrySet()), is(true));
                assertThat(ingress.getMetadata().getAnnotations().entrySet().containsAll(perPodIngressAnnotations2.entrySet()), is(true));
                assertThat(ingress.getMetadata().getLabels().entrySet().containsAll(perPodIngressLabels.entrySet()), is(false));
                assertThat(ingress.getMetadata().getAnnotations().entrySet().containsAll(perPodIngressAnnotations.entrySet()), is(false));
            } else {
                assertThat(ingress.getMetadata().getLabels().entrySet().containsAll(perPodIngressLabels2.entrySet()), is(false));
                assertThat(ingress.getMetadata().getAnnotations().entrySet().containsAll(perPodIngressAnnotations2.entrySet()), is(false));
                assertThat(ingress.getMetadata().getLabels().entrySet().containsAll(perPodIngressLabels.entrySet()), is(true));
                assertThat(ingress.getMetadata().getAnnotations().entrySet().containsAll(perPodIngressAnnotations.entrySet()), is(true));
            }
        }
    }

    @ParallelTest
    public void testListenersTemplateFromNodePools() {
        // Node pool values
        Map<String, String> perPodSvcLabels2 = Map.of("l21", "v21", "l22", "v22");
        Map<String, String> perPodSvcAnnotations2 = Map.of("a21", "v21", "a22", "v22");

        Map<String, String> perPodRouteLabels2 = Map.of("l25", "v25", "l26", "v26");
        Map<String, String> perPodRouteAnnotations2 = Map.of("a25", "v25", "a26", "v26");

        Map<String, String> perPodIngressLabels2 = Map.of("l29", "v29", "l30", "v30");
        Map<String, String> perPodIngressAnnotations2 = Map.of("a29", "v29", "a30", "v30");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("external")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.ROUTE)
                                    .withTls(true)
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("external2")
                                    .withPort(9095)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(true)
                                    .build(),
                            new GenericKafkaListenerBuilder()
                                    .withName("external3")
                                    .withPort(9096)
                                    .withType(KafkaListenerType.INGRESS)
                                    .withTls(true)
                                    .withNewConfiguration()
                                        .withNewBootstrap()
                                            .withHost("bootstrap")
                                        .endBootstrap()
                                        .withBrokers(new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(3)
                                                        .withHost("mixed-3")
                                                        .build(),
                                                new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(4)
                                                        .withHost("mixed-4")
                                                        .build(),
                                                new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(5)
                                                        .withHost("brokers-5")
                                                        .build(),
                                                new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(6)
                                                        .withHost("brokers-6")
                                                        .build(),
                                                new GenericKafkaListenerConfigurationBrokerBuilder()
                                                        .withBroker(7)
                                                        .withHost("brokers-7")
                                                        .build())
                                    .endConfiguration()
                                    .build())
                    .endKafka()
                .endSpec()
                .build();

        KafkaNodePool brokers = new KafkaNodePoolBuilder(POOL_BROKERS)
                .editSpec()
                    .withNewTemplate()
                        .withNewPerPodService()
                            .withNewMetadata()
                                .withLabels(perPodSvcLabels2)
                                .withAnnotations(perPodSvcAnnotations2)
                            .endMetadata()
                        .endPerPodService()
                        .withNewPerPodRoute()
                            .withNewMetadata()
                                .withLabels(perPodRouteLabels2)
                                .withAnnotations(perPodRouteAnnotations2)
                            .endMetadata()
                        .endPerPodRoute()
                        .withNewPerPodIngress()
                            .withNewMetadata()
                                .withLabels(perPodIngressLabels2)
                                .withAnnotations(perPodIngressAnnotations2)
                            .endMetadata()
                        .endPerPodIngress()
                    .endTemplate()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, brokers), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check per pod service
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(15));
        for (Service service : services) {
            if (service.getMetadata().getName().contains("brokers")) {
                assertThat(service.getMetadata().getLabels().entrySet().containsAll(perPodSvcLabels2.entrySet()), is(true));
                assertThat(service.getMetadata().getAnnotations().entrySet().containsAll(perPodSvcAnnotations2.entrySet()), is(true));
            } else {
                assertThat(service.getMetadata().getLabels().entrySet().containsAll(perPodSvcLabels2.entrySet()), is(false));
                assertThat(service.getMetadata().getAnnotations().entrySet().containsAll(perPodSvcAnnotations2.entrySet()), is(false));
            }
        }

        // Check PerPodRoute
        List<Route> routes = kc.generateExternalRoutes();
        assertThat(routes.size(), is(5));
        for (Route route : routes) {
            if (route.getMetadata().getName().contains("brokers")) {
                assertThat(route.getMetadata().getLabels().entrySet().containsAll(perPodRouteLabels2.entrySet()), is(true));
                assertThat(route.getMetadata().getAnnotations().entrySet().containsAll(perPodRouteAnnotations2.entrySet()), is(true));
            } else {
                assertThat(route.getMetadata().getLabels().entrySet().containsAll(perPodRouteLabels2.entrySet()), is(false));
                assertThat(route.getMetadata().getAnnotations().entrySet().containsAll(perPodRouteAnnotations2.entrySet()), is(false));
            }
        }

        // Check PerPodIngress
        List<Ingress> ingresses = kc.generateExternalIngresses();
        assertThat(ingresses.size(), is(5));
        for (Ingress ingress : ingresses) {
            if (ingress.getMetadata().getName().contains("brokers")) {
                assertThat(ingress.getMetadata().getLabels().entrySet().containsAll(perPodIngressLabels2.entrySet()), is(true));
                assertThat(ingress.getMetadata().getAnnotations().entrySet().containsAll(perPodIngressAnnotations2.entrySet()), is(true));
            } else {
                assertThat(ingress.getMetadata().getLabels().entrySet().containsAll(perPodIngressLabels2.entrySet()), is(false));
                assertThat(ingress.getMetadata().getAnnotations().entrySet().containsAll(perPodIngressAnnotations2.entrySet()), is(false));
            }
        }
    }

    @ParallelTest
    public void testListenerResourcesWithInternalListenerOnly()  {
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, KAFKA, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, KAFKA, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        assertThat(kc.generatePerPodServices().size(), is(0));
        assertThat(kc.generateExternalIngresses().size(), is(0));
        assertThat(kc.generateExternalRoutes().size(), is(0));
    }

    @ParallelTest
    public void testExternalRoutes() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withPort(9094)
                        .withType(KafkaListenerType.ROUTE)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationTlsAuth()
                        .endKafkaListenerAuthenticationTlsAuth()
                        .build())
                .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        
        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();
            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(false));
            } else {
                assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(true));
            }
        }));

        // Check external bootstrap service
        List<Service> bootstrapServices = kc.generateExternalBootstrapServices();
        assertThat(bootstrapServices.size(), is(1));
        assertThat(bootstrapServices.get(0).getMetadata().getName(), is(CLUSTER + "-kafka-external-bootstrap"));
        assertThat(bootstrapServices.get(0).getSpec().getType(), is("ClusterIP"));
        assertThat(bootstrapServices.get(0).getSpec().getSelector(), is(expectedBrokerSelectorLabels()));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().size(), is(1));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        TestUtils.checkOwnerReference(bootstrapServices.get(0), KAFKA);

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services)    {
            if (service.getMetadata().getName().contains("foo-brokers-")) {
                assertThat(service.getMetadata().getName(), startsWith("foo-brokers-"));
                assertThat(service.getSpec().getSelector().get(Labels.STRIMZI_POD_NAME_LABEL), oneOf("foo-brokers-5", "foo-brokers-6", "foo-brokers-7"));
                TestUtils.checkOwnerReference(service, POOL_BROKERS);
            } else {
                assertThat(service.getMetadata().getName(), startsWith("foo-mixed-"));
                assertThat(service.getSpec().getSelector().get(Labels.STRIMZI_POD_NAME_LABEL), oneOf("foo-mixed-3", "foo-mixed-4"));
                TestUtils.checkOwnerReference(service, POOL_MIXED);
            }

            assertThat(service.getSpec().getType(), is("ClusterIP"));
            assertThat(service.getSpec().getPorts().size(), is(1));
            assertThat(service.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
            assertThat(service.getSpec().getPorts().get(0).getPort(), is(9094));
            assertThat(service.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
            assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
            assertThat(service.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        }

        // Check bootstrap route
        List<Route> bootstrapRoutes = kc.generateExternalBootstrapRoutes();
        assertThat(bootstrapRoutes.size(), is(1));

        assertThat(bootstrapRoutes.get(0).getMetadata().getName(), is(KafkaResources.bootstrapServiceName(CLUSTER)));
        assertThat(bootstrapRoutes.get(0).getSpec().getTls().getTermination(), is("passthrough"));
        assertThat(bootstrapRoutes.get(0).getSpec().getTo().getKind(), is("Service"));
        assertThat(bootstrapRoutes.get(0).getSpec().getTo().getName(), is(CLUSTER + "-kafka-external-bootstrap"));
        assertThat(bootstrapRoutes.get(0).getSpec().getPort().getTargetPort(), is(new IntOrString(9094)));
        TestUtils.checkOwnerReference(bootstrapRoutes.get(0), KAFKA);

        // Check per pod router
        List<Route> routes = kc.generateExternalRoutes();
        assertThat(routes.size(), is(5));

        for (Route route : routes)    {
            if (route.getMetadata().getName().contains("foo-brokers-")) {
                assertThat(route.getMetadata().getName(), startsWith("foo-brokers-"));
                assertThat(route.getSpec().getTo().getName(), oneOf("foo-brokers-5", "foo-brokers-6", "foo-brokers-7"));
                TestUtils.checkOwnerReference(route, POOL_BROKERS);
            } else {
                assertThat(route.getMetadata().getName(), startsWith("foo-mixed-"));
                assertThat(route.getSpec().getTo().getName(), oneOf("foo-mixed-3", "foo-mixed-4"));
                TestUtils.checkOwnerReference(route, POOL_MIXED);
            }

            assertThat(route.getSpec().getTls().getTermination(), is("passthrough"));
            assertThat(route.getSpec().getTo().getKind(), is("Service"));
            assertThat(route.getSpec().getPort().getTargetPort(), is(new IntOrString(9094)));
        }
    }

    @ParallelTest
    public void testExternalRoutesWithHostOverrides() {
        GenericKafkaListenerConfigurationBroker routeListenerBrokerConfig3 = new GenericKafkaListenerConfigurationBroker();
        routeListenerBrokerConfig3.setBroker(3);
        routeListenerBrokerConfig3.setHost("my-host-3.cz");

        GenericKafkaListenerConfigurationBroker routeListenerBrokerConfig6 = new GenericKafkaListenerConfigurationBroker();
        routeListenerBrokerConfig6.setBroker(6);
        routeListenerBrokerConfig6.setHost("my-host-6.cz");

        GenericKafkaListenerConfigurationBroker routeListenerBrokerConfig7 = new GenericKafkaListenerConfigurationBroker();
        routeListenerBrokerConfig7.setBroker(7);
        routeListenerBrokerConfig7.setHost("my-host-7.cz");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.ROUTE)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("my-boostrap.cz")
                                    .endBootstrap()
                                    .withBrokers(routeListenerBrokerConfig3, routeListenerBrokerConfig6, routeListenerBrokerConfig7)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check bootstrap route
        Route brt = kc.generateExternalBootstrapRoutes().get(0);
        assertThat(brt.getMetadata().getName(), is(KafkaResources.bootstrapServiceName(CLUSTER)));
        assertThat(brt.getSpec().getHost(), is("my-boostrap.cz"));

        // Check per pod router
        List<Route> routes = kc.generateExternalRoutes();
        assertThat(routes.size(), is(5));

        for (Route route : routes)    {
            if (route.getMetadata().getName().startsWith("foo-brokers-6")) {
                assertThat(route.getSpec().getHost(), is("my-host-6.cz"));
            } else if (route.getMetadata().getName().startsWith("foo-brokers-7")) {
                assertThat(route.getSpec().getHost(), is("my-host-7.cz"));
            } else if (route.getMetadata().getName().startsWith("foo-mixed-3")) {
                assertThat(route.getSpec().getHost(), is("my-host-3.cz"));
            } else {
                assertThat(route.getSpec().getHost(), is(nullValue()));
            }
        }
    }

    @ParallelTest
    public void testExternalRoutesWithLabelsAndAnnotations() {
        GenericKafkaListenerConfigurationBroker routeListenerBrokerConfig3 = new GenericKafkaListenerConfigurationBroker();
        routeListenerBrokerConfig3.setBroker(3);
        routeListenerBrokerConfig3.setAnnotations(Map.of("anno", "anno-value-3"));
        routeListenerBrokerConfig3.setLabels(Map.of("label", "label-value-3"));

        GenericKafkaListenerConfigurationBroker routeListenerBrokerConfig6 = new GenericKafkaListenerConfigurationBroker();
        routeListenerBrokerConfig6.setBroker(6);
        routeListenerBrokerConfig6.setAnnotations(Map.of("anno", "anno-value-6"));
        routeListenerBrokerConfig6.setLabels(Map.of("label", "label-value-6"));

        GenericKafkaListenerConfigurationBroker routeListenerBrokerConfig7 = new GenericKafkaListenerConfigurationBroker();
        routeListenerBrokerConfig7.setBroker(7);
        routeListenerBrokerConfig7.setAnnotations(Map.of("anno", "anno-value-7"));
        routeListenerBrokerConfig7.setLabels(Map.of("label", "label-value-7"));

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.ROUTE)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withAnnotations(Map.of("anno", "anno-value"))
                                        .withLabels(Map.of("label", "label-value"))
                                    .endBootstrap()
                                    .withBrokers(routeListenerBrokerConfig3, routeListenerBrokerConfig6, routeListenerBrokerConfig7)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check bootstrap route
        Route brt = kc.generateExternalBootstrapRoutes().get(0);
        assertThat(brt.getMetadata().getName(), is(KafkaResources.bootstrapServiceName(CLUSTER)));
        assertThat(brt.getMetadata().getAnnotations().get("anno"), is("anno-value"));
        assertThat(brt.getMetadata().getLabels().get("label"), is("label-value"));

        // Check per pod router
        List<Route> routes = kc.generateExternalRoutes();
        assertThat(routes.size(), is(5));

        for (Route route : routes)    {
            if (route.getMetadata().getName().startsWith("foo-brokers-6")) {
                assertThat(route.getMetadata().getAnnotations().get("anno"), is("anno-value-6"));
                assertThat(route.getMetadata().getLabels().get("label"), is("label-value-6"));
            } else if (route.getMetadata().getName().startsWith("foo-brokers-7")) {
                assertThat(route.getMetadata().getAnnotations().get("anno"), is("anno-value-7"));
                assertThat(route.getMetadata().getLabels().get("label"), is("label-value-7"));
            } else if (route.getMetadata().getName().startsWith("foo-mixed-3")) {
                assertThat(route.getMetadata().getAnnotations().get("anno"), is("anno-value-3"));
                assertThat(route.getMetadata().getLabels().get("label"), is("label-value-3"));
            } else {
                assertThat(route.getMetadata().getAnnotations().get("anno"), is(nullValue()));
                assertThat(route.getMetadata().getLabels().get("label"), is(nullValue()));
            }
        }
    }

    @ParallelTest
    public void testExternalLoadBalancers() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(false));
            } else {
                assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(true));
            }
        }));

        // Check external bootstrap service
        List<Service> bootstrapServices = kc.generateExternalBootstrapServices();
        assertThat(bootstrapServices.size(), is(1));

        assertThat(bootstrapServices.get(0).getMetadata().getName(), is(CLUSTER + "-kafka-external-bootstrap"));
        assertThat(bootstrapServices.get(0).getMetadata().getFinalizers(), is(List.of()));
        assertThat(bootstrapServices.get(0).getSpec().getType(), is("LoadBalancer"));
        assertThat(bootstrapServices.get(0).getSpec().getSelector(), is(expectedBrokerSelectorLabels()));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().size(), is(1));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(bootstrapServices.get(0).getSpec().getLoadBalancerIP(), is(nullValue()));
        assertThat(bootstrapServices.get(0).getSpec().getExternalTrafficPolicy(), is("Cluster"));
        assertThat(bootstrapServices.get(0).getSpec().getLoadBalancerSourceRanges(), is(List.of()));
        assertThat(bootstrapServices.get(0).getSpec().getAllocateLoadBalancerNodePorts(), is(nullValue()));
        TestUtils.checkOwnerReference(bootstrapServices.get(0), KAFKA);

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services)    {
            if (service.getMetadata().getName().contains("foo-brokers-")) {
                assertThat(service.getMetadata().getName(), startsWith("foo-brokers-"));
                assertThat(service.getSpec().getSelector().get(Labels.STRIMZI_POD_NAME_LABEL), oneOf("foo-brokers-5", "foo-brokers-6", "foo-brokers-7"));
                TestUtils.checkOwnerReference(service, POOL_BROKERS);
            } else {
                assertThat(service.getMetadata().getName(), startsWith("foo-mixed-"));
                assertThat(service.getSpec().getSelector().get(Labels.STRIMZI_POD_NAME_LABEL), oneOf("foo-mixed-3", "foo-mixed-4"));
                TestUtils.checkOwnerReference(service, POOL_MIXED);
            }

            assertThat(service.getMetadata().getFinalizers(), is(List.of()));
            assertThat(service.getSpec().getType(), is("LoadBalancer"));
            assertThat(service.getSpec().getPorts().size(), is(1));
            assertThat(service.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
            assertThat(service.getSpec().getPorts().get(0).getPort(), is(9094));
            assertThat(service.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
            assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
            assertThat(service.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            assertThat(service.getSpec().getLoadBalancerIP(), is(nullValue()));
            assertThat(service.getSpec().getExternalTrafficPolicy(), is("Cluster"));
            assertThat(service.getSpec().getLoadBalancerSourceRanges(), is(List.of()));
            assertThat(service.getSpec().getAllocateLoadBalancerNodePorts(), is(nullValue()));
        }
    }

    @ParallelTest
    public void testExternalLoadBalancersWithoutBootstrapService() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withPort(9094)
                        .withType(KafkaListenerType.LOADBALANCER)
                        .withTls(true)
                        .withNewKafkaListenerAuthenticationTlsAuth()
                        .endKafkaListenerAuthenticationTlsAuth()
                        .withNewConfiguration()
                            .withCreateBootstrapService(false)
                        .endConfiguration()
                        .build())
                .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check external bootstrap service
        assertThat(kc.generateExternalBootstrapServices().isEmpty(), is(true));
    }

    @ParallelTest
    public void testLoadBalancerExternalTrafficPolicyLocalFromListener() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withExternalTrafficPolicy(ExternalTrafficPolicy.LOCAL)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getSpec().getExternalTrafficPolicy(), is(ExternalTrafficPolicy.LOCAL.toValue()));
        assertThat(ext.getSpec().getAllocateLoadBalancerNodePorts(), is(nullValue()));

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services)    {
            assertThat(service.getSpec().getExternalTrafficPolicy(), is(ExternalTrafficPolicy.LOCAL.toValue()));
            assertThat(service.getSpec().getAllocateLoadBalancerNodePorts(), is(nullValue()));
        }
    }

    @ParallelTest
    public void testLoadBalancerExternalTrafficPolicyClusterFromListener() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withExternalTrafficPolicy(ExternalTrafficPolicy.CLUSTER)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getSpec().getExternalTrafficPolicy(), is(ExternalTrafficPolicy.CLUSTER.toValue()));

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services)    {
            assertThat(service.getSpec().getExternalTrafficPolicy(), is(ExternalTrafficPolicy.CLUSTER.toValue()));
        }
    }

    @ParallelTest
    public void testExternalLoadBalancerAllocateNodePorts() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                        .withName("lb1")
                                        .withType(KafkaListenerType.LOADBALANCER)
                                        .withPort(9094)
                                        .withNewConfiguration()
                                            .withAllocateLoadBalancerNodePorts(false)
                                        .endConfiguration()
                                        .build())
                .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_BROKERS, POOL_MIXED), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<Service> externalServices = kc.generateExternalBootstrapServices();
        assertThat(externalServices, hasSize(1));
        assertThat(externalServices.get(0).getSpec().getAllocateLoadBalancerNodePorts(), is(false));
        assertThat(externalServices.get(0).getSpec().getPorts(), hasSize(1));
        assertThat(externalServices.get(0).getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-lb1"));

        List<Service> perPodServices = kc.generatePerPodServices();
        assertThat(perPodServices, hasSize(5));
        for (Service service : perPodServices) {
            assertThat(service.getSpec().getAllocateLoadBalancerNodePorts(), is(false));
            assertThat(service.getSpec().getPorts(), hasSize(1));
            assertThat(service.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-lb1"));
        }
    }

    @ParallelTest
    public void testFinalizersFromListener() {
        List<String> finalizers = List.of("service.kubernetes.io/load-balancer-cleanup", "my-domain.io/my-custom-finalizer");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withFinalizers(finalizers)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getMetadata().getFinalizers(), is(finalizers));

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services)    {
            assertThat(service.getMetadata().getFinalizers(), is(finalizers));
        }
    }

    @ParallelTest
    public void testLoadBalancerSourceRangeFromListener() {
        List<String> sourceRanges = List.of("10.0.0.0/8", "130.211.204.1/32");

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withLoadBalancerSourceRanges(sourceRanges)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getSpec().getLoadBalancerSourceRanges(), is(sourceRanges));

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services)    {
            assertThat(service.getSpec().getLoadBalancerSourceRanges(), is(sourceRanges));
        }
    }

    @ParallelTest
    public void testExternalLoadBalancersWithLabelsAndAnnotations() {
        GenericKafkaListenerConfigurationBootstrap bootstrapConfig = new GenericKafkaListenerConfigurationBootstrapBuilder()
                .withAnnotations(Map.of("external-dns.alpha.kubernetes.io/hostname", "bootstrap.my-ingress.com."))
                .withLabels(Map.of("label", "label-value"))
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig3 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(3)
                .withAnnotations(Map.of("external-dns.alpha.kubernetes.io/hostname", "broker-3.my-ingress.com."))
                .withLabels(Map.of("label", "label-value-3"))
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig6 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(6)
                .withAnnotations(Map.of("external-dns.alpha.kubernetes.io/hostname", "broker-6.my-ingress.com."))
                .withLabels(Map.of("label", "label-value-6"))
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBootstrap(bootstrapConfig)
                                    .withBrokers(brokerConfig3, brokerConfig6)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check annotations
        assertThat(kc.generateExternalBootstrapServices().get(0).getMetadata().getAnnotations(), is(Map.of("external-dns.alpha.kubernetes.io/hostname", "bootstrap.my-ingress.com.")));
        assertThat(kc.generateExternalBootstrapServices().get(0).getMetadata().getLabels().get("label"), is("label-value"));

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services)    {
            if (service.getMetadata().getName().startsWith("foo-brokers-6")) {
                assertThat(service.getMetadata().getAnnotations().get("external-dns.alpha.kubernetes.io/hostname"), is("broker-6.my-ingress.com."));
                assertThat(service.getMetadata().getLabels().get("label"), is("label-value-6"));
            } else if (service.getMetadata().getName().startsWith("foo-mixed-3")) {
                assertThat(service.getMetadata().getAnnotations().get("external-dns.alpha.kubernetes.io/hostname"), is("broker-3.my-ingress.com."));
                assertThat(service.getMetadata().getLabels().get("label"), is("label-value-3"));
            } else {
                assertThat(service.getMetadata().getAnnotations().get("external-dns.alpha.kubernetes.io/hostname"), is(nullValue()));
                assertThat(service.getMetadata().getLabels().get("label"), is(nullValue()));
            }
        }
    }

    @ParallelTest
    public void testExternalLoadBalancersWithLoadBalancerIPOverride() {
        GenericKafkaListenerConfigurationBootstrap bootstrapConfig = new GenericKafkaListenerConfigurationBootstrapBuilder()
                .withLoadBalancerIP("10.0.0.1")
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig6 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(6)
                .withLoadBalancerIP("10.0.0.6")
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig3 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(3)
                .withLoadBalancerIP("10.0.0.3")
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBootstrap(bootstrapConfig)
                                    .withBrokers(brokerConfig6, brokerConfig3)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check annotations
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getLoadBalancerIP(), is("10.0.0.1"));

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services) {
            if (service.getMetadata().getName().startsWith("foo-brokers-6")) {
                assertThat(service.getSpec().getLoadBalancerIP(), is("10.0.0.6"));
            } else if (service.getMetadata().getName().startsWith("foo-mixed-3")) {
                assertThat(service.getSpec().getLoadBalancerIP(), is("10.0.0.3"));
            } else {
                assertThat(service.getSpec().getLoadBalancerIP(), is(nullValue()));
            }
        }
    }

    @ParallelTest
    public void testExternalLoadBalancersWithLoadBalancerClass() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withPort(9094)
                        .withType(KafkaListenerType.LOADBALANCER)
                        .withNewConfiguration()
                            .withControllerClass("metalLB-class")
                        .endConfiguration()
                        .withTls(true)
                        .build())
                .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check Service Class
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getSpec().getLoadBalancerClass(), is("metalLB-class"));

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services) {
            assertThat(service.getSpec().getLoadBalancerClass(), is("metalLB-class"));
        }
    }

    @ParallelTest
    public void testExternalNodePorts() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewKafkaListenerAuthenticationTlsAuth()
                                .endKafkaListenerAuthenticationTlsAuth()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        
        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(false));
            } else {
                assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(true));
            }
        }));

        // Check external bootstrap service
        List<Service> bootstrapServices = kc.generateExternalBootstrapServices();
        assertThat(bootstrapServices.size(), is(1));

        assertThat(bootstrapServices.get(0).getMetadata().getName(), is(CLUSTER + "-kafka-external-bootstrap"));
        assertThat(bootstrapServices.get(0).getSpec().getType(), is("NodePort"));
        assertThat(bootstrapServices.get(0).getSpec().getSelector(), is(expectedBrokerSelectorLabels()));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().size(), is(1));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        TestUtils.checkOwnerReference(bootstrapServices.get(0), KAFKA);

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services)    {
            if (service.getMetadata().getName().contains("foo-brokers-")) {
                assertThat(service.getMetadata().getName(), startsWith("foo-brokers-"));
                assertThat(service.getSpec().getSelector().get(Labels.STRIMZI_POD_NAME_LABEL), oneOf("foo-brokers-5", "foo-brokers-6", "foo-brokers-7"));
                TestUtils.checkOwnerReference(service, POOL_BROKERS);
            } else {
                assertThat(service.getMetadata().getName(), startsWith("foo-mixed-"));
                assertThat(service.getSpec().getSelector().get(Labels.STRIMZI_POD_NAME_LABEL), oneOf("foo-mixed-3", "foo-mixed-4"));
                TestUtils.checkOwnerReference(service, POOL_MIXED);
            }

            assertThat(service.getSpec().getType(), is("NodePort"));
            assertThat(service.getSpec().getPorts().size(), is(1));
            assertThat(service.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
            assertThat(service.getSpec().getPorts().get(0).getPort(), is(9094));
            assertThat(service.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
            assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
            assertThat(service.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        }
    }

    @ParallelTest
    public void testExternalNodePortWithLabelsAndAnnotations() {
        GenericKafkaListenerConfigurationBootstrap bootstrapConfig = new GenericKafkaListenerConfigurationBootstrapBuilder()
                .withAnnotations(Map.of("external-dns.alpha.kubernetes.io/hostname", "bootstrap.my-ingress.com."))
                .withLabels(Map.of("label", "label-value"))
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig3 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(3)
                .withAnnotations(Map.of("external-dns.alpha.kubernetes.io/hostname", "broker-3.my-ingress.com."))
                .withLabels(Map.of("label", "label-value-3"))
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig6 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(6)
                .withAnnotations(Map.of("external-dns.alpha.kubernetes.io/hostname", "broker-6.my-ingress.com."))
                .withLabels(Map.of("label", "label-value-6"))
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBootstrap(bootstrapConfig)
                                    .withBrokers(brokerConfig3, brokerConfig6)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check annotations
        assertThat(kc.generateExternalBootstrapServices().get(0).getMetadata().getAnnotations(), is(Map.of("external-dns.alpha.kubernetes.io/hostname", "bootstrap.my-ingress.com.")));
        assertThat(kc.generateExternalBootstrapServices().get(0).getMetadata().getLabels().get("label"), is("label-value"));

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services)    {
            if (service.getMetadata().getName().startsWith("foo-brokers-6")) {
                assertThat(service.getMetadata().getAnnotations().get("external-dns.alpha.kubernetes.io/hostname"), is("broker-6.my-ingress.com."));
                assertThat(service.getMetadata().getLabels().get("label"), is("label-value-6"));
            } else if (service.getMetadata().getName().startsWith("foo-mixed-3")) {
                assertThat(service.getMetadata().getAnnotations().get("external-dns.alpha.kubernetes.io/hostname"), is("broker-3.my-ingress.com."));
                assertThat(service.getMetadata().getLabels().get("label"), is("label-value-3"));
            } else {
                assertThat(service.getMetadata().getAnnotations().get("external-dns.alpha.kubernetes.io/hostname"), is(nullValue()));
                assertThat(service.getMetadata().getLabels().get("label"), is(nullValue()));
            }
        }
    }

    @ParallelTest
    public void testExternalNodePortsWithAddressType() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withPreferredNodePortAddressType(NodeAddressType.INTERNAL_DNS)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        
        podSets.forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Check Init container
            Container initCont = pod.getSpec().getInitContainers().stream().findAny().orElse(null);
            Container cont = pod.getSpec().getContainers().stream().findAny().orElseThrow();

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(initCont, is(nullValue()));

                assertThat(pod.getSpec().getVolumes().stream().map(Volume::getName)
                        .filter(KafkaCluster.INIT_VOLUME_NAME::equals).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getVolumeMounts().stream().map(VolumeMount::getName)
                        .filter(KafkaCluster.INIT_VOLUME_NAME::equals).findFirst().orElse(null), is(nullValue()));
            } else {
                assertThat(initCont, is(notNullValue()));
                assertThat(initCont.getEnv().stream().filter(env -> KafkaCluster.ENV_VAR_KAFKA_INIT_EXTERNAL_ADDRESS.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse(null), is("TRUE"));

                assertThat(pod.getSpec().getVolumes().stream().map(Volume::getName)
                        .filter(KafkaCluster.INIT_VOLUME_NAME::equals).findFirst().orElse(null), is(notNullValue()));
                assertThat(cont.getVolumeMounts().stream().map(VolumeMount::getName)
                        .filter(KafkaCluster.INIT_VOLUME_NAME::equals).findFirst().orElse(null), is(notNullValue()));
            }
        }));
    }

    @ParallelTest
    public void testExternalNodePortOverrides() {
        GenericKafkaListenerConfigurationBroker nodePortListenerBrokerConfig3 = new GenericKafkaListenerConfigurationBroker();
        nodePortListenerBrokerConfig3.setBroker(3);
        nodePortListenerBrokerConfig3.setNodePort(32103);

        GenericKafkaListenerConfigurationBroker nodePortListenerBrokerConfig6 = new GenericKafkaListenerConfigurationBroker();
        nodePortListenerBrokerConfig6.setBroker(6);
        nodePortListenerBrokerConfig6.setNodePort(32106);

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
            .editSpec()
                .editKafka()
                    .withListeners(new GenericKafkaListenerBuilder()
                            .withName("external")
                            .withPort(9094)
                            .withType(KafkaListenerType.NODEPORT)
                            .withTls(false)
                            .withNewConfiguration()
                                .withNewBootstrap()
                                    .withNodePort(32001)
                                .endBootstrap()
                                .withBrokers(nodePortListenerBrokerConfig3, nodePortListenerBrokerConfig6)
                            .endConfiguration()
                            .build())
                .endKafka()
            .endSpec()
            .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        
        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();

            if ((CLUSTER + "-controllers").equals(podSet.getMetadata().getName())) {
                assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(false));
            } else {
                assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(true));
            }
        }));

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getMetadata().getName(), is(CLUSTER + "-kafka-external-bootstrap"));
        assertThat(ext.getSpec().getType(), is("NodePort"));
        assertThat(ext.getSpec().getSelector(), is(expectedBrokerSelectorLabels()));
        assertThat(ext.getSpec().getPorts().size(), is(1));
        assertThat(ext.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(ext.getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(ext.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
        assertThat(ext.getSpec().getPorts().get(0).getNodePort(), is(32001));
        assertThat(ext.getSpec().getPorts().get(0).getProtocol(), is("TCP"));

        TestUtils.checkOwnerReference(ext, KAFKA);

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services) {
            if (service.getMetadata().getName().startsWith("foo-brokers-6")) {
                assertThat(service.getSpec().getPorts().size(), is(1));
                assertThat(service.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
                assertThat(service.getSpec().getPorts().get(0).getPort(), is(9094));
                assertThat(service.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
                assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(32106));
                assertThat(service.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            } else if (service.getMetadata().getName().startsWith("foo-mixed-3")) {
                assertThat(service.getSpec().getPorts().size(), is(1));
                assertThat(service.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
                assertThat(service.getSpec().getPorts().get(0).getPort(), is(9094));
                assertThat(service.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
                assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(32103));
                assertThat(service.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            } else {
                assertThat(service.getSpec().getPorts().size(), is(1));
                assertThat(service.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
                assertThat(service.getSpec().getPorts().get(0).getPort(), is(9094));
                assertThat(service.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
                assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
                assertThat(service.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            }
        }
    }

    @ParallelTest
    public void testNodePortListenerWithExternalIPs() {
        // set externalIP
        GenericKafkaListenerConfigurationBootstrap bootstrapConfig = new GenericKafkaListenerConfigurationBootstrapBuilder()
                .withNodePort(32100)
                .withExternalIPs(List.of("10.0.0.1"))
                .build();

        GenericKafkaListenerConfigurationBroker nodePortListenerBrokerConfig3 = new GenericKafkaListenerConfigurationBroker();
        nodePortListenerBrokerConfig3.setBroker(3);
        nodePortListenerBrokerConfig3.setNodePort(32003);
        nodePortListenerBrokerConfig3.setExternalIPs(List.of("10.0.0.3"));

        GenericKafkaListenerConfigurationBroker nodePortListenerBrokerConfig6 = new GenericKafkaListenerConfigurationBroker();
        nodePortListenerBrokerConfig6.setBroker(6);
        nodePortListenerBrokerConfig6.setNodePort(32006);
        nodePortListenerBrokerConfig6.setExternalIPs(List.of("10.0.0.6"));

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withBootstrap(bootstrapConfig)
                                    .withBrokers(nodePortListenerBrokerConfig3, nodePortListenerBrokerConfig6)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<Service> bootstrapServices = kc.generateExternalBootstrapServices();
        assertThat(bootstrapServices.get(0).getSpec().getExternalIPs(), is(List.of("10.0.0.1")));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getNodePort(), is(32100));

        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services)    {
            if (service.getMetadata().getName().startsWith("foo-brokers-6")) {
                assertThat(service.getSpec().getExternalIPs(), is(List.of("10.0.0.6")));
                assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(32006));
            } else if (service.getMetadata().getName().startsWith("foo-mixed-3")) {
                assertThat(service.getSpec().getExternalIPs(), is(List.of("10.0.0.3")));
                assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(32003));
            } else {
                assertThat(service.getSpec().getExternalIPs(), is(List.of()));
                assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
            }
        }
    }

    @ParallelTest
    public void testNodePortWithLoadbalancer() {
        GenericKafkaListenerConfigurationBootstrap bootstrapConfig = new GenericKafkaListenerConfigurationBootstrapBuilder()
                .withNodePort(32189)
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig3 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(3)
                .withNodePort(32003)
                .build();

        GenericKafkaListenerConfigurationBroker brokerConfig6 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(6)
                .withNodePort(32006)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withPort(9094)
                        .withType(KafkaListenerType.LOADBALANCER)
                        .withTls(true)
                        .withNewConfiguration()
                        .withBootstrap(bootstrapConfig)
                        .withBrokers(brokerConfig3, brokerConfig6)
                        .endConfiguration()
                        .build())
                .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().size(), is(1));
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().get(0).getNodePort(), is(32189));
        assertThat(kc.generateExternalBootstrapServices().get(0).getSpec().getPorts().get(0).getProtocol(), is("TCP"));

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services) {
            if (service.getMetadata().getName().startsWith("foo-brokers-6")) {
                assertThat(service.getSpec().getPorts().size(), is(1));
                assertThat(service.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
                assertThat(service.getSpec().getPorts().get(0).getPort(), is(9094));
                assertThat(service.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
                assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(32006));
                assertThat(service.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            } else if (service.getMetadata().getName().startsWith("foo-mixed-3")) {
                assertThat(service.getSpec().getPorts().size(), is(1));
                assertThat(service.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
                assertThat(service.getSpec().getPorts().get(0).getPort(), is(9094));
                assertThat(service.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
                assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(32003));
                assertThat(service.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            } else {
                assertThat(service.getSpec().getPorts().size(), is(1));
                assertThat(service.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
                assertThat(service.getSpec().getPorts().get(0).getPort(), is(9094));
                assertThat(service.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
                assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
                assertThat(service.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
            }
        }
    }

    @ParallelTest
    public void testPublishNotReadyAddressesFromListener() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("nodeport")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withNewConfiguration()
                                    .withPublishNotReadyAddresses(true)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        // Check external bootstrap service
        Service ext = kc.generateExternalBootstrapServices().get(0);
        assertThat(ext.getSpec().getPublishNotReadyAddresses(), is(true));

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services)    {
            assertThat(service.getSpec().getPublishNotReadyAddresses(), is(true));
        }
    }

    @SuppressWarnings({"checkstyle:MethodLength"})
    @ParallelTest
    public void testExternalIngress() {
        GenericKafkaListenerConfigurationBroker broker3 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-kafka-broker-3.com")
                .withLabels(Map.of("label", "label-value-3"))
                .withAnnotations(Map.of("dns-annotation", "my-kafka-broker-3.com"))
                .withBroker(3)
                .build();

        GenericKafkaListenerConfigurationBroker broker4 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-kafka-broker-4.com")
                .withLabels(Map.of("label", "label-value-4"))
                .withAnnotations(Map.of("dns-annotation", "my-kafka-broker-4.com"))
                .withBroker(4)
                .build();

        GenericKafkaListenerConfigurationBroker broker5 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-kafka-broker-5.com")
                .withLabels(Map.of("label", "label-value-5"))
                .withAnnotations(Map.of("dns-annotation", "my-kafka-broker-5.com"))
                .withBroker(5)
                .build();

        GenericKafkaListenerConfigurationBroker broker6 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-kafka-broker-6.com")
                .withLabels(Map.of("label", "label-value-6"))
                .withAnnotations(Map.of("dns-annotation", "my-kafka-broker-6.com"))
                .withBroker(6)
                .build();

        GenericKafkaListenerConfigurationBroker broker7 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-kafka-broker-7.com")
                .withLabels(Map.of("label", "label-value-7"))
                .withAnnotations(Map.of("dns-annotation", "my-kafka-broker-7.com"))
                .withBroker(7)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.INGRESS)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withNewBootstrap()
                                        .withHost("my-kafka-bootstrap.com")
                                        .withAnnotations(Map.of("dns-annotation", "my-kafka-bootstrap.com"))
                                        .withLabels(Map.of("label", "label-value"))
                                    .endBootstrap()
                                    .withBrokers(broker3, broker4, broker5, broker6, broker7)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        assertThat(kc.getListeners().stream().findFirst().orElseThrow().getType(), is(KafkaListenerType.INGRESS));

        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());
        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(false));
            } else {
                assertThat(ports.contains(ContainerUtils.createContainerPort(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME, 9094)), is(true));
            }
        }));

        // Check external bootstrap service
        List<Service> bootstrapServices = kc.generateExternalBootstrapServices();
        assertThat(bootstrapServices.size(), is(1));

        assertThat(bootstrapServices.get(0).getMetadata().getName(), is(CLUSTER + "-kafka-external-bootstrap"));
        assertThat(bootstrapServices.get(0).getSpec().getType(), is("ClusterIP"));
        assertThat(bootstrapServices.get(0).getSpec().getSelector(), is(expectedBrokerSelectorLabels()));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().size(), is(1));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        TestUtils.checkOwnerReference(bootstrapServices.get(0), KAFKA);

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services)    {
            if (service.getMetadata().getName().contains("foo-brokers-")) {
                assertThat(service.getMetadata().getName(), startsWith("foo-brokers-"));
                assertThat(service.getSpec().getSelector().get(Labels.STRIMZI_POD_NAME_LABEL), oneOf("foo-brokers-5", "foo-brokers-6", "foo-brokers-7"));
                TestUtils.checkOwnerReference(service, POOL_BROKERS);
            } else {
                assertThat(service.getMetadata().getName(), startsWith("foo-mixed-"));
                assertThat(service.getSpec().getSelector().get(Labels.STRIMZI_POD_NAME_LABEL), oneOf("foo-mixed-3", "foo-mixed-4"));
                TestUtils.checkOwnerReference(service, POOL_MIXED);
            }

            assertThat(service.getSpec().getType(), is("ClusterIP"));
            assertThat(service.getSpec().getPorts().size(), is(1));
            assertThat(service.getSpec().getPorts().get(0).getName(), is(ListenersUtils.BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME));
            assertThat(service.getSpec().getPorts().get(0).getPort(), is(9094));
            assertThat(service.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-external"));
            assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
            assertThat(service.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        }

        // Check bootstrap ingress
        List<Ingress> bootstrapIngresses = kc.generateExternalBootstrapIngresses();
        assertThat(bootstrapIngresses.size(), is(1));

        assertThat(bootstrapIngresses.get(0).getMetadata().getName(), is(KafkaResources.bootstrapServiceName(CLUSTER)));
        assertThat(bootstrapIngresses.get(0).getSpec().getIngressClassName(), is(nullValue()));
        assertThat(bootstrapIngresses.get(0).getMetadata().getAnnotations().get("dns-annotation"), is("my-kafka-bootstrap.com"));
        assertThat(bootstrapIngresses.get(0).getMetadata().getLabels().get("label"), is("label-value"));
        assertThat(bootstrapIngresses.get(0).getSpec().getTls().size(), is(1));
        assertThat(bootstrapIngresses.get(0).getSpec().getTls().get(0).getHosts().size(), is(1));
        assertThat(bootstrapIngresses.get(0).getSpec().getTls().get(0).getHosts().get(0), is("my-kafka-bootstrap.com"));
        assertThat(bootstrapIngresses.get(0).getSpec().getRules().size(), is(1));
        assertThat(bootstrapIngresses.get(0).getSpec().getRules().get(0).getHost(), is("my-kafka-bootstrap.com"));
        assertThat(bootstrapIngresses.get(0).getSpec().getRules().get(0).getHttp().getPaths().size(), is(1));
        assertThat(bootstrapIngresses.get(0).getSpec().getRules().get(0).getHttp().getPaths().get(0).getPath(), is("/"));
        assertThat(bootstrapIngresses.get(0).getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getService().getName(), is(CLUSTER + "-kafka-external-bootstrap"));
        assertThat(bootstrapIngresses.get(0).getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getService().getPort().getNumber(), is(9094));
        TestUtils.checkOwnerReference(bootstrapIngresses.get(0), KAFKA);

        // Check per pod ingress
        List<Ingress> ingresses = kc.generateExternalIngresses();
        assertThat(ingresses.size(), is(5));

        for (Ingress ingress : ingresses)    {
            if (ingress.getMetadata().getName().contains("foo-brokers-")) {
                assertThat(ingress.getMetadata().getName(), oneOf("foo-brokers-5", "foo-brokers-6", "foo-brokers-7"));
                TestUtils.checkOwnerReference(ingress, POOL_BROKERS);
            } else {
                assertThat(ingress.getMetadata().getName(), oneOf("foo-mixed-3", "foo-mixed-4"));
                TestUtils.checkOwnerReference(ingress, POOL_MIXED);
            }

            assertThat(ingress.getSpec().getIngressClassName(), is(nullValue()));
            assertThat(ingress.getMetadata().getAnnotations().get("dns-annotation"), oneOf("my-kafka-broker-3.com", "my-kafka-broker-4.com", "my-kafka-broker-5.com", "my-kafka-broker-6.com", "my-kafka-broker-7.com"));
            assertThat(ingress.getMetadata().getLabels().get("label"), oneOf("label-value-3", "label-value-4", "label-value-5", "label-value-6", "label-value-7"));
            assertThat(ingress.getSpec().getIngressClassName(), is(nullValue()));
            assertThat(ingress.getSpec().getTls().size(), is(1));
            assertThat(ingress.getSpec().getTls().get(0).getHosts().size(), is(1));
            assertThat(ingress.getSpec().getTls().get(0).getHosts().get(0), oneOf("my-kafka-broker-3.com", "my-kafka-broker-4.com", "my-kafka-broker-5.com", "my-kafka-broker-6.com", "my-kafka-broker-7.com"));
            assertThat(ingress.getSpec().getRules().size(), is(1));
            assertThat(ingress.getSpec().getRules().get(0).getHost(), oneOf("my-kafka-broker-3.com", "my-kafka-broker-4.com", "my-kafka-broker-5.com", "my-kafka-broker-6.com", "my-kafka-broker-7.com"));
            assertThat(ingress.getSpec().getRules().get(0).getHttp().getPaths().size(), is(1));
            assertThat(ingress.getSpec().getRules().get(0).getHttp().getPaths().get(0).getPath(), is("/"));
            assertThat(ingress.getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getService().getName(), oneOf("foo-mixed-3", "foo-mixed-4", "foo-brokers-5", "foo-brokers-6", "foo-brokers-7"));
            assertThat(ingress.getSpec().getRules().get(0).getHttp().getPaths().get(0).getBackend().getService().getPort().getNumber(), is(9094));
        }
    }

    @ParallelTest
    public void testExternalIngressClass() {
        GenericKafkaListenerConfigurationBroker broker3 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-kafka-broker-3.com")
                .withBroker(3)
                .build();

        GenericKafkaListenerConfigurationBroker broker4 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-kafka-broker-4.com")
                .withBroker(4)
                .build();

        GenericKafkaListenerConfigurationBroker broker5 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-kafka-broker-5.com")
                .withBroker(5)
                .build();

        GenericKafkaListenerConfigurationBroker broker6 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-kafka-broker-6.com")
                .withBroker(6)
                .build();

        GenericKafkaListenerConfigurationBroker broker7 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withHost("my-kafka-broker-7.com")
                .withBroker(7)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.INGRESS)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withControllerClass("nginx-internal")
                                    .withNewBootstrap()
                                        .withHost("my-kafka-bootstrap.com")
                                        .withAnnotations(Map.of("dns-annotation", "my-kafka-bootstrap.com"))
                                    .endBootstrap()
                                    .withBrokers(broker3, broker4, broker5, broker6, broker7)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

         // Check bootstrap ingress
        Ingress bing = kc.generateExternalBootstrapIngresses().get(0);
        assertThat(bing.getSpec().getIngressClassName(), is("nginx-internal"));

        // Check per pod ingress
        List<Ingress> ingresses = kc.generateExternalIngresses();
        assertThat(ingresses.size(), is(5));

        for (Ingress ingress : ingresses)    {
            assertThat(ingress.getSpec().getIngressClassName(), is("nginx-internal"));
        }
    }

    @ParallelTest
    public void testExternalIngressMissingConfiguration() {
        GenericKafkaListenerConfigurationBroker broker0 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withBroker(0)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.INGRESS)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withControllerClass("nginx-internal")
                                    .withNewBootstrap()
                                        .withHost("my-kafka-bootstrap.com")
                                        .withAnnotations(Map.of("dns-annotation", "my-kafka-bootstrap.com"))
                                    .endBootstrap()
                                    .withBrokers(broker0)
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();

        assertThrows(InvalidResourceException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testClusterIP() {
        GenericKafkaListenerConfigurationBroker broker3 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withAdvertisedHost("my-ingress-3.com")
                .withAdvertisedPort(9991)
                .withLabels(Map.of("label", "label-value-3"))
                .withAnnotations(Map.of("anno", "anno-value-3"))
                .withBroker(3)
                .build();

        GenericKafkaListenerConfigurationBroker broker6 = new GenericKafkaListenerConfigurationBrokerBuilder()
                .withAdvertisedHost("my-ingress-6.com")
                .withAdvertisedPort(9992)
                .withLabels(Map.of("label", "label-value-6"))
                .withAnnotations(Map.of("anno", "anno-value-6"))
                .withBroker(6)
                .build();

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("clusterip")
                        .withPort(9094)
                        .withType(KafkaListenerType.CLUSTER_IP)
                        .withTls(true)
                        .withNewConfiguration()
                            .withBrokers(broker3, broker6)
                        .endConfiguration()
                        .build())
                .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        
        assertThat(kc.getListeners().stream().findFirst().orElseThrow().getType(), is(KafkaListenerType.CLUSTER_IP));
        
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            List<ContainerPort> ports = pod.getSpec().getContainers().stream().findAny().orElseThrow().getPorts();

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(ports.contains(ContainerUtils.createContainerPort("tcp-clusterip", 9094)), is(false));
            } else {
                assertThat(ports.contains(ContainerUtils.createContainerPort("tcp-clusterip", 9094)), is(true));
            }
        }));

        // Check external bootstrap service
        List<Service> bootstrapServices = kc.generateExternalBootstrapServices();
        assertThat(bootstrapServices.size(), is(1));

        assertThat(bootstrapServices.get(0).getMetadata().getName(), is("foo-kafka-clusterip-bootstrap"));
        assertThat(bootstrapServices.get(0).getSpec().getType(), is("ClusterIP"));
        assertThat(bootstrapServices.get(0).getSpec().getSelector(), is(expectedBrokerSelectorLabels()));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().size(), is(1));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getName(), is("tcp-clusterip"));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getPort(), is(9094));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-clusterip"));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
        assertThat(bootstrapServices.get(0).getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        TestUtils.checkOwnerReference(bootstrapServices.get(0), KAFKA);

        // Check per pod services
        List<Service> services = kc.generatePerPodServices();
        assertThat(services.size(), is(5));

        for (Service service : services) {
            if (service.getMetadata().getName().startsWith("foo-brokers-clusterip-6")) {
                assertThat(service.getMetadata().getAnnotations().get("anno"), is("anno-value-6"));
                assertThat(service.getMetadata().getLabels().get("label"), is("label-value-6"));
                TestUtils.checkOwnerReference(service, POOL_BROKERS);
            } else if (service.getMetadata().getName().startsWith("foo-mixed-clusterip-3")) {
                assertThat(service.getMetadata().getAnnotations().get("anno"), is("anno-value-3"));
                assertThat(service.getMetadata().getLabels().get("label"), is("label-value-3"));
                TestUtils.checkOwnerReference(service, POOL_MIXED);
            } else {
                assertThat(service.getMetadata().getAnnotations().get("anno"), is(nullValue()));
                assertThat(service.getMetadata().getLabels().get("label"), is(nullValue()));

                if (service.getMetadata().getName().startsWith("foo-controllers-")) {
                    TestUtils.checkOwnerReference(service, POOL_CONTROLLERS);
                } else if (service.getMetadata().getName().startsWith("foo-mixed-")) {
                    TestUtils.checkOwnerReference(service, POOL_MIXED);
                } else {
                    TestUtils.checkOwnerReference(service, POOL_BROKERS);
                }
            }

            assertThat(service.getMetadata().getName(), oneOf("foo-mixed-clusterip-3", "foo-mixed-clusterip-4", "foo-brokers-clusterip-5", "foo-brokers-clusterip-6", "foo-brokers-clusterip-7"));
            assertThat(service.getSpec().getType(), is("ClusterIP"));
            assertThat(service.getSpec().getSelector().get(Labels.STRIMZI_POD_NAME_LABEL), oneOf("foo-mixed-3", "foo-mixed-4", "foo-brokers-5", "foo-brokers-6", "foo-brokers-7"));
            assertThat(service.getSpec().getPorts().size(), is(1));
            assertThat(service.getSpec().getPorts().get(0).getName(), is("tcp-clusterip"));
            assertThat(service.getSpec().getPorts().get(0).getPort(), is(9094));
            assertThat(service.getSpec().getPorts().get(0).getTargetPort().getStrVal(), is("tcp-clusterip"));
            assertThat(service.getSpec().getPorts().get(0).getNodePort(), is(nullValue()));
            assertThat(service.getSpec().getPorts().get(0).getProtocol(), is("TCP"));
        }
    }

    @ParallelTest
    public void testClusterIPMissingConfiguration() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("external")
                        .withType(KafkaListenerType.CLUSTER_IP)
                        .withTls(false)
                        .build())
                .endKafka()
                .endSpec()
                .build();

        assertThrows(InvalidResourceException.class, () -> {
            List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
            KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        });
    }

    @ParallelTest
    public void testExternalServiceWithDualStackNetworking() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                    .withName("np")
                                    .withPort(9094)
                                    .withType(KafkaListenerType.NODEPORT)
                                    .withTls(true)
                                    .withNewConfiguration()
                                        .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                                        .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                                    .endConfiguration()
                                    .build(),
                                new GenericKafkaListenerBuilder()
                                    .withName("lb")
                                    .withPort(9095)
                                    .withType(KafkaListenerType.LOADBALANCER)
                                    .withTls(true)
                                    .withNewConfiguration()
                                        .withIpFamilyPolicy(IpFamilyPolicy.PREFER_DUAL_STACK)
                                        .withIpFamilies(IpFamily.IPV6, IpFamily.IPV4)
                                    .endConfiguration()
                                    .build())
                    .endKafka()
                .endSpec()
                .build();

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);

        List<Service> services = new ArrayList<>();
        services.addAll(kc.generateExternalBootstrapServices());
        services.addAll(kc.generatePerPodServices());

        for (Service svc : services)    {
            assertThat(svc.getSpec().getIpFamilyPolicy(), is("PreferDualStack"));
            assertThat(svc.getSpec().getIpFamilies(), contains("IPv6", "IPv4"));
        }
    }

    @ParallelTest
    public void testCustomAuthSecretsAreMounted() {
        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                .editKafka()
                .withListeners(new GenericKafkaListenerBuilder()
                        .withName("plain")
                        .withPort(9092)
                        .withType(KafkaListenerType.INTERNAL)
                        .withTls(false)
                        .withAuth(
                                new KafkaListenerAuthenticationCustomBuilder()
                                        .withSecrets(new GenericSecretSourceBuilder().withSecretName("test").withKey("foo").build(),
                                                new GenericSecretSourceBuilder().withSecretName("test2").withKey("bar").build())
                                        .build())
                        .build())
                .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            Container cont = pod.getSpec().getContainers().stream().findFirst().orElseThrow();
            List<Volume> volumes = pod.getSpec().getVolumes();

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "custom-listener-plain-9092-0".equals(mount.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "custom-listener-plain-9092-1".equals(mount.getName())).findFirst().orElse(null), is(nullValue()));

                assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-0".equals(vol.getName())).findFirst().orElse(null), is(nullValue()));
                assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-1".equals(vol.getName())).findFirst().orElse(null), is(nullValue()));
            } else {
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "custom-listener-plain-9092-0".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.CUSTOM_AUTHN_SECRETS_VOLUME_MOUNT + "/custom-listener-plain-9092/test"));
                assertThat(cont.getVolumeMounts().stream().filter(mount -> "custom-listener-plain-9092-1".equals(mount.getName())).findFirst().orElseThrow().getMountPath(), is(KafkaCluster.CUSTOM_AUTHN_SECRETS_VOLUME_MOUNT + "/custom-listener-plain-9092/test2"));

                assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
                assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("foo"));
                assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-0".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("foo"));
                assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().size(), is(1));
                assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getKey(), is("bar"));
                assertThat(volumes.stream().filter(vol -> "custom-listener-plain-9092-1".equals(vol.getName())).findFirst().orElseThrow().getSecret().getItems().get(0).getPath(), is("bar"));
            }
        }));
    }

    @ParallelTest
    public void testExternalCertificateIngress() {
        String cert = "my-external-cert.crt";
        String key = "my.key";
        String secret = "my-secret";

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.NODEPORT)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withNewBrokerCertChainAndKey()
                                        .withCertificate(cert)
                                        .withKey(key)
                                        .withSecretName(secret)
                                    .endBrokerCertChainAndKey()
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Volumes
            List<Volume> volumes = pod.getSpec().getVolumes();
            Volume vol = volumes.stream().filter(v -> "custom-external-9094-certs".equals(v.getName())).findFirst().orElse(null);

            // Volume mounts
            Container container = pod.getSpec().getContainers().stream().findFirst().orElseThrow();
            VolumeMount mount = container.getVolumeMounts().stream().filter(v -> "custom-external-9094-certs".equals(v.getName())).findFirst().orElse(null);

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(vol, is(nullValue()));
                assertThat(mount, is(nullValue()));
            } else {
                assertThat(vol, is(notNullValue()));
                assertThat(vol.getSecret().getSecretName(), is(secret));
                assertThat(vol.getSecret().getItems().get(0).getKey(), is(key));
                assertThat(vol.getSecret().getItems().get(0).getPath(), is("tls.key"));
                assertThat(vol.getSecret().getItems().get(1).getKey(), is(cert));
                assertThat(vol.getSecret().getItems().get(1).getPath(), is("tls.crt"));

                assertThat(mount, is(notNullValue()));
                assertThat(mount.getName(), is("custom-external-9094-certs"));
                assertThat(mount.getMountPath(), is("/opt/kafka/certificates/custom-external-9094-certs"));
            }
        }));
    }

    @ParallelTest
    public void testCustomCertificateTls() {
        String cert = "my-external-cert.crt";
        String key = "my.key";
        String secret = "my-secret";

        Kafka kafkaAssembly = new KafkaBuilder(KAFKA)
                .editSpec()
                    .editKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("tls")
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withNewConfiguration()
                                    .withNewBrokerCertChainAndKey()
                                        .withCertificate(cert)
                                        .withKey(key)
                                        .withSecretName(secret)
                                    .endBrokerCertChainAndKey()
                                .endConfiguration()
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, List.of(POOL_CONTROLLERS, POOL_MIXED, POOL_BROKERS), Map.of(), KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, SHARED_ENV_PROVIDER);
        KafkaCluster kc = KafkaCluster.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly, pools, VERSIONS, KafkaVersionTestUtils.DEFAULT_KRAFT_VERSION_CHANGE, null, SHARED_ENV_PROVIDER);
        List<StrimziPodSet> podSets = kc.generatePodSets(true, null, null, node -> Map.of());

        podSets.stream().forEach(podSet -> PodSetUtils.podSetToPods(podSet).stream().forEach(pod -> {
            // Test volumes
            List<Volume> volumes = pod.getSpec().getVolumes();
            Volume vol = volumes.stream().filter(v -> "custom-tls-9093-certs".equals(v.getName())).findFirst().orElse(null);

            // Test volume mounts
            Container container = pod.getSpec().getContainers().stream().findAny().orElseThrow();
            VolumeMount mount = container.getVolumeMounts().stream().filter(v -> "custom-tls-9093-certs".equals(v.getName())).findFirst().orElse(null);

            if (pod.getMetadata().getName().startsWith(CLUSTER + "-controllers")) {
                assertThat(vol, is(nullValue()));
                assertThat(mount, is(nullValue()));
            } else {
                assertThat(vol, is(notNullValue()));
                assertThat(vol.getSecret().getSecretName(), is(secret));
                assertThat(vol.getSecret().getItems().get(0).getKey(), is(key));
                assertThat(vol.getSecret().getItems().get(0).getPath(), is("tls.key"));
                assertThat(vol.getSecret().getItems().get(1).getKey(), is(cert));
                assertThat(vol.getSecret().getItems().get(1).getPath(), is("tls.crt"));

                assertThat(mount, is(notNullValue()));
                assertThat(mount.getName(), is("custom-tls-9093-certs"));
                assertThat(mount.getMountPath(), is("/opt/kafka/certificates/custom-tls-9093-certs"));
            }
        }));
    }
}
