/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPassword;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.template.JmxTransOutputDefinitionTemplateBuilder;
import io.strimzi.api.kafka.model.template.JmxTransQueryTemplateBuilder;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedSuite;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.specific.ScraperTemplates;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.specific.JmxUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static io.strimzi.systemtest.Constants.CONNECT;
import static io.strimzi.systemtest.Constants.CONNECT_COMPONENTS;
import static io.strimzi.systemtest.Constants.INFRA_NAMESPACE;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(REGRESSION)
@IsolatedSuite
public class JmxIsolatedST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(JmxIsolatedST.class);

    @ParallelNamespaceTest
    @Tag(CONNECT)
    @Tag(CONNECT_COMPONENTS)
    void testKafkaZookeeperAndKafkaConnectWithJMX(ExtensionContext extensionContext) {
        final String clusterName = mapWithClusterNames.get(extensionContext.getDisplayName());
        final String scraperName = mapWithScraperNames.get(extensionContext.getDisplayName());
        final String namespaceName = StUtils.getNamespaceBasedOnRbac(clusterOperator.getDeploymentNamespace(), extensionContext);
        final String zkSecretName = clusterName + "-zookeeper-jmx";
        final String connectJmxSecretName = clusterName + "-kafka-connect-jmx";
        final String kafkaJmxSecretName = clusterName + "-kafka-jmx";
        final String jmxTransName = clusterName + "-kafka-jmx-trans";

        final String jmxTransMetricTypeName = "KafkaServer";
        final String jmxTransMetricName = "BrokerState";
        final String expectedJmxTransValue = "value=3";

        Map<String, String> jmxSecretLabels = Collections.singletonMap("my-label", "my-value");
        Map<String, String> jmxSecretAnnotations = Collections.singletonMap("my-annotation", "some-value");

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editOrNewSpec()
                .withNewJmxTrans()
                    .withOutputDefinitions(new JmxTransOutputDefinitionTemplateBuilder()
                            .withName("standardOut")
                            .withOutputType("com.googlecode.jmxtrans.model.output.StdOutWriter")
                            .build())
                    .withKafkaQueries(new JmxTransQueryTemplateBuilder()
                            .withTargetMBean("kafka.server:type=" + jmxTransMetricTypeName + ",name=" + jmxTransMetricName)
                            .withAttributes("Value")
                            .withOutputs("standardOut")
                            .build())
                .endJmxTrans()
                .editKafka()
                    .withNewJmxOptions()
                        .withAuthentication(new KafkaJmxAuthenticationPassword())
                    .endJmxOptions()
                .endKafka()
                .editOrNewZookeeper()
                    .withNewJmxOptions()
                        .withAuthentication(new KafkaJmxAuthenticationPassword())
                    .endJmxOptions()
                    .editOrNewTemplate()
                        .withNewJmxSecret()
                            .withNewMetadata()
                                .withLabels(jmxSecretLabels)
                                .withAnnotations(jmxSecretAnnotations)
                            .endMetadata()
                        .endJmxSecret()
                    .endTemplate()
                .endZookeeper()
            .endSpec()
            .build());

        resourceManager.createResource(extensionContext, ScraperTemplates.scraperPod(namespaceName, scraperName).build());
        String scraperPodName = kubeClient().listPodsByPrefixInName(scraperName).get(0).getMetadata().getName();
        JmxUtils.downloadJmxTermToPod(namespaceName, scraperPodName);

        resourceManager.createResource(extensionContext, KafkaConnectTemplates.kafkaConnect(clusterName, namespaceName, 1)
            .editOrNewSpec()
                .withNewJmxOptions()
                    .withAuthentication(new KafkaJmxAuthenticationPassword())
                .endJmxOptions()
            .endSpec()
            .build());

        String kafkaResults = JmxUtils.collectJmxMetricsWithWait(namespaceName, KafkaResources.brokersServiceName(clusterName), kafkaJmxSecretName, scraperPodName, "bean kafka.server:type=app-info\nget -i *");
        String kafkaConnectResults = JmxUtils.collectJmxMetricsWithWait(namespaceName, KafkaConnectResources.serviceName(clusterName), connectJmxSecretName, scraperPodName, "bean kafka.connect:type=app-info\nget -i *");
        assertThat("Result from Kafka JMX doesn't contain right version of Kafka, result: " + kafkaResults, kafkaResults, containsString("version = " + Environment.ST_KAFKA_VERSION));
        assertThat("Result from KafkaConnect JMX doesn't contain right version of Kafka, result: " + kafkaConnectResults, kafkaConnectResults, containsString("version = " + Environment.ST_KAFKA_VERSION));

        String jmxTransResult = JmxUtils.collectJmxTransMetricsWithWait(namespaceName, jmxTransMetricName, jmxTransName);
        // Note: Broker State = 3: Running as Broker
        assertThat("Result from Kafka JmxTrans doesn't contain correct metric" + jmxTransMetricName +  ", result: " + jmxTransResult, jmxTransResult, containsString(expectedJmxTransValue));

        if (!Environment.isKRaftModeEnabled()) {
            Secret jmxZkSecret = kubeClient().getSecret(namespaceName, zkSecretName);

            String zkBeans = JmxUtils.collectJmxMetricsWithWait(namespaceName, KafkaResources.zookeeperHeadlessServiceName(clusterName), zkSecretName, scraperPodName, "domain org.apache.ZooKeeperService\nbeans");
            String zkBean = Arrays.asList(zkBeans.split("\\n")).stream().filter(bean -> bean.matches("org.apache.ZooKeeperService:name[0-9]+=ReplicatedServer_id[0-9]+")).findFirst().get();

            String zkResults = JmxUtils.collectJmxMetricsWithWait(namespaceName, KafkaResources.zookeeperHeadlessServiceName(clusterName), zkSecretName, scraperPodName, "bean " + zkBean + "\nget -i *");
            assertThat("Result from Zookeeper JMX doesn't contain right quorum size, result: " + zkResults, zkResults, containsString("QuorumSize = 3"));

            LOGGER.info("Checking that Zookeeper JMX secret is created with custom labels and annotations");
            assertTrue(jmxZkSecret.getMetadata().getLabels().entrySet().containsAll(jmxSecretLabels.entrySet()));
            assertTrue(jmxZkSecret.getMetadata().getAnnotations().entrySet().containsAll(jmxSecretAnnotations.entrySet()));
        }
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        final String namespaceToWatch = Environment.isNamespaceRbacScope() ? INFRA_NAMESPACE : Constants.WATCH_ALL_NAMESPACES;

        clusterOperator.unInstall();
        clusterOperator = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
            .withNamespace(INFRA_NAMESPACE)
            .withWatchingNamespaces(namespaceToWatch)
            .withOperationTimeout(Constants.CO_OPERATION_TIMEOUT_SHORT)
            .createInstallation()
            .runInstallation();
    }
}
