/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.olm;

import io.strimzi.api.kafka.model.KafkaBridgeResources;
import io.strimzi.api.kafka.model.KafkaConnectResources;
import io.strimzi.api.kafka.model.KafkaMirrorMakerResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.MessagingBaseST;
import io.strimzi.systemtest.annotations.SeleniumFirefox;
import io.strimzi.systemtest.selenium.SeleniumProvider;
import io.strimzi.systemtest.selenium.UserCredentials;
import io.strimzi.systemtest.selenium.page.Openshift4WebPage;
import io.strimzi.systemtest.utils.StUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

import static io.strimzi.systemtest.Constants.OLM;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag(OLM)
@SeleniumFirefox
class OperatorLifecycleManagerST extends MessagingBaseST {
    private static final Logger LOGGER = LogManager.getLogger(OperatorLifecycleManagerST.class);
    private static final String MARKETPLACE_NAMESPACE = "openshift-marketplace";
    private static final String INFRA_NAMESPACE = "openshift-operators";
    private static final String USER_NAME = "my-user";
    private static final String TOPIC_NAME = "my-topic";
    private final UserCredentials clusterUser = new UserCredentials("admin");

    @AfterAll
    void cleanRestOfResources() {
        cmdKubeClient().exec("oc", "delete", "all", "--selector", "app=strimzi", "-n", INFRA_NAMESPACE);
        cmdKubeClient().exec("oc", "delete", "crd", "-l", "app=strimzi", "-n", INFRA_NAMESPACE);
        cmdKubeClient().exec("oc", "delete", "apiservices", "-l", "app=strimzi", "-n", INFRA_NAMESPACE);
        cmdKubeClient().exec("oc", "delete", "cm", "-l", "app=strimzi", "-n", INFRA_NAMESPACE);
        cmdKubeClient().exec("oc", "delete", "secret", "-l", "app=strimzi", "-n", INFRA_NAMESPACE);
        try {
            cmdKubeClient().exec("oc", "delete", "subscription", Environment.APP_NAME, "-n", INFRA_NAMESPACE);
            cmdKubeClient().exec("oc", "delete", "deployment", Environment.APP_NAME + "-cluster-operator", "-n", INFRA_NAMESPACE);
        } catch (Exception ex) {
            LOGGER.info("Operator has been already deleted");
        }

        deleteNamespaces();
    }

    @Test
    @Order(1)
    void installOperator() throws Exception {
        Openshift4WebPage page = new Openshift4WebPage(SeleniumProvider.getInstance(), getOCConsoleRoute(), clusterUser);
        page.openOpenshiftPage();
        page.installFromCatalog(Environment.OPERATOR_NAME);
        setNamespace(INFRA_NAMESPACE);
        StUtils.waitForDeploymentReady(Environment.APP_NAME + "-cluster-operator", 1);
    }

    @Test
    @Order(2)
    void testCreateExampleKafkaResources() throws Exception {
        Openshift4WebPage page = new Openshift4WebPage(SeleniumProvider.getInstance(), getOCConsoleRoute(), clusterUser);
        page.openOpenshiftPage();
        page.openInstalledOperators();
        page.selectNamespaceFromBar(INFRA_NAMESPACE);
        page.selectOperator(Environment.OPERATOR_NAME);
        page.createExampleResourceItem("kafka");

        setNamespace(INFRA_NAMESPACE);
        StUtils.waitForAllStatefulSetPodsReady(KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME), 3);
        LOGGER.info("StatefulSet {} is ready", KafkaResources.zookeeperStatefulSetName(CLUSTER_NAME));
        StUtils.waitForAllStatefulSetPodsReady(KafkaResources.kafkaStatefulSetName(CLUSTER_NAME), 3);
        LOGGER.info("StatefulSet {} is ready", KafkaResources.kafkaStatefulSetName(CLUSTER_NAME));
        StUtils.waitForDeploymentReady(KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME), 1);
        LOGGER.info("Deployment {} is ready", KafkaResources.entityOperatorDeploymentName(CLUSTER_NAME));

        page.createExampleResourceItem("kafkatopic");
        page.createExampleResourceItem("kafkauser");

        StUtils.waitForSecretReady(USER_NAME);
    }

    @Test
    @Order(3)
    void testCreateExampleBridgeResources() throws Exception {
        Openshift4WebPage page = new Openshift4WebPage(SeleniumProvider.getInstance(), getOCConsoleRoute(), clusterUser);
        page.openOpenshiftPage();
        page.openInstalledOperators();
        page.selectNamespaceFromBar(INFRA_NAMESPACE);
        page.selectOperator(Environment.OPERATOR_NAME);
        page.createExampleResourceItem("kafkabridge");

        setNamespace(INFRA_NAMESPACE);
        StUtils.waitForDeploymentReady(KafkaBridgeResources.deploymentName("my-bridge"), 1);
    }

    @Test
    @Order(4)
    void testCreateExampleConnectResources() throws Exception {
        Openshift4WebPage page = new Openshift4WebPage(SeleniumProvider.getInstance(), getOCConsoleRoute(), clusterUser);
        page.openOpenshiftPage();
        page.openInstalledOperators();
        page.selectNamespaceFromBar(INFRA_NAMESPACE);
        page.selectOperator(Environment.OPERATOR_NAME);
        page.createExampleResourceItem("kafkaconnect");

        setNamespace(INFRA_NAMESPACE);
        StUtils.waitForDeploymentReady(KafkaConnectResources.deploymentName("my-connect-cluster"), 1);
    }

    @Test
    @Order(5)
    @Disabled("ConnectS2I is currently not working properly")
    void testCreateExampleConnectS2IResources() throws Exception {
        Openshift4WebPage page = new Openshift4WebPage(SeleniumProvider.getInstance(), getOCConsoleRoute(), clusterUser);
        page.openOpenshiftPage();
        page.openInstalledOperators();
        page.selectNamespaceFromBar(INFRA_NAMESPACE);
        page.selectOperator(Environment.OPERATOR_NAME);
        page.createExampleResourceItem("kafkaconnects2i");

        setNamespace(INFRA_NAMESPACE);
        StUtils.waitForDeploymentReady(KafkaConnectResources.deploymentName(CLUSTER_NAME), 1);
    }

    @Test
    @Order(6)
    void testCreateMirrorMakerResources() throws Exception {
        setNamespace(INFRA_NAMESPACE);
        createTestMethodResources();
        testMethodResources().kafkaEphemeral("my-source-cluster", 1, 1).done();
        testMethodResources().kafkaEphemeral("my-target-cluster", 1, 1).done();

        Openshift4WebPage page = new Openshift4WebPage(SeleniumProvider.getInstance(), getOCConsoleRoute(), clusterUser);
        page.openOpenshiftPage();
        page.openInstalledOperators();
        page.selectNamespaceFromBar(INFRA_NAMESPACE);
        page.selectOperator(Environment.OPERATOR_NAME);
        page.createExampleResourceItem("kafkamirrormaker");

        StUtils.waitForDeploymentReady(KafkaMirrorMakerResources.deploymentName("my-mirror-maker"), 1);
        deleteTestMethodResources();
    }

    @Test
    @Order(7)
    void testBasicMessagingAfterOlmInstallation() throws Exception {
        createTestMethodResources();
        KafkaUser user = testMethodResources().kafkaUser().inNamespace(INFRA_NAMESPACE).withName(USER_NAME).get();
        testMethodResources().createServiceResource(Constants.KAFKA_CLIENTS, Environment.KAFKA_CLIENTS_DEFAULT_PORT, INFRA_NAMESPACE).done();
        testMethodResources().createIngress(Constants.KAFKA_CLIENTS, Environment.KAFKA_CLIENTS_DEFAULT_PORT, CONFIG.getMasterUrl(), INFRA_NAMESPACE).done();
        testMethodResources().deployKafkaClients(true, CLUSTER_NAME, INFRA_NAMESPACE, user).done();
        availabilityTest(50, Constants.TIMEOUT_AVAILABILITY_TEST, CLUSTER_NAME, true, TOPIC_NAME, user);
        deleteTestMethodResources();
    }

    @Test
    @Order(8)
    void uninstallOperator() throws Exception {
        Openshift4WebPage page = new Openshift4WebPage(SeleniumProvider.getInstance(), getOCConsoleRoute(), clusterUser);
        page.openOpenshiftPage();
        page.uninstallFromCatalog(Environment.OPERATOR_NAME);
        StUtils.waitForDeploymentDeletion(Environment.APP_NAME + "-cluster-operator");
    }

    @Override
    protected void tearDownEnvironmentAfterAll() {
    }

    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) {
        LOGGER.info("Skip env recreation after failed tests! In OLM tests, every test has to pass!");
    }
}

