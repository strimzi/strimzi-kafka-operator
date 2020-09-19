/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.utils.FileUtils;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static io.strimzi.systemtest.Constants.METRICS;
import static io.strimzi.systemtest.Constants.PROMETHEUS;
import static io.strimzi.systemtest.Constants.REGRESSION;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@Tag(PROMETHEUS)
@Tag(METRICS)
public class PrometheusST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(PrometheusST.class);

    public static final String NAMESPACE = "prometheus-test";

    private static final String PROMETHEUS_POD = "prometheus-prometheus-0";
    private static final String ALERTMANAGER = "alertmanager";
    private static final String ALERTMANAGER_POD = "alertmanager-alertmanager-0";

    @Test
    public void testAlertManagerService() {
        assertThat("AlertManager service not found", kubeClient().getService(ALERTMANAGER) != null);
        assertThat("AlertManager service port is not 9090", kubeClient().getService(ALERTMANAGER).getSpec().getPorts().get(0).getPort() == 9093);
    }

    @Test
    public void testAlertManagerPodIsUp() {
        assertThat("AlertManager pod not found", kubeClient().getPod(ALERTMANAGER_POD) != null);
        int conditionsSize = kubeClient().getPod(ALERTMANAGER_POD).getStatus().getConditions().get(0).getStatus().length();
        assertThat("AlertManager pod is not ready", kubeClient().getPod(ALERTMANAGER_POD).getStatus().getConditions().get(conditionsSize - 1).getStatus().equals("True"));
    }

    @Test
    public void testPrometheusPodIsUp() {
        assertThat("Prometheus pod not found", kubeClient().getPod(PROMETHEUS_POD) != null);
        int conditionsSize = kubeClient().getPod(PROMETHEUS_POD).getStatus().getConditions().get(0).getStatus().length();
        assertThat("Prometheus pod is not ready", kubeClient().getPod(PROMETHEUS_POD).getStatus().getConditions().get(conditionsSize - 1).getStatus().equals("True"));
    }

    @Test
    public void testSecretsCreated() {
        assertThat("additional-scrape-configs secret not found", kubeClient().getSecret("additional-scrape-configs") != null);
        assertThat("additional-scrape-configs secret does not contain key prometheus-additional.yaml", kubeClient().getSecret("additional-scrape-configs").getData().get("prometheus-additional.yaml") != null);
        assertThat("alertmanager-alertmanager secret", kubeClient().getSecret("alertmanager-alertmanager") != null);
        assertThat("alertmanager-alertmanager secret does not contain key alertmanager.yaml", kubeClient().getSecret("alertmanager-alertmanager").getData().get("alertmanager.yaml") != null);
    }

    @BeforeAll
    void setup() throws IOException {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        cmdKubeClient().apply(FileUtils.downloadYamlAndReplaceNamespace("https://raw.githubusercontent.com/coreos/prometheus-operator/v0.38.1/bundle.yaml", NAMESPACE));

        SecretUtils.createSecretFromFile(TestUtils.USER_PATH + "/../examples/metrics/prometheus-additional-properties/prometheus-additional.yaml", "prometheus-additional.yaml", "additional-scrape-configs", NAMESPACE);
        SecretUtils.createSecretFromFile(TestUtils.USER_PATH + "/../examples/metrics/prometheus-alertmanager-config/alert-manager-config.yaml", "alertmanager.yaml", "alertmanager-alertmanager", NAMESPACE);

        SecretUtils.waitForSecretReady("additional-scrape-configs");
        SecretUtils.waitForSecretReady("alertmanager-alertmanager");

        DeploymentUtils.waitForDeploymentAndPodsReady("prometheus-operator", 1);

        cmdKubeClient().apply(FileUtils.updateNamespaceOfYamlFile(TestUtils.USER_PATH + "/../examples/metrics/prometheus-install/strimzi-pod-monitor.yaml", NAMESPACE));
        cmdKubeClient().apply(FileUtils.updateNamespaceOfYamlFile(TestUtils.USER_PATH + "/../examples/metrics/prometheus-install/prometheus-rules.yaml", NAMESPACE));
        cmdKubeClient().apply(FileUtils.updateNamespaceOfYamlFile(TestUtils.USER_PATH + "/../examples/metrics/prometheus-install/alert-manager.yaml", NAMESPACE));
        cmdKubeClient().apply(FileUtils.updateNamespaceOfYamlFile(TestUtils.USER_PATH + "/../examples/metrics/prometheus-install/prometheus.yaml", NAMESPACE));

        PodUtils.waitForPod(ALERTMANAGER_POD);
        PodUtils.waitForPod(PROMETHEUS_POD);
    }
}
