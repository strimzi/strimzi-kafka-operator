/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.metrics;

import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.utils.StUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.strimzi.systemtest.Constants.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
public class PrometheusST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(PrometheusST.class);

    public static final String NAMESPACE = "prometheus-test";

    private static final String PROMETHEUS = "prometheus";
    private static final String PROMETHEUS_POD = "prometheus-prometheus-0";
    private static final String ALERTMANAGER = "alertmanager";
    private static final String ALERTMANAGER_POD = "alertmanager-alertmanager-0";

    @Test
    public void testPrometheusService() {
        assertThat("Prometheus service not found", kubeClient().getService(PROMETHEUS) != null);
        assertThat("Prometheus service port is not 9090", kubeClient().getService(PROMETHEUS).getSpec().getPorts().get(0).getPort() == 9090);
    }

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

    // No need to recreate environment after failed test. Only values from collected metrics are checked
    @Override
    protected void recreateTestEnv(String coNamespace, List<String> bindingsNamespaces) { }

    @BeforeAll
    void setupEnvironment() {
        LOGGER.info("Creating resources before the test class");
        prepareEnvForOperator(NAMESPACE);

        createTestClassResources();
        cmdKubeClient().apply(StUtils.downloadYamlAndReplaceNameSpace("https://raw.githubusercontent.com/coreos/prometheus-operator/master/bundle.yaml", NAMESPACE));

        StUtils.createSecretFromFile("../metrics/examples/prometheus/additional-properties/prometheus-additional.yaml", "prometheus-additional.yaml", "additional-scrape-configs", NAMESPACE);
        StUtils.createSecretFromFile("../metrics/examples/prometheus/alertmanager-config/alert-manager-config.yaml", "alertmanager.yaml", "alertmanager-alertmanager", NAMESPACE);

        StUtils.waitForSecretReady("additional-scrape-configs");
        StUtils.waitForSecretReady("alertmanager-alertmanager");

        StUtils.waitForDeploymentReady("prometheus-operator", 1);

        cmdKubeClient().apply(StUtils.updateNamespaceOfYamlFile("../metrics/examples/prometheus/install/strimzi-service-monitor.yaml", NAMESPACE));
        cmdKubeClient().apply(StUtils.updateNamespaceOfYamlFile("../metrics/examples/prometheus/install/prometheus-rules.yaml", NAMESPACE));
        cmdKubeClient().apply(StUtils.updateNamespaceOfYamlFile("../metrics/examples/prometheus/install/alert-manager.yaml", NAMESPACE));
        cmdKubeClient().apply(StUtils.updateNamespaceOfYamlFile("../metrics/examples/prometheus/install/prometheus.yaml", NAMESPACE));

        StUtils.waitForPod(ALERTMANAGER_POD);
        StUtils.waitForPod(PROMETHEUS_POD);
    }
}
