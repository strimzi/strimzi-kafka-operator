/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.specific;

import io.fabric8.openshift.api.model.operatorhub.v1alpha1.ClusterServiceVersion;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.InstallPlan;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.InstallPlanBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.test.TestUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class containing utilization methods for everything related to OLM
 */
public class OlmUtils {

    private static final Logger LOGGER = LogManager.getLogger(OlmUtils.class);

    private OlmUtils() {}

    /**
     * Returns {@link ClusterServiceVersion} from specified Namespace containing specified prefix in its name.
     *
     * @param namespaceName     Namespace name where the ClusterServiceVersion should be present.
     * @param csvNamePrefix     Prefix in name of the desired ClusterServiceVersion.
     *
     * @return  {@link ClusterServiceVersion} from specified Namespace containing specified prefix in its name.
     */
    public static ClusterServiceVersion getCsvWithPrefix(String namespaceName, String csvNamePrefix) {
        return KubeResourceManager.get().kubeClient().getClient().adapt(OpenShiftClient.class)
            .operatorHub()
            .clusterServiceVersions()
            .inNamespace(namespaceName)
            .list()
            .getItems()
            .stream()
            .filter(csv -> csv.getMetadata().getName().contains(csvNamePrefix))
            .findFirst()
            .get();
    }

    /**
     * Returns non-approved {@link InstallPlan} from specified Namespace and for specific {@link ClusterServiceVersion}
     * searched by prefix and not complete name.
     *
     * @param namespaceName     Namespace name where the InstallPlan should be present.
     * @param csvNamePrefix     ClusterServiceVersion prefix InstallPlan should contain.
     *
     * @return  non-approved {@link InstallPlan} from specified Namespace and for specific {@link ClusterServiceVersion}.
     */
    public static InstallPlan getNonApprovedInstallPlanForCsvNamePrefix(String namespaceName, String csvNamePrefix) {
        return KubeResourceManager.get().kubeClient().getClient().adapt(OpenShiftClient.class)
            .operatorHub()
            .installPlans()
            .inNamespace(namespaceName)
            .list()
            .getItems()
            .stream()
            .filter(installPlan -> !installPlan.getSpec().getApproved()
                && installPlan.getSpec().getClusterServiceVersionNames().get(0).contains(csvNamePrefix))
            .findFirst()
            .get();
    }

    /**
     * Method that waits for appearance of non-approved InstallPlan for specified CSV name (or its prefix) in particular
     * namespace.
     *
     * @param namespaceName     name of the Namespace where the non-approved InstallPlan should be
     * @param csvNameOrPrefix   CSV name or prefix, that will be in the non-approved InstallPLan
     */
    public static void waitForNonApprovedInstallPlanWithCsvNameOrPrefix(String namespaceName, String csvNameOrPrefix) {
        LOGGER.info("Waiting for unused InstallPlan with CSV name or prefix: {}/{} to be present", namespaceName, csvNameOrPrefix);
        TestUtils.waitFor(
            "unused InstallPlan with CSV name or prefix: " + namespaceName + "/" + csvNameOrPrefix + " to be present",
            TestConstants.OLM_UPGRADE_INSTALL_PLAN_POLL,
            TestConstants.OLM_UPGRADE_INSTALL_PLAN_TIMEOUT,
            () -> getNonApprovedInstallPlanForCsvNamePrefix(namespaceName, csvNameOrPrefix) != null
        );
    }

    /**
     * Method that gets the non-approved InstallPlan from the Namespace by CSV name (or its prefix) and approves it, so
     * the installation of the newer version of the Deployment can begin.
     *
     * @param namespaceName     name of the Namespace where the non-approved InstallPlan is
     * @param csvNameOrPrefix   CSV name or prefix, that is in the non-approved InstallPLan
     */
    public static void approveNonApprovedInstallPlan(String namespaceName, String csvNameOrPrefix) {
        InstallPlan nonApprovedInstallPlan = getNonApprovedInstallPlanForCsvNamePrefix(namespaceName, csvNameOrPrefix);

        approveInstallPlan(namespaceName, nonApprovedInstallPlan.getMetadata().getName());
    }

    /**
     * Method for approving InstallPlan with specified name and Namespace.
     *
     * @param namespaceName     name of the Namespace where the InstallPlan is
     * @param installPlanName   name of the InstallPlan that should be approved
     */
    public static void approveInstallPlan(String namespaceName, String installPlanName) {
        LOGGER.info("Approving following InstallPlan: {}/{}", namespaceName, installPlanName);
        InstallPlan installPlan = KubeResourceManager.get().kubeClient().getClient().adapt(OpenShiftClient.class)
            .operatorHub().installPlans().inNamespace(namespaceName).withName(installPlanName).get();

        installPlan = new InstallPlanBuilder(installPlan)
            .editSpec()
                .withApproved()
            .endSpec()
            .build();

        KubeResourceManager.get().kubeClient().getClient().adapt(OpenShiftClient.class)
            .operatorHub().installPlans().inNamespace(namespaceName).withName(installPlanName).patch(installPlan);
    }

    /**
     * Method that waits for creation of the CSV with specified name and Namespace.
     *
     * @param namespaceName     name of the Namespace where the CSV should be created
     * @param csvName           name of the CSV that should be created
     */
    public static void waitForCsvWithNameCreation(String namespaceName, String csvName) {
        LOGGER.info("Waiting for creation of CSV: {}/{}", namespaceName, csvName);

        TestUtils.waitFor(
            "for creation of CSV: " + namespaceName + "/" + csvName,
            TestConstants.OLM_UPGRADE_INSTALL_PLAN_POLL,
            TestConstants.OLM_UPGRADE_INSTALL_PLAN_TIMEOUT,
            () -> KubeResourceManager.get().kubeClient().getClient()
                .adapt(OpenShiftClient.class)
                .operatorHub()
                .clusterServiceVersions()
                .inNamespace(namespaceName)
                .withName(csvName)
                .get() != null
        );
    }

    /**
     * Method that approves the non-approved InstallPlan, waits for creation of the CSV, and returns the name of the
     * Deployment that will be created, from the CSV.
     * This method is mainly used in the OLM upgrade tests.
     *
     * @param namespaceName     name of the Namespace, where the non-approved InstallPlan is
     * @param csvNameOrPrefix   CSV name or prefix, that is in the non-approved InstallPLan
     *
     * @return  full name of the Deployment, by which is the new installation done
     */
    public static String approveNonApprovedInstallPlanAndReturnDeploymentName(String namespaceName, String csvNameOrPrefix) {
        InstallPlan nonApprovedInstallPlan = getNonApprovedInstallPlanForCsvNamePrefix(namespaceName, csvNameOrPrefix);

        approveInstallPlan(namespaceName, nonApprovedInstallPlan.getMetadata().getName());

        String csvName = nonApprovedInstallPlan.getSpec().getClusterServiceVersionNames().get(0);

        waitForCsvWithNameCreation(namespaceName, csvName);

        return KubeResourceManager.get().kubeClient().getClient()
            .adapt(OpenShiftClient.class)
            .operatorHub()
            .clusterServiceVersions()
            .inNamespace(namespaceName)
            .withName(csvName)
            .get()
            .getSpec()
            .getInstall()
            .getSpec()
            .getDeployments()
            .get(0)
            .getName();
    }

    /**
     * Returns Map of Kind and particular object in JsonObject from CSV.
     *
     * @param coNamespaceName   Namespace name where the CSV should be located
     * @param olmBundlePrefix   Prefix for the OLM bundle - by that the CSV is taken
     *
     * @return  Map of examples that particular CSV contains
     */
    public static Map<String, JsonObject> getExamplesFromCsv(String coNamespaceName, String olmBundlePrefix) {
        JsonArray examples = new JsonArray(getCsvWithPrefix(coNamespaceName, olmBundlePrefix).getMetadata().getAnnotations().get("alm-examples"));
        return examples.stream().map(o -> (JsonObject) o).collect(Collectors.toMap(object -> object.getString("kind"), object -> object));
    }
}
