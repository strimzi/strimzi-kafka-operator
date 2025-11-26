/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.cli;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceList;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionVersion;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.kafka.api.conversion.v1.utils.Utils;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;

/**
 * Upgrades the API versions in CRDs to store the new v1 version only
 */
@CommandLine.Command(name = "crd-upgrade", aliases = {"crd"}, description = "Upgrades the Strimzi CRDs and CRs to use v1beta2 version")
public class CrdUpgradeCommand extends AbstractCommand {
    private KubernetesClient client;

    /**
     * Touches all Strimzi custom resources of a given kind to make sure they are stored under the new version. It is
     * using the replace command to make sure this happens.
     *
     * @param kind  The kind of the resources which should be updated
     */
    private void storeCrsUnderNewVersionForKind(String kind) {
        MixedOperation<GenericKubernetesResource, GenericKubernetesResourceList, Resource<GenericKubernetesResource>> op = Utils.versionedOperation(client, kind, STRIMZI_GROUPS.get(kind), TO_API_VERSION);

        List<GenericKubernetesResource> crs = op.inAnyNamespace().list().getItems();

        for (GenericKubernetesResource cr : crs)    {
            println("Updating " + kind + " " + cr.getMetadata().getName() + " to be stored as " + TO_API_VERSION.toString());
            op.inNamespace(cr.getMetadata().getNamespace()).resource(cr).update();
        }
    }

    /**
     * Touches all existing Strimzi custom resources to make sure they are stored in Kubernetes API under the latest
     * version.
     */
    private void storeCrsUnderNewVersion()   {
        for (String kind : STRIMZI_KINDS)  {
            storeCrsUnderNewVersionForKind(kind);
        }
    }

    /**
     * Changes the stored versions in the CRD spec to keep v1beta2 as stored and the other versions as served. This
     * method does the change to a single Strimzi CRD.
     *
     * @param kind Kind of the custom resource definition where the stored version should be changed
     */
    private void changeStoredVersionInSpecForKind(String kind) {
        String crdName = CRD_NAMES.get(kind);
        CustomResourceDefinition crd = client.apiextensions().v1().customResourceDefinitions().withName(crdName).get();

        if (crd != null) {
            for (CustomResourceDefinitionVersion crdVersion : crd.getSpec().getVersions()) {
                if (TO_API_VERSION.toString().equals(crdVersion.getName())) {
                    if (debug)
                        println("Updating " + crdVersion.getName() + " version of Kind " + kind + " to be served and stored");
                    crdVersion.setServed(true);
                    crdVersion.setStorage(true);
                } else {
                    if (debug)
                        println("Updating " + crdVersion.getName() + " version of Kind " + kind + " to be served but not stored");
                    crdVersion.setServed(true);
                    crdVersion.setStorage(false);
                }
            }

            println("Updating " + kind + " CRD");
            client.apiextensions().v1().customResourceDefinitions().withName(crdName).patch(crd);
        } else {
            throw new RuntimeException("CRD " + kind + " not found. CRD Upgrade cannot be completed.");
        }
    }

    /**
     * Changes the stored versions in the CRD spec to keep v1beta2 as stored and the other versions as served. This does
     * the change for all Strimzi CRDs.
     */
    private void changeStoredVersionInSpec()   {
        for (String kind : STRIMZI_KINDS)  {
            changeStoredVersionInSpecForKind(kind);
        }
    }

    /**
     * Changes the stored versions in the CRD status to keep only v1beta2 for given Strimzi kind.
     *
     * @param kind Kind of the custom resource definition where the stored versions should be changed
     */
    private void changeStoredVersionInStatusForKind(String kind) {
        String crdName = CRD_NAMES.get(kind);
        CustomResourceDefinition crd = client.apiextensions().v1().customResourceDefinitions().withName(crdName).get();

        if (crd != null)    {
            List<String> crdVersions = new ArrayList<>(crd.getStatus().getStoredVersions());

            for (String version : crdVersions) {
                if (!TO_API_VERSION.toString().equals(version)) {
                    if (debug) println("Removing version " + version + " version of Kind " + kind + " from stored versions in CRD status");
                    crd.getStatus().getStoredVersions().remove(version);
                }
            }

            println("Updating " + kind + " CRD");
            client.apiextensions().v1().customResourceDefinitions().resource(crd).updateStatus();
        } else {
            throw new RuntimeException("CRD " + kind + " not found. CRD Upgrade cannot be completed.");
        }
    }

    /**
     * Changes the stored versions in the CRD status to keep only v1beta2 there. This does the change for all Strimzi CRDs.
     */
    private void changeStoredVersionInStatus()   {
        for (String kind : STRIMZI_KINDS)  {
            changeStoredVersionInStatusForKind(kind);
        }
    }

    /**
     * The main line of the crd-upgrade command. It first updates the stored version in the spec section of the CRDs.
     * Then it touches all the custom resources to make sure they are stored under the new version. And at the end it
     * removes the previous versions from the CRD status section.
     */
    @Override
    public void run() {
        client = new KubernetesClientBuilder().build();

        // Pre-check for more user-friendliness
        println("Checking that the CRDs are present and have the desired API versions.");
        Utils.checkCrdsHaveApiVersions(client, CRD_NAMES.values(), FROM_API_VERSION, TO_API_VERSION);

        // Change the stored version in CRD spec to v1beta2
        println("Changing stored version in all Strimzi CRDs to " + TO_API_VERSION + ":");
        changeStoredVersionInSpec();
        println();

        // "Touch" all resources to have them stored as v1beta2
        println("Updating all Strimzi CRs to be stored under " + TO_API_VERSION + ":");
        storeCrsUnderNewVersion();
        println();

        // Change the stored versions in CRDs to v1beta2
        println("Changing stored version in statuses of all Strimzi CRDs to " + TO_API_VERSION + ":");
        changeStoredVersionInStatus();
    }
}
