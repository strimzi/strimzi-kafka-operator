/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.cli;

import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinitionVersion;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.strimzi.api.kafka.Crds;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"rawtypes"})
@CommandLine.Command(name = "crd-upgrade", aliases = {"crd"}, description = "Upgrades the Strimzi CRDs and CRs to use v1beta2 version")
public class CrdUpgradeCommand extends AbstractCommand {
    final static Map<String, String> CRD_NAMES = Map.of(
            "Kafka", "kafkas.kafka.strimzi.io",
            "KafkaConnect", "kafkaconnects.kafka.strimzi.io",
            "KafkaConnectS2I", "kafkaconnects2is.kafka.strimzi.io",
            "KafkaMirrorMaker", "kafkatopics.kafka.strimzi.io",
            "KafkaBridge", "kafkabridges.kafka.strimzi.io",
            "KafkaMirrorMaker2", "kafkamirrormaker2s.kafka.strimzi.io",
            "KafkaTopic", "kafkamirrormakers.kafka.strimzi.io",
            "KafkaUser", "kafkausers.kafka.strimzi.io",
            "KafkaConnector", "kafkaconnectors.kafka.strimzi.io",
            "KafkaRebalance", "kafkarebalances.kafka.strimzi.io"
    );

    private KubernetesClient client;

    static {
        Crds.registerCustomKinds();
    }

    @SuppressWarnings({"unchecked"})
    private <R extends CustomResource, L extends CustomResourceList<R>> void storeCrsUnderNewVersionForKind(String kind) {
        MixedOperation<R, L, ?> op = VERSIONED_OPERATIONS.get(kind).apply(client, TO_API_VERSION.toString());

        List<R> crs = op.inAnyNamespace().list().getItems();

        for (R cr : crs)    {
            println("Updating " + kind + " " + cr.getMetadata().getName() + " to be stored as " + TO_API_VERSION.toString());
            op.inNamespace(cr.getMetadata().getNamespace()).withName(cr.getMetadata().getName()).replace(cr);
        }
    }

    private void storeCrsUnderNewVersion()   {
        for (String kind : STRIMZI_KINDS)  {
            storeCrsUnderNewVersionForKind(kind);
        }
    }

    private void changeStoredVersionInSpecForKind(String kind) {
        String crdName = CRD_NAMES.get(kind);
        CustomResourceDefinition crd = client.apiextensions().v1beta1().customResourceDefinitions().withName(crdName).get();

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
            client.apiextensions().v1beta1().customResourceDefinitions().withName(crdName).patch(crd);
        } else {
            throw new RuntimeException("CRD " + kind + " not found. CRD Upgrade cannot be completed.");
        }
    }

    private void changeStoredVersionInSpec()   {
        for (String kind : STRIMZI_KINDS)  {
            changeStoredVersionInSpecForKind(kind);
        }
    }

    private void changeStoredVersionInStatusForKind(String kind) {
        String crdName = CRD_NAMES.get(kind);
        CustomResourceDefinition crd = client.apiextensions().v1beta1().customResourceDefinitions().withName(crdName).get();

        if (crd != null)    {
            List<String> crdVersions = new ArrayList<>(crd.getStatus().getStoredVersions());

            for (String version : crdVersions) {
                if (!TO_API_VERSION.toString().equals(version)) {
                    if (debug) println("Removing version " + version + " version of Kind " + kind + " from stored versions in CRD status");
                    crd.getStatus().getStoredVersions().remove(version);
                }
            }

            println("Updating " + kind + " CRD");
            client.apiextensions().v1beta1().customResourceDefinitions().withName(crdName).updateStatus(crd);
        } else {
            throw new RuntimeException("CRD " + kind + " not found. CRD Upgrade cannot be completed.");
        }
    }

    private void changeStoredVersionInStatus()   {
        for (String kind : STRIMZI_KINDS)  {
            changeStoredVersionInStatusForKind(kind);
        }
    }

    /**
     * Reads the resources from the Kubernetes API and converts them
     */
    @Override
    public void run() {
        client = new DefaultKubernetesClient();

        // Change the stored version in CRD spec to v1beta2
        println("Changing stored version in all Strimzi CRDs to " + TO_API_VERSION.toString() + ":");
        changeStoredVersionInSpec();
        println();

        // "Touch" all resources to have them stored as v1beta2
        println("Updating all Strimzi CRs to be stored under " + TO_API_VERSION.toString() + ":");
        storeCrsUnderNewVersion();
        println();

        // Change the stored versions in CRDs to v1beta2
        println("Changing stored version in statuses of all Strimzi CRDs to " + TO_API_VERSION.toString() + ":");
        changeStoredVersionInStatus();
    }
}
