/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.strimzi.api.kafka.model.common.template.ResourceTemplate;
import io.strimzi.api.kafka.model.kafka.JbodStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorage;
import io.strimzi.api.kafka.model.kafka.SingleVolumeStorage;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared methods for working with Persistent Volume Claims
 */
public class PersistentVolumeClaimUtils {
    /**
     * Creates list of PersistentVolumeClaims required by stateful deployments (Kafka). This method calls itself
     * recursively to handle volumes inside JBOD storage. When it calls itself to handle the volumes inside JBOD array,
     * the {@code jbod} flag should be set to {@code true}. When called from outside, it should be set to {@code false}.
     *
     * @param namespace         Namespace of the PVC
     * @param nodes             List of node references for which the PCX should be generated
     * @param storage           The user supplied configuration of the PersistentClaimStorage
     * @param jbod              Indicator whether the {@code storage} is part of JBOD array or not
     * @param labels            Labels of the PVC
     * @param ownerReference    OwnerReference of the PVC
     * @param template          PVC template with user's custom configuration
     *
     * @return  List with Persistent Volume Claims
     */
    public static List<PersistentVolumeClaim> createPersistentVolumeClaims(
            String namespace,
            Set<NodeRef> nodes,
            Storage storage,
            boolean jbod,
            Labels labels,
            OwnerReference ownerReference,
            ResourceTemplate template
    )   {
        List<PersistentVolumeClaim> pvcs = new ArrayList<>();

        if (storage != null) {
            if (storage instanceof PersistentClaimStorage persistentStorage) {
                String namePrefix = VolumeUtils.createVolumePrefix(persistentStorage.getId(), jbod);

                for (NodeRef node : nodes) {
                    pvcs.add(createPersistentVolumeClaim(namePrefix + "-" + node.podName(), namespace, persistentStorage, labels, ownerReference, template));
                }
            } else if (storage instanceof JbodStorage jbodStorage) {
                for (SingleVolumeStorage volume : jbodStorage.getVolumes()) {
                    // it's called recursively for setting the information from the current volume
                    pvcs.addAll(createPersistentVolumeClaims(namespace, nodes, volume, true, labels, ownerReference, template));
                }
            }
        }

        return pvcs;
    }

    /**
     * Generates a persistent volume claim for a given broker ID.
     *
     * @param name              Name of the PVC
     * @param namespace         Namespace of the PVC
     * @param storage           The user supplied configuration of the PersistentClaimStorage
     * @param labels            Labels of the PVC
     * @param ownerReference    OwnerReference of the PVC
     * @param template          PVC template with user's custom configuration
     *
     * @return Generated PersistentVolumeClaim
     */
    private static PersistentVolumeClaim createPersistentVolumeClaim(
            String name,
            String namespace,
            PersistentClaimStorage storage,
            Labels labels,
            OwnerReference ownerReference,
            ResourceTemplate template
    ) {
        Map<String, Quantity> requests = new HashMap<>(1);
        requests.put("storage", new Quantity(storage.getSize(), null));

        LabelSelector storageSelector = null;
        if (storage.getSelector() != null && !storage.getSelector().isEmpty()) {
            storageSelector = new LabelSelector(null, storage.getSelector());
        }

        PersistentVolumeClaim pvc = new PersistentVolumeClaimBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(namespace)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withAnnotations(Util.mergeLabelsOrAnnotations(Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM, Boolean.toString(storage.isDeleteClaim())), TemplateUtils.annotations(template)))
                .endMetadata()
                .withNewSpec()
                    .withAccessModes("ReadWriteOnce")
                    .withNewResources()
                        .withRequests(requests)
                    .endResources()
                    .withStorageClassName(storage.getStorageClass())
                    .withSelector(storageSelector)
                    .withVolumeMode("Filesystem")
                .endSpec()
                .build();

        // if the persistent volume claim has to be deleted when the cluster is un-deployed then set an owner reference of the CR
        if (storage.isDeleteClaim())    {
            pvc.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));
        }

        return pvc;
    }
}