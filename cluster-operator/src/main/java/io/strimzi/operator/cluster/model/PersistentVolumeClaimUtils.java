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
import io.strimzi.api.kafka.model.storage.JbodStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorage;
import io.strimzi.api.kafka.model.storage.PersistentClaimStorageOverride;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.template.ResourceTemplate;
import io.strimzi.api.kafka.model.template.StatefulSetTemplate;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared methods for working with Persistent Volume Claims
 */
public class PersistentVolumeClaimUtils {
    /**
     * Creates list of PersistentVolumeClaims required by stateful deployments (Kafka and Zoo). This method calls itself
     * recursively to handle volumes inside JBOD storage. When it calls itself to handle the volumes inside JBOD array,
     * the {@code jbod} flag should be set to {@code true}. When called from outside, it should be set to {@code false}.
     *
     * @param componentName             Name of the Strimzi component to which these PVCs belong. It is used to generate
     *                                  the PVC names.
     * @param namespace                 Namespace of the PVC
     * @param replicas                  Number of replicas this component has
     * @param storage                   The user supplied configuration of the PersistentClaimStorage
     * @param jbod                      Indicator whether the {@code storage} is part of JBOD array or not
     * @param labels                    Labels of the PVC
     * @param ownerReference            OwnerReference of the PVC
     * @param template                  PVC template with user's custom configuration
     * @param stsTemplate               StatefulSet template with user configured labels. These are added to the PVC
     *                                  labels for historical reasons. This should be removed when removing StatefulSet
     *                                  support.
     *
     * @return  List with Persistent Volume Claims
     */
    public static List<PersistentVolumeClaim> createPersistentVolumeClaims(
            String componentName,
            String namespace,
            int replicas,
            Storage storage,
            boolean jbod,
            Labels labels,
            OwnerReference ownerReference,
            ResourceTemplate template,
            StatefulSetTemplate stsTemplate
    )   {
        List<PersistentVolumeClaim> pvcs = new ArrayList<>();

        if (storage != null) {
            if (storage instanceof PersistentClaimStorage persistentStorage) {
                String pvcBaseName = VolumeUtils.createVolumePrefix(persistentStorage.getId(), jbod) + "-" + componentName;

                for (int brokerId = 0; brokerId < replicas; brokerId++) {
                    pvcs.add(createPersistentVolumeClaim(pvcBaseName + "-" + brokerId, namespace, brokerId, persistentStorage, labels, ownerReference, template, TemplateUtils.labels(stsTemplate)));
                }
            } else if (storage instanceof JbodStorage jbodStorage) {
                for (SingleVolumeStorage volume : jbodStorage.getVolumes()) {
                    // it's called recursively for setting the information from the current volume
                    pvcs.addAll(createPersistentVolumeClaims(componentName, namespace, replicas, volume, true, labels, ownerReference, template, stsTemplate));
                }
            }
        }

        return pvcs;
    }

    /**
     * Gets the storage class configured for given PVC. This either the regularly configured storage class or the
     * storage class from the per-broker configuration overrides. If not storage class is specified, it returns null
     * and the default storage class will be used.
     *
     * @param brokerId          ID of the broker to which this PVC belongs. It is used to find configuration overrides
     *                          for Storage class.
     * @param storage           The user supplied configuration of the PersistentClaimStorage
     *
     * @return  Storage class which should be used for this PVC
     */
    private static String storageClassNameForBrokerId(int brokerId, PersistentClaimStorage storage)    {
        String storageClass = storage.getStorageClass();

        if (storage.getOverrides() != null) {
            storageClass = storage.getOverrides().stream()
                    .filter(broker -> broker != null
                            && broker.getBroker() != null
                            && broker.getBroker() == brokerId
                            && broker.getStorageClass() != null)
                    .map(PersistentClaimStorageOverride::getStorageClass)
                    .findAny()
                    // if none are found for broker do not change storage class from overrides
                    .orElse(storageClass);
        }

        return storageClass;
    }

    /**
     * Generates a persistent volume claim for a given broker ID.
     *
     * @param name                      Name of the PVC
     * @param namespace                 Namespace of the PVC
     * @param brokerId                  ID of the broker to which this PVC belongs. It is used to find configuration
     *                                  overrides for Storage class.
     * @param storage                   The user supplied configuration of the PersistentClaimStorage
     * @param labels                    Labels of the PVC
     * @param ownerReference            OwnerReference of the PVC
     * @param template                  PVC template with user's custom configuration
     * @param statefulSetTemplateLabels User configured labels from StatefulSet template. These are added to the PVC
     *                                  labels for historical reasons. This should be removed when removing StatefulSet support.
     *
     * @return  Generated PersistentVolumeClaim
     */
    private static PersistentVolumeClaim createPersistentVolumeClaim(
            String name,
            String namespace,
            int brokerId,
            PersistentClaimStorage storage,
            Labels labels,
            OwnerReference ownerReference,
            ResourceTemplate template,
            Map<String, String> statefulSetTemplateLabels
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
                    .withLabels(labels.withAdditionalLabels(Util.mergeLabelsOrAnnotations(TemplateUtils.labels(template), statefulSetTemplateLabels)).toMap())
                    .withAnnotations(Util.mergeLabelsOrAnnotations(Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_DELETE_CLAIM, Boolean.toString(storage.isDeleteClaim())), TemplateUtils.annotations(template)))
                .endMetadata()
                .withNewSpec()
                    .withAccessModes("ReadWriteOnce")
                    .withNewResources()
                        .withRequests(requests)
                    .endResources()
                    .withStorageClassName(storageClassNameForBrokerId(brokerId, storage))
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