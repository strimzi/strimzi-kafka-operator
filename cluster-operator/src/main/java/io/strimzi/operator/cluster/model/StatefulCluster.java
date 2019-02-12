/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetUpdateStrategyBuilder;
import io.strimzi.api.kafka.model.Storage;
import io.strimzi.operator.common.model.Labels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Clusters with state.  Stateful implies long-term storage.
 */
public abstract class StatefulCluster extends AbstractModel {

    static final Long DEFAULT_FS_GROUPID = 0L;

    protected Storage storage;

    /**
     * Lists with volumes, persistent volume claims and related volume mount paths for the storage
     */
    List<Volume> dataVolumes = new ArrayList<>();
    List<PersistentVolumeClaim> dataPvcs = new ArrayList<>();
    List<VolumeMount> dataVolumeMountPaths = new ArrayList<>();

    public StatefulCluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
    }

    public Storage getStorage() {
        return storage;
    }

    /**
     * Check validity of storage after setting
     */
    protected void setStorage(Storage storage) {
        this.storage = storage;
        if (storage != null) {
            String msg = storage.invalidityReason();
            if (msg != null) {
                throw new InvalidResourceException(msg);
            }
            setDataVolumesClaimsAndMountPaths(storage);
        }
    }

    /**
     * Fill the volumes, persistent volume claims, and related volume mount paths for the storage
     */
    private void setDataVolumesClaimsAndMountPaths(Storage storage) {
        storage.iteratePersistentClaimStorage((pcs, name) -> {
            dataPvcs.add(createPersistentVolumeClaim(name, pcs));
            dataVolumeMountPaths.add(createVolumeMount(name, fullPath(name)));
        }, AbstractModel.VOLUME_NAME);

        storage.iterateEphemeralStorage((es, name) -> {
            dataVolumes.add(createEmptyDirVolume(name));
            dataVolumeMountPaths.add(createVolumeMount(name, fullPath(name)));
        }, AbstractModel.VOLUME_NAME);
    }

    private String fullPath(String name) {
        return mountPath + '/' + name;
    }

    /* test */ List<PersistentVolumeClaim> getVolumeClaims() {
        List<PersistentVolumeClaim> pvcList = new ArrayList<>();
        pvcList.addAll(dataPvcs);
        return pvcList;
    }

    protected StatefulSet createStatefulSet(
            Map<String, String> annotations,
            List<Volume> volumes,
            List<PersistentVolumeClaim> volumeClaims,
            Affinity affinity,
            List<Container> initContainers,
            List<Container> containers,
            boolean isOpenShift) {

        PodSecurityContext securityContext = templateSecurityContext;

        // if a persistent volume claim is requested and the running cluster is a Kubernetes one and we have no user configured PodSecurityContext
        // we set the security context
        if (storage != null && storage.containsPersistentStorage() && !isOpenShift && securityContext == null) {
            securityContext = new PodSecurityContextBuilder()
                    .withFsGroup(DEFAULT_FS_GROUPID)
                    .build();
        }

        StatefulSet statefulSet = new StatefulSetBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(getLabelsWithName(templateStatefulSetLabels))
                    .withNamespace(namespace)
                    .withAnnotations(mergeAnnotations(annotations, templateStatefulSetAnnotations))
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withNewSpec()
                    .withPodManagementPolicy("Parallel")
                    .withUpdateStrategy(new StatefulSetUpdateStrategyBuilder().withType("OnDelete").build())
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(getSelectorLabels()).build())
                    .withServiceName(headlessServiceName)
                    .withReplicas(replicas)
                    .withNewTemplate()
                        .withNewMetadata()
                            .withName(name)
                            .withLabels(getLabelsWithName(templatePodLabels))
                            .withAnnotations(mergeAnnotations(null, templatePodAnnotations))
                        .endMetadata()
                        .withNewSpec()
                            .withServiceAccountName(getServiceAccountName())
                            .withAffinity(affinity)
                            .withInitContainers(initContainers)
                            .withContainers(containers)
                            .withVolumes(volumes)
                            .withTolerations(getTolerations())
                            .withTerminationGracePeriodSeconds(Long.valueOf(templateTerminationGracePeriodSeconds))
                            .withImagePullSecrets(templateImagePullSecrets)
                            .withSecurityContext(securityContext)
                        .endSpec()
                    .endTemplate()
                    .withVolumeClaimTemplates(volumeClaims)
                .endSpec()
                .build();

        return statefulSet;
    }
}
