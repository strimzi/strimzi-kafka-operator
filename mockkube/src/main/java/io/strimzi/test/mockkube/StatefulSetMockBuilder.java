/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube;

import io.fabric8.kubernetes.api.model.DoneablePersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.apps.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.api.model.apps.StatefulSetStatus;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.EditReplacePatchDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StatefulSetMockBuilder extends MockBuilder<StatefulSet, StatefulSetList, DoneableStatefulSet,
        RollableScalableResource<StatefulSet, DoneableStatefulSet>> {

    private static final Logger LOGGER = LogManager.getLogger(StatefulSetMockBuilder.class);

    private final MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods;
    private final MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> mockPvcs;
    private final Map<String, Pod> podDb;

    public StatefulSetMockBuilder(MockBuilder<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> podMockBuilder, Map<String, StatefulSet> ssDb,
                                  Map<String, Pod> podDb,
                                  MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods, MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> mockPvcs) {
        super(StatefulSet.class, StatefulSetList.class, DoneableStatefulSet.class, castClass(RollableScalableResource.class), ssDb);
        podMockBuilder.addObserver(new Observer<Pod>() {
            @Override
            public void beforeWatcherFire(Watcher.Action action, Pod resource) {

            }

            @Override
            public void afterWatcherFire(Watcher.Action action, Pod resource) {
                if (action == Watcher.Action.DELETED) {
                    List<OwnerReference> ownerReferences = resource.getMetadata().getOwnerReferences();
                    if (ownerReferences != null) {
                        for (OwnerReference ownerReference : ownerReferences) {
                            if ("StatefulSet".equals(ownerReference.getKind())) {
                                String stsName = ownerReference.getName();
                                String podName = resource.getMetadata().getName();
                                doRecreatePod(resource.getMetadata().getNamespace(),
                                        stsName,
                                        podName);
                            }
                        }
                    }
                }
            }
        });
        this.podDb = podDb;
        this.mockPods = mockPods;
        this.mockPvcs = mockPvcs;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void nameScopedMocks(String resourceName, RollableScalableResource<StatefulSet, DoneableStatefulSet> resource) {
        super.nameScopedMocks(resourceName, resource);
        EditReplacePatchDeletable<StatefulSet, StatefulSet, DoneableStatefulSet, Boolean> c = mock(EditReplacePatchDeletable.class);
        when(c.withGracePeriod(anyLong())).thenReturn(resource);
        when(resource.cascading(false)).thenReturn(c);
        mockNoncascadingPatch(resourceName, c);
        mockScale(resourceName, resource);
        mockNoncascadingDelete(resourceName, c);
    }

    private void mockNoncascadingDelete(String resourceName, EditReplacePatchDeletable<StatefulSet, StatefulSet, DoneableStatefulSet, Boolean> c) {
        when(c.delete()).thenAnswer(i -> {
            LOGGER.info("delete {} {}", resourceType, resourceName);
            StatefulSet removed = db.remove(resourceName);
            return removed != null;
        });
    }

    private void mockNoncascadingPatch(String resourceName, EditReplacePatchDeletable<StatefulSet, StatefulSet, DoneableStatefulSet, Boolean> c) {
        when(c.patch(any())).thenAnswer(patchInvocation -> {
            StatefulSet argument = patchInvocation.getArgument(0);
            return doPatch(resourceName, argument);
        });
    }

    private void mockScale(String resourceName, RollableScalableResource<StatefulSet, DoneableStatefulSet> resource) {
        when(resource.scale(anyInt(), anyBoolean())).thenAnswer(invocation -> {
            checkDoesExist(resourceName);
            StatefulSet ss = copyResource(db.get(resourceName));
            int newScale = invocation.getArgument(0);
            ss.getSpec().setReplicas(newScale);
            return doPatch(resourceName, ss);
        });
        when(resource.scale(anyInt())).thenAnswer(invocation -> {
            checkDoesExist(resourceName);
            StatefulSet ss = copyResource(db.get(resourceName));
            int newScale = invocation.getArgument(0);
            ss.getSpec().setReplicas(newScale);
            return doPatch(resourceName, ss);
        });
    }

    @Override
    protected void mockCreate(String resourceName, RollableScalableResource<StatefulSet, DoneableStatefulSet> resource) {
        when(resource.create(any())).thenAnswer(cinvocation -> {
            checkNotExists(resourceName);
            StatefulSet argument = cinvocation.getArgument(0);
            LOGGER.debug("create {} {} -> {}", resourceType, resourceName, argument);
            StatefulSet value = copyResource(argument);
            value.setStatus(new StatefulSetStatus());
            db.put(resourceName, value);
            for (int i = 0; i < argument.getSpec().getReplicas(); i++) {
                final int podNum = i;
                String podName = argument.getMetadata().getName() + "-" + podNum;
                LOGGER.debug("create Pod {} because it's in StatefulSet {}", podName, resourceName);
                mockPods.inNamespace(argument.getMetadata().getNamespace()).createOrReplace(doCreatePod(argument, podName));

                if (value.getSpec().getVolumeClaimTemplates().size() > 0) {

                    for (PersistentVolumeClaim pvcTemplate: value.getSpec().getVolumeClaimTemplates()) {

                        String pvcName = pvcTemplate.getMetadata().getName() + "-" + podName;
                        if (mockPvcs.inNamespace(argument.getMetadata().getNamespace()).withName(pvcName).get() == null) {

                            LOGGER.debug("create Pvc {} because it's in VolumeClaimTemplate of StatefulSet {}", pvcName, resourceName);
                            PersistentVolumeClaim pvc = new PersistentVolumeClaimBuilder()
                                    .withNewMetadata()
                                        .withLabels(argument.getSpec().getSelector().getMatchLabels())
                                        .withNamespace(argument.getMetadata().getNamespace())
                                        .withName(pvcName)
                                    .endMetadata()
                                    .build();
                            mockPvcs.inNamespace(argument.getMetadata().getNamespace()).withName(pvcName).create(pvc);
                        }
                    }

                }
            }
            return argument;
        });
    }

    private Pod doCreatePod(StatefulSet sts, String podName) {
        return new PodBuilder().withNewMetadataLike(sts.getSpec().getTemplate().getMetadata())
                                .withUid(UUID.randomUUID().toString())
                                .withNamespace(sts.getMetadata().getNamespace())
                                .withName(podName)
                                .addNewOwnerReference()
                                    .withKind(sts.getKind())
                                    .withName(sts.getMetadata().getName())
                                .endOwnerReference()
                            .endMetadata()
                            .withNewSpecLike(sts.getSpec().getTemplate().getSpec()).endSpec()
                            .build();
    }

    private void doRecreatePod(String namespace,
                               String stsName,
                               String podName) {
        int podNum = Integer.parseInt(podName.substring(stsName.length() + 1));
        StatefulSet statefulSet = db.get(stsName);
        if (statefulSet != null &&
                podNum < statefulSet.getSpec().getReplicas()) {
            Pod copy = doCreatePod(statefulSet, podName);
            LOGGER.debug("Recreating Pod {} because it's in StatefulSet {}", podName, stsName);
            mockPods.inNamespace(namespace).withName(podName).create(copy);
        }
    }

    @Override
    protected void mockDelete(String resourceName, RollableScalableResource<StatefulSet, DoneableStatefulSet> resource) {
        when(resource.delete()).thenAnswer(i -> {
            LOGGER.debug("delete {} {}", resourceType, resourceName);
            StatefulSet removed = db.remove(resourceName);
            if (removed != null) {
                fireWatchers(resourceName, removed, Watcher.Action.DELETED, "delete");
                for (Map.Entry<String, Pod> pod : new HashMap<>(podDb).entrySet()) {
                    if (pod.getKey().matches(resourceName + "-[0-9]+")) {
                        mockPods.inNamespace(removed.getMetadata().getNamespace()).withName(pod.getKey()).delete();
                    }
                }
            }
            return removed != null;
        });
    }

    private StatefulSet doPatch(String resourceName, StatefulSet argument) {
        int oldScale = db.get(resourceName).getSpec().getReplicas();
        int newScale = argument.getSpec().getReplicas();
        if (newScale > oldScale) {
            LOGGER.debug("scaling up {} {} from {} to {}", resourceType, resourceName, oldScale, newScale);
            Pod examplePod = mockPods.inNamespace(argument.getMetadata().getNamespace())
                    .withName(argument.getMetadata().getName() + "-0").get();
            for (int i = oldScale; i < newScale; i++) {
                String newPodName = argument.getMetadata().getName() + "-" + i;
                mockPods.inNamespace(argument.getMetadata().getNamespace()).withName(newPodName).create(
                        new PodBuilder(examplePod).editMetadata().withName(newPodName).endMetadata().build());
            }
            db.put(resourceName, copyResource(argument));
        } else if (newScale < oldScale) {
            db.put(resourceName, copyResource(argument));
            LOGGER.debug("scaling down {} {} from {} to {}", resourceType, resourceName, oldScale, newScale);
            for (int i = oldScale - 1; i >= newScale; i--) {
                String newPodName = argument.getMetadata().getName() + "-" + i;
                mockPods.inNamespace(argument.getMetadata().getNamespace()).withName(newPodName).delete();
            }
        } else {
            db.put(resourceName, copyResource(argument));
        }
        return argument;
    }
}
