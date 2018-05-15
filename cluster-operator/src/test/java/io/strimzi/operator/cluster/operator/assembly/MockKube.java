/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.DoneableEndpoints;
import io.fabric8.kubernetes.api.model.DoneablePersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentList;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.EditReplacePatchDeletable;
import io.fabric8.kubernetes.client.dsl.ExtensionsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.stubbing.OngoingStubbing;



import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockKube {

    private static final Logger LOGGER = LogManager.getLogger(MockKube.class);

    private final Map<String, ConfigMap> cmDb = db(emptySet(), ConfigMap.class, DoneableConfigMap.class);
    private final Collection<Watcher<ConfigMap>> cmWatchers = new ArrayList();
    private final Map<String, PersistentVolumeClaim> pvcDb = db(emptySet(), PersistentVolumeClaim.class, DoneablePersistentVolumeClaim.class);
    private final Map<String, Service> svcDb = db(emptySet(), Service.class, DoneableService.class);
    private final Map<String, Endpoints> endpointDb = db(emptySet(), Endpoints.class, DoneableEndpoints.class);
    private final Map<String, Pod> podDb = db(emptySet(), Pod.class, DoneablePod.class);
    private final Collection<Watcher<Pod>> podWatchers = new HashSet<>();
    private final Map<String, StatefulSet> ssDb = db(emptySet(), StatefulSet.class, DoneableStatefulSet.class);
    private final Map<String, Deployment> depDb = db(emptySet(), Deployment.class, DoneableDeployment.class);

    public MockKube withInitialCms(Set<ConfigMap> initialCms) {
        this.cmDb.putAll(db(initialCms, ConfigMap.class, DoneableConfigMap.class));
        return this;
    }

    public KubernetesClient build() {
        KubernetesClient mockClient = mock(KubernetesClient.class);
        MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> mockCms = buildConfigMaps();
        MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> mockPvcs = buildPvcs();
        MixedOperation<Endpoints, EndpointsList, DoneableEndpoints, Resource<Endpoints, DoneableEndpoints>> mockEndpoints = buildEndpoints();
        MixedOperation<Service, ServiceList, DoneableService, Resource<Service, DoneableService>> mockSvc = buildServices();
        MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods = buildPods();
        MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> mockSs = buildStatefulSets(mockPods);
        MixedOperation<Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> mockDep = buildDeployments();

        when(mockClient.configMaps()).thenReturn(mockCms);

        when(mockClient.services()).thenReturn(mockSvc);
        AppsAPIGroupDSL api = mock(AppsAPIGroupDSL.class);

        when(api.statefulSets()).thenReturn(mockSs);
        when(mockClient.apps()).thenReturn(api);
        ExtensionsAPIGroupDSL ext = mock(ExtensionsAPIGroupDSL.class);
        when(mockClient.extensions()).thenReturn(ext);
        when(ext.deployments()).thenReturn(mockDep);
        when(mockClient.pods()).thenReturn(mockPods);
        when(mockClient.endpoints()).thenReturn(mockEndpoints);
        when(mockClient.persistentVolumeClaims()).thenReturn(mockPvcs);

        return mockClient;
    }

    private MixedOperation<Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> buildDeployments() {
        return new AbstractMockBuilder<Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>>(
            Deployment.class, DeploymentList.class, DoneableDeployment.class, castClass(ScalableResource.class), depDb) {
            @Override
            protected void nameScopedMocks(ScalableResource<Deployment, DoneableDeployment> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
            }
        }.build();
    }

    private MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> buildStatefulSets(MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods) {
        return new AbstractMockBuilder<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>>(
                    StatefulSet.class, StatefulSetList.class, DoneableStatefulSet.class, castClass(RollableScalableResource.class), ssDb) {

                @Override
                protected void nameScopedMocks(RollableScalableResource<StatefulSet, DoneableStatefulSet> resource, String resourceName) {
                    mockGet(resourceName, resource);
                    //mockCreate("endpoint", endpointDb, resourceName, resource);
                    mockCascading(resource);
                    mockPatch(resourceName, resource);
                    mockDelete(resourceName, resource);
                    mockIsReady(resourceName, resource);
                    when(resource.create(any())).thenAnswer(cinvocation -> {
                        checkNotExists(resourceName);
                        StatefulSet argument = cinvocation.getArgument(0);
                        LOGGER.debug("create {} {} -> {}", resourceType, resourceName, argument);
                        ssDb.put(resourceName, copyResource(argument));
                        for (int i = 0; i < argument.getSpec().getReplicas(); i++) {
                            String podName = argument.getMetadata().getName() + "-" + i;
                            podDb.put(podName,
                                    new PodBuilder().withNewMetadata()
                                            .withNamespace(argument.getMetadata().getNamespace())
                                            .withName(podName)
                                            .endMetadata().build());
                        }
                        return argument;
                    });
                    EditReplacePatchDeletable<StatefulSet, StatefulSet, DoneableStatefulSet, Boolean> c = mock(EditReplacePatchDeletable.class);
                    when(resource.cascading(false)).thenReturn(c);
                    when(c.patch(any())).thenAnswer(patchInvocation -> {
                        StatefulSet argument = patchInvocation.getArgument(0);
                        return doPatch(resourceName, argument);
                    });
                    when(resource.scale(anyInt(), anyBoolean())).thenAnswer(invocation -> {
                        checkDoesExist(resourceName);
                        StatefulSet ss = copyResource(ssDb.get(resourceName));
                        int newScale = invocation.getArgument(0);
                        ss.getSpec().setReplicas(newScale);
                        return doPatch(resourceName, ss);
                    });
                    when(resource.scale(anyInt())).thenAnswer(invocation -> {
                        checkDoesExist(resourceName);
                        StatefulSet ss = copyResource(ssDb.get(resourceName));
                        int newScale = invocation.getArgument(0);
                        ss.getSpec().setReplicas(newScale);
                        return doPatch(resourceName, ss);
                    });
                    when(resource.isReady()).thenAnswer(i -> {
                        LOGGER.debug("{} {} is ready", resourceType, resourceName);
                        return true;
                    });

                    mockPods.inNamespace(any()).withName(any()).watch(new Watcher<Pod>() {
                        @Override
                        public void eventReceived(Action action, Pod resource) {
                            if (action == Action.DELETED) {
                                String podName = resource.getMetadata().getName();
                                String podNamespace = resource.getMetadata().getNamespace();
                                StatefulSet statefulSet = ssDb.get(resourceName);
                                if (podName.startsWith(resourceName + "-")
                                        && Integer.parseInt(podName.substring(podName.lastIndexOf("-") + 1)) <
                                        statefulSet.getSpec().getReplicas()) {
                                    mockPods.inNamespace(podNamespace).withName(podName).create(resource);
                                }
                            }
                        }

                        @Override
                        public void onClose(KubernetesClientException e) {

                        }
                    });
                }

                private StatefulSet doPatch(String resourceName, StatefulSet argument) {
                    int oldScale = ssDb.get(resourceName).getSpec().getReplicas();
                    int newScale = argument.getSpec().getReplicas();
                    if (newScale > oldScale) {
                        LOGGER.debug("scaling up {} {} from {} to {}", resourceType, resourceName, oldScale, newScale);
                        Pod examplePod = mockPods.inNamespace(argument.getMetadata().getNamespace()).withName(argument.getMetadata().getName() + "-0").get();
                        for (int i = oldScale; i < newScale; i++) {
                            String newPodName = argument.getMetadata().getName() + "-" + i;
                            mockPods.inNamespace(argument.getMetadata().getNamespace()).withName(newPodName).create(
                                    new PodBuilder(examplePod).editMetadata().withName(newPodName).endMetadata().build());
                        }
                        ssDb.put(resourceName, copyResource(argument));
                    } else if (newScale < oldScale) {
                        ssDb.put(resourceName, copyResource(argument));
                        LOGGER.debug("scaling down {} {} from {} to {}", resourceType, resourceName, oldScale, newScale);
                        for (int i = oldScale - 1; i >= newScale; i--) {
                            String newPodName = argument.getMetadata().getName() + "-" + i;
                            mockPods.inNamespace(argument.getMetadata().getNamespace()).withName(newPodName).delete();
                        }
                    } else {
                        ssDb.put(resourceName, copyResource(argument));
                    }
                    return argument;
                }
            }.build();
    }

    private MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> buildPods() {
        return new AbstractMockBuilder<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>>(
                Pod.class, PodList.class, DoneablePod.class, castClass(PodResource.class), podDb) {

            @Override
            protected void nameScopedMocks(PodResource<Pod, DoneablePod> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockWatch(resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
                mockIsReady(resourceName, resource);
            }
        }.build();
    }

    private MixedOperation<Service, ServiceList, DoneableService, Resource<Service, DoneableService>> buildServices() {
        return new AbstractMockBuilder<Service, ServiceList, DoneableService, Resource<Service, DoneableService>>(
                Service.class, ServiceList.class, DoneableService.class, castClass(Resource.class), svcDb) {

            @Override
            protected void nameScopedMocks(Resource<Service, DoneableService> resource, String resourceName) {
                mockGet(resourceName, resource);
                //mockCreate("endpoint", endpointDb, resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
                when(resource.create(any())).thenAnswer(i -> {
                    Service argument = i.getArgument(0);
                    svcDb.put(resourceName, copyResource(argument));
                    LOGGER.debug("create {} (and endpoint) {} ", resourceType, resourceName);
                    endpointDb.put(resourceName, new Endpoints());
                    return argument;
                });
            }
        }.build();
    }

    private MixedOperation<Endpoints, EndpointsList, DoneableEndpoints, Resource<Endpoints, DoneableEndpoints>> buildEndpoints() {
        return new AbstractMockBuilder<Endpoints, EndpointsList, DoneableEndpoints, Resource<Endpoints, DoneableEndpoints>>(
                Endpoints.class, EndpointsList.class, DoneableEndpoints.class, castClass(Resource.class), endpointDb) {
            @Override
            protected void nameScopedMocks(Resource<Endpoints, DoneableEndpoints> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
                mockIsReady(resourceName, resource);
            }
        }.build();
    }

    private MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> buildPvcs() {
        return new AbstractMockBuilder<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>>(
                PersistentVolumeClaim.class, PersistentVolumeClaimList.class, DoneablePersistentVolumeClaim.class, castClass(Resource.class), pvcDb) {
            @Override
            protected void nameScopedMocks(Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
            }
        }.build();
    }

    private MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> buildConfigMaps() {
        return new AbstractMockBuilder<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>>(
            ConfigMap.class, ConfigMapList.class, DoneableConfigMap.class, castClass(Resource.class), cmDb) {
            @Override
            protected void nameScopedMocks(Resource<ConfigMap, DoneableConfigMap> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockWatch(resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
            }
        }.build();
    }

    private static <T extends HasMetadata, D extends Doneable<T>> Map<String, T> db(Collection<T> initialResources, Class<T> cls, Class<D> doneableClass) {
        return new HashMap(initialResources.stream().collect(Collectors.toMap(
            c -> c.getMetadata().getName(),
            c -> copyResource(c, cls, doneableClass))));
    }

    private static <T extends HasMetadata, D extends Doneable<T>> T copyResource(T resource, Class<T> resourceClass, Class<D> doneableClass) {
        try {
            D doneableInstance = doneableClass.getDeclaredConstructor(resourceClass).newInstance(resource);
            T done = (T) Doneable.class.getMethod("done").invoke(doneableInstance);
            return done;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * @param db In-memory db of resources (e.g. ConfigMap's by their name)
     * @param resourceClass The type of {@link Resource} class
     * @param extraMocksOnResource A callback for adding extra mocks to the mock for the Resource type.
     *                             This is necessary for those things like scalable and "ready-able" resources.
     * @param <CM> The type of resource (e.g. ConfigMap)
     * @param <CML> The type of listable resource
     * @param <DCM> The type of doneable resource
     * @param <R> The type of the Resource
     */
    static abstract class AbstractMockBuilder<CM extends HasMetadata,
            CML extends KubernetesResource/*<CM>*/ & KubernetesResourceList/*<CM>*/,
            DCM extends Doneable<CM>,
            R extends Resource<CM, DCM>> {

        protected final Class<CM> resourceTypeClass;
        protected final Class<DCM> doneableClass;
        protected final Class<R> resourceClass;
        private final Map<String, CM> db;
        protected final Class<CML> listClass;
        protected final String resourceType;
        protected final Collection<Watcher<CM>> watchers = new ArrayList<>();

        public AbstractMockBuilder(Class<CM> resourceTypeClass, Class<CML> listClass, Class<DCM> doneableClass, Class<R> resourceClass, Map<String, CM> db) {
            this.resourceTypeClass = resourceTypeClass;
            this.resourceType = resourceTypeClass.getSimpleName();
            this.doneableClass = doneableClass;
            this.resourceClass = resourceClass;
            this.db = db;
            this.listClass = listClass;
        }

        protected CM copyResource(CM resource) {
            if (resource == null) {
                return null;
            }
            try {
                DCM doneableInstance = doneableClass.getDeclaredConstructor(resourceTypeClass).newInstance(resource);
                CM done = (CM) Doneable.class.getMethod("done").invoke(doneableInstance);
                return done;
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Generate a stateful mock for CRUD-like interactions.
         * @return The mock
         */
        public MixedOperation<CM, CML, DCM, R> build() {
            MixedOperation<CM, CML, DCM, R> mixed = mock(MixedOperation.class);

            when(mixed.inNamespace(any())).thenReturn(mixed);
            when(mixed.list()).thenAnswer(i -> mockList(p -> true));
            when(mixed.withLabels(any())).thenAnswer(i -> {
                MixedOperation<CM, CML, DCM, R> mixedWithLabels = mock(MixedOperation.class);
                Map<String, String> labels = i.getArgument(0);
                when(mixedWithLabels.list()).thenAnswer(i2 -> mockList(p -> {
                    Map<String, String> m = new HashMap(p.getMetadata().getLabels());
                    m.keySet().retainAll(labels.keySet());
                    return labels.equals(m);
                }));
                return mixedWithLabels;
            });
            when(mixed.withName(any())).thenAnswer(invocation -> {
                String resourceName = invocation.getArgument(0);
                R resource = mock(resourceClass);
                nameScopedMocks(resource, resourceName);
                return resource;
            });
            return mixed;
        }

        private KubernetesResourceList<CM> mockList(Predicate<? super CM> predicate) {
            KubernetesResourceList<CM> l = mock(listClass);
            Collection<CM> values = db.values().stream().filter(predicate).map(resource -> copyResource(resource)).collect(Collectors.toList());
            when(l.getItems()).thenAnswer(i3 -> {
                LOGGER.debug("{} list -> {}", resourceTypeClass.getSimpleName(), values);
                return values;
            });
            return l;
        }

        /**
         * Mock operations on the given {@code resource} which are scoped to accessing the given {@code resourceName}.
         * For example the methods accessible from
         * {@code client.configMaps().inNamespace(ns).withName(resourceName)...}
         *
         * @param resource
         * @param resourceName
         */
        protected abstract void nameScopedMocks(R resource, String resourceName);

        protected void checkNotExists(String resourceName) {
            if (db.containsKey(resourceName)) {
                throw new KubernetesClientException(resourceType + " " + resourceName + " already exists");
            }
        }

        protected void checkDoesExist(String resourceName) {
            if (!db.containsKey(resourceName)) {
                throw new KubernetesClientException(resourceType + " " + resourceName + " does not exist");
            }
        }

        protected void mockDelete(String resourceName, R resource) {
            when(resource.delete()).thenAnswer(i -> {
                LOGGER.debug("delete {} {}", resourceType, resourceName);
                CM removed = db.remove(resourceName);
                if (removed != null && watchers != null) {
                    for (Watcher<CM> watcher : watchers) {
                        watcher.eventReceived(Watcher.Action.DELETED, removed);
                    }
                }
                return removed != null;
            });
        }

        protected void mockPatch(String resourceName, R resource) {
            when(resource.patch(any())).thenAnswer(invocation -> {
                checkDoesExist(resourceName);
                CM argument = copyResource(invocation.getArgument(0));
                LOGGER.debug("patch {} {} -> {}", resourceType, resourceName, resource);
                db.put(resourceName, argument);
                if (watchers != null) {
                    for (Watcher<CM> watcher : watchers) {
                        watcher.eventReceived(Watcher.Action.MODIFIED, argument);
                    }
                }
                return argument;
            });
        }

        protected void mockCascading(R resource) {
            EditReplacePatchDeletable<CM, CM, DCM, Boolean> c = mock(EditReplacePatchDeletable.class);
            when(resource.cascading(true)).thenReturn(c);
        }

        protected void mockWatch(R resource) {
            when(resource.watch(any())).thenAnswer(i -> {
                Watcher<CM> argument = (Watcher<CM>) i.getArguments()[0];
                LOGGER.debug("watch {} {} ", resourceType, argument);
                watchers.add(argument);
                Watch watch = mock(Watch.class);
                doAnswer(z -> {
                    watchers.remove(argument);
                    return null;
                }).when(watch).close();
                return watch;
            });
        }

        protected void mockCreate(String resourceName, R resource) {
            when(resource.create(any())).thenAnswer(i -> {
                checkNotExists(resourceName);
                CM argument = (CM) i.getArguments()[0];
                LOGGER.debug("create {} {} -> {}", resourceType, resourceName, argument);
                db.put(resourceName, copyResource(argument));
                if (watchers != null) {
                    for (Watcher<CM> watcher : watchers) {
                        watcher.eventReceived(Watcher.Action.ADDED, argument);
                    }
                }
                return argument;
            });
        }

        protected OngoingStubbing<CM> mockGet(String resourceName, R resource) {
            return when(resource.get()).thenAnswer(i -> {
                CM r = copyResource(db.get(resourceName));
                LOGGER.debug("{} {} get {}", resourceType, resourceName, r);
                return r;
            });
        }

        protected OngoingStubbing<Boolean> mockIsReady(String resourceName, R resource) {
            return when(resource.isReady()).thenAnswer(i -> {
                LOGGER.debug("{} {} is ready", resourceType, resourceName);
                return Boolean.TRUE;
            });
        }
    }

    /**
     * This method is just used to appease javac and avoid having a very ugly "double cast" (cast to raw Class,
     * followed by a cast to parameterised Class) in all the calls to
     * {@link AbstractMockBuilder#AbstractMockBuilder(Class, Class, Class, Class, Map)}
     */
    @SuppressWarnings("unchecked")
    private static <T extends HasMetadata, D extends Doneable<T>, R extends Resource<T, D>, R2 extends Resource>
            Class<R> castClass(Class<R2> c) {
        return (Class) c;
    }



}
