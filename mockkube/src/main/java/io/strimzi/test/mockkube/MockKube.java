/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.mockkube;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.DoneableEndpoints;
import io.fabric8.kubernetes.api.model.DoneablePersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.DoneableSecret;
import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.DoneableServiceAccount;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountList;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.CustomResourceDefinitionList;
import io.fabric8.kubernetes.api.model.apiextensions.DoneableCustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.fabric8.kubernetes.api.model.apps.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetList;
import io.fabric8.kubernetes.api.model.apps.StatefulSetStatus;
import io.fabric8.kubernetes.api.model.networking.DoneableNetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.NetworkPolicyList;
import io.fabric8.kubernetes.api.model.policy.DoneablePodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.PodDisruptionBudgetList;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingList;
import io.fabric8.kubernetes.api.model.rbac.DoneableClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.DoneableRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingList;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.AppsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.CreateOrReplaceable;
import io.fabric8.kubernetes.client.dsl.EditReplacePatchDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NetworkAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.PolicyAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.RbacAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.fabric8.openshift.api.model.DoneableRoute;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteList;
import io.fabric8.openshift.client.OpenShiftClient;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"checkstyle:ClassFanOutComplexity"})
public class MockKube {
    private static final Logger LOGGER = LogManager.getLogger(MockKube.class);

    private final Map<String, ConfigMap> cmDb = db(emptySet(), ConfigMap.class, DoneableConfigMap.class);
    private final Map<String, PersistentVolumeClaim> pvcDb = db(emptySet(), PersistentVolumeClaim.class, DoneablePersistentVolumeClaim.class);
    private final Map<String, Service> svcDb = db(emptySet(), Service.class, DoneableService.class);
    private final Map<String, Endpoints> endpointDb = db(emptySet(), Endpoints.class, DoneableEndpoints.class);
    private final Map<String, Pod> podDb = db(emptySet(), Pod.class, DoneablePod.class);
    private final Map<String, StatefulSet> ssDb = db(emptySet(), StatefulSet.class, DoneableStatefulSet.class);
    private final Map<String, Deployment> depDb = db(emptySet(), Deployment.class, DoneableDeployment.class);
    private final Map<String, Secret> secretDb = db(emptySet(), Secret.class, DoneableSecret.class);
    private final Map<String, ServiceAccount> serviceAccountDb = db(emptySet(), ServiceAccount.class, DoneableServiceAccount.class);
    private final Map<String, NetworkPolicy> policyDb = db(emptySet(), NetworkPolicy.class, DoneableNetworkPolicy.class);
    private final Map<String, Route> routeDb = db(emptySet(), Route.class, DoneableRoute.class);
    private final Map<String, PodDisruptionBudget> pdbDb = db(emptySet(), PodDisruptionBudget.class, DoneablePodDisruptionBudget.class);
    private final Map<String, RoleBinding> pdbRb = db(emptySet(), RoleBinding.class,
            DoneableRoleBinding.class);
    private final Map<String, ClusterRoleBinding> pdbCrb = db(emptySet(), ClusterRoleBinding.class,
            DoneableClusterRoleBinding.class);

    private Map<String, List<String>> podsForDeployments = new HashMap<>();
    private Map<String, CreateOrReplaceable> crdMixedOps = new HashMap<>();

    public MockKube withInitialCms(Set<ConfigMap> initialCms) {
        this.cmDb.putAll(db(initialCms, ConfigMap.class, DoneableConfigMap.class));
        return this;
    }

    public MockKube withInitialStatefulSets(Set<StatefulSet> initial) {
        this.ssDb.putAll(db(initial, StatefulSet.class, DoneableStatefulSet.class));
        return this;
    }

    public MockKube withInitialPods(Set<Pod> initial) {
        this.podDb.putAll(db(initial, Pod.class, DoneablePod.class));
        return this;
    }

    public MockKube withInitialSecrets(Set<Secret> initial) {
        this.secretDb.putAll(db(initial, Secret.class, DoneableSecret.class));
        return this;
    }

    public MockKube withInitialNetworkPolicy(Set<NetworkPolicy> initial) {
        this.policyDb.putAll(db(initial, NetworkPolicy.class, DoneableNetworkPolicy.class));
        return this;
    }

    public MockKube withInitialRoute(Set<Route> initial) {
        this.routeDb.putAll(db(initial, Route.class, DoneableRoute.class));
        return this;
    }

    private final List<MockedCrd> mockedCrds = new ArrayList<>();

    public class MockedCrd<T extends CustomResource, L extends KubernetesResourceList<T>, D extends Doneable<T>> {
        private final CustomResourceDefinition crd;
        private final Class<T> crClass;
        private final Class<L> crListClass;
        private final Class<D> crDoneableClass;
        private final Map<String, T> instances;
        private MockedCrd(CustomResourceDefinition crd, Class<T> crClass, Class<L> crListClass, Class<D> crDoneableClass) {
            this.crd = crd;
            this.crClass = crClass;
            this.crListClass = crListClass;
            this.crDoneableClass = crDoneableClass;
            instances = db(emptySet(), crClass, crDoneableClass);
        }
        public MockedCrd<T, L, D> withInitialInstances(Set<T> instances) {
            for (T instance : instances) {
                this.instances.put(instance.getMetadata().getName(), instance);
            }
            return this;
        }
        public MockKube end() {
            return MockKube.this;
        }
    }

    public <T extends CustomResource, L extends KubernetesResourceList<T>, D extends Doneable<T>> MockedCrd<T, L, D>
            withCustomResourceDefinition(CustomResourceDefinition crd, Class<T> instanceClass, Class<L> instanceListClass, Class<D> doneableInstanceClass) {
        MockedCrd<T, L, D> mockedCrd = new MockedCrd<>(crd, instanceClass, instanceListClass, doneableInstanceClass);
        this.mockedCrds.add(mockedCrd);
        return mockedCrd;
    }

    @SuppressWarnings("unchecked")
    public KubernetesClient build() {
        KubernetesClient mockClient = mock(KubernetesClient.class);
        OpenShiftClient mockOpenShiftClient = mock(OpenShiftClient.class);
        MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> mockCms = buildConfigMaps();
        MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim,
                Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> mockPvcs = buildPvcs();
        MixedOperation<Endpoints, EndpointsList, DoneableEndpoints, Resource<Endpoints, DoneableEndpoints>> mockEndpoints = buildEndpoints();
        MixedOperation<Service, ServiceList, DoneableService, ServiceResource<Service, DoneableService>> mockSvc = buildServices();
        MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods = buildPods();
        MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet,
                RollableScalableResource<StatefulSet, DoneableStatefulSet>> mockSs = buildStatefulSets(mockPods, mockPvcs);
        MixedOperation<Deployment, DeploymentList, DoneableDeployment, RollableScalableResource<Deployment,
                DoneableDeployment>> mockDep = buildDeployments(mockPods);
        MixedOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> mockSecrets = buildSecrets();
        MixedOperation<ServiceAccount, ServiceAccountList, DoneableServiceAccount, Resource<ServiceAccount,
                DoneableServiceAccount>> mockServiceAccounts = buildServiceAccount();
        MixedOperation<NetworkPolicy, NetworkPolicyList, DoneableNetworkPolicy, Resource<NetworkPolicy,
                DoneableNetworkPolicy>> mockNetworkPolicy = buildNetworkPolicy();
        MixedOperation<Route, RouteList, DoneableRoute, Resource<Route, DoneableRoute>> mockRoute = buildRoute();
        MixedOperation<PodDisruptionBudget, PodDisruptionBudgetList, DoneablePodDisruptionBudget,
                Resource<PodDisruptionBudget, DoneablePodDisruptionBudget>> mockPdb = buildPdb();
        MixedOperation<RoleBinding, RoleBindingList, DoneableRoleBinding,
                Resource<RoleBinding, DoneableRoleBinding>> mockRb = buildRb();
        MixedOperation<ClusterRoleBinding, ClusterRoleBindingList, DoneableClusterRoleBinding,
                Resource<ClusterRoleBinding, DoneableClusterRoleBinding>> mockCrb = buildCrb();

        when(mockClient.configMaps()).thenReturn(mockCms);
        when(mockClient.services()).thenReturn(mockSvc);
        AppsAPIGroupDSL api = mock(AppsAPIGroupDSL.class);

        when(api.statefulSets()).thenReturn(mockSs);
        when(api.deployments()).thenReturn(mockDep);
        when(mockClient.apps()).thenReturn(api);
        when(mockClient.pods()).thenReturn(mockPods);
        when(mockClient.endpoints()).thenReturn(mockEndpoints);
        when(mockClient.persistentVolumeClaims()).thenReturn(mockPvcs);
        if (mockedCrds != null && !mockedCrds.isEmpty()) {
            NonNamespaceOperation<CustomResourceDefinition, CustomResourceDefinitionList, DoneableCustomResourceDefinition,
                    Resource<CustomResourceDefinition, DoneableCustomResourceDefinition>>
                    crds = mock(NonNamespaceOperation.class);
            for (MockedCrd<?, ?, ?> mockedCrd : this.mockedCrds) {
                CustomResourceDefinition crd = mockedCrd.crd;
                Resource crdResource = mock(Resource.class);
                when(crdResource.get()).thenReturn(crd);
                when(crds.withName(crd.getMetadata().getName())).thenReturn(crdResource);
                when(mockClient.customResources(any(CustomResourceDefinition.class), any(Class.class), any(Class.class),
                        any(Class.class))).thenAnswer(invocation -> {
                            CustomResourceDefinition crdArg = invocation.getArgument(0);
                            if (crd.getSpec().getGroup().equals(crdArg.getSpec().getGroup())
                                    && crd.getSpec().getVersion().equals(crdArg.getSpec().getVersion())) {
                                String key = crdArg.getSpec().getGroup() + "##" + crdArg.getSpec().getVersion();
                                CreateOrReplaceable crdMixedOp = crdMixedOps.get(key);
                                if (crdMixedOp == null) {
                                    crdMixedOp = buildCrd((MockedCrd) mockedCrd);
                                    crdMixedOps.put(key, crdMixedOp);
                                }
                                return crdMixedOp;
                            } else {
                                throw new RuntimeException();
                            }
                        });
            }
            when(mockClient.customResourceDefinitions()).thenReturn(crds);
        }

        when(mockClient.secrets()).thenReturn(mockSecrets);
        when(mockClient.serviceAccounts()).thenReturn(mockServiceAccounts);
        NetworkAPIGroupDSL network = mock(NetworkAPIGroupDSL.class);
        when(mockClient.network()).thenReturn(network);
        when(network.networkPolicies()).thenReturn(mockNetworkPolicy);
        when(mockClient.adapt(OpenShiftClient.class)).thenReturn(mockOpenShiftClient);
        when(mockOpenShiftClient.routes()).thenReturn(mockRoute);
        PolicyAPIGroupDSL policy = mock(PolicyAPIGroupDSL.class);
        when(mockClient.policy()).thenReturn(policy);
        RbacAPIGroupDSL rbac = mock(RbacAPIGroupDSL.class);
        when(mockClient.rbac()).thenReturn(rbac);
        when(mockClient.policy().podDisruptionBudget()).thenReturn(mockPdb);
        when(mockClient.rbac().roleBindings()).thenReturn(mockRb);
        when(mockClient.rbac().clusterRoleBindings()).thenReturn(mockCrb);

        mockHttpClient(mockClient);

        return mockClient;
    }

    private void mockHttpClient(KubernetesClient mockClient)   {
        // The CRD status update is build on the HTTP client directly since it is not supported in Fabric8.
        // We have to mock the HTTP client to make it pass.
        URL fakeUrl = null;
        try {
            fakeUrl = new URL("http", "my-host", 9443, "/");
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        when(mockClient.getMasterUrl()).thenReturn(fakeUrl);
        OkHttpClient mockedOkHttp = mock(OkHttpClient.class);
        when(mockClient.adapt(OkHttpClient.class)).thenReturn(mockedOkHttp);
        Call mockedCall = mock(Call.class);
        when(mockedOkHttp.newCall(any(Request.class))).thenReturn(mockedCall);
        Response response = new Response.Builder().code(200).request(new Request.Builder().url(fakeUrl).build()).message("HTTP OK").protocol(Protocol.HTTP_1_1).build();
        try {
            when(mockedCall.execute()).thenReturn(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private MixedOperation<Deployment, DeploymentList, DoneableDeployment, RollableScalableResource<Deployment, DoneableDeployment>>
            buildDeployments(MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods) {
        return new AbstractMockBuilder<Deployment, DeploymentList, DoneableDeployment, RollableScalableResource<Deployment,
                DoneableDeployment>>(
            Deployment.class, DeploymentList.class, DoneableDeployment.class, castClass(RollableScalableResource.class), depDb) {
            @Override
            protected void nameScopedMocks(RollableScalableResource<Deployment, DoneableDeployment> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockWatch(resourceName, resource);
                //mockCreate(resourceName, resource);
                when(resource.create(any())).thenAnswer(invocation -> {
                    checkNotExists(resourceName);
                    Deployment deployment = invocation.getArgument(0);
                    LOGGER.debug("create {} {} -> {}", resourceType, resourceName, deployment);
                    depDb.put(resourceName, copyResource(deployment));
                    for (int i = 0; i < deployment.getSpec().getReplicas(); i++) {
                        String uuid = UUID.randomUUID().toString();
                        String podName = deployment.getMetadata().getName() + "-" + uuid;
                        LOGGER.debug("create Pod {} because it's in Deployment {}", podName, resourceName);
                        Pod pod = new PodBuilder()
                                .withNewMetadataLike(deployment.getSpec().getTemplate().getMetadata())
                                    .withUid(uuid)
                                    .withNamespace(deployment.getMetadata().getNamespace())
                                    .withName(podName)
                                .endMetadata()
                                .withNewSpecLike(deployment.getSpec().getTemplate().getSpec()).endSpec()
                                .build();
                        mockPods.inNamespace(deployment.getMetadata().getNamespace()).withName(podName).create(pod);
                        podsForDeployments.computeIfAbsent(deployment.getMetadata().getName(), s -> new ArrayList<>());
                        podsForDeployments.get(deployment.getMetadata().getName()).add(podName);
                    }
                    return deployment;
                });
                mockCascading(resource);
                //mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
                when(resource.patch(any())).thenAnswer(invocation -> {
                    Deployment deployment = invocation.getArgument(0);
                    LOGGER.debug("patched {} {} -> {}", resourceType, resourceName, deployment);
                    depDb.put(resourceName, copyResource(deployment));
                    List<String> newPodNames = new ArrayList<>();
                    for (int i = 0; i < deployment.getSpec().getReplicas(); i++) {
                        // create a "new" Pod
                        String uuid = UUID.randomUUID().toString();
                        String newPodName = deployment.getMetadata().getName() + "-" + uuid;

                        Pod newPod = new PodBuilder()
                                .withNewMetadataLike(deployment.getSpec().getTemplate().getMetadata())
                                    .withUid(uuid)
                                    .withNamespace(deployment.getMetadata().getNamespace())
                                    .withName(newPodName)
                                .endMetadata()
                                .withNewSpecLike(deployment.getSpec().getTemplate().getSpec()).endSpec()
                                .build();
                        mockPods.inNamespace(deployment.getMetadata().getNamespace()).withName(newPodName).create(newPod);
                        newPodNames.add(newPodName);

                        // delete the first one "old" Pod
                        String podToDelete = podsForDeployments.get(deployment.getMetadata().getName()).remove(0);
                        mockPods.inNamespace(deployment.getMetadata().getNamespace()).withName(podToDelete).delete();
                    }
                    podsForDeployments.get(deployment.getMetadata().getName()).addAll(newPodNames);

                    return deployment;
                });
            }
        }.build();
    }

    private MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>>
        buildStatefulSets(MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods,
                      MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim,
                              Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> mockPvcs) {
        MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet,
                DoneableStatefulSet>> result = new AbstractMockBuilder<StatefulSet, StatefulSetList, DoneableStatefulSet,
                RollableScalableResource<StatefulSet, DoneableStatefulSet>>(
                StatefulSet.class, StatefulSetList.class, DoneableStatefulSet.class, castClass(RollableScalableResource.class), ssDb) {

                    @Override
                    @SuppressWarnings("unchecked")
                    protected void nameScopedMocks(RollableScalableResource<StatefulSet, DoneableStatefulSet> resource, String resourceName) {
                        mockGet(resourceName, resource);
                        //mockCreate("endpoint", endpointDb, resourceName, resource);
                        mockCascading(resource);
                        mockPatch(resourceName, resource);
                        when(resource.delete()).thenAnswer(i -> {
                            LOGGER.debug("delete {} {}", resourceType, resourceName);
                            StatefulSet removed = ssDb.remove(resourceName);
                            if (removed != null) {
                                fireWatchers(resourceName, removed, Watcher.Action.DELETED);
                                for (Map.Entry<String, Pod> pod : new HashMap<>(podDb).entrySet()) {
                                    if (pod.getKey().matches(resourceName + "-[0-9]+")) {
                                        mockPods.inNamespace(removed.getMetadata().getNamespace()).withName(pod.getKey()).delete();
                                    }
                                }
                            }
                            return removed != null;
                        });
                        mockIsReady(resourceName, resource);
                        when(resource.create(any())).thenAnswer(cinvocation -> {
                            checkNotExists(resourceName);
                            StatefulSet argument = cinvocation.getArgument(0);
                            LOGGER.debug("create {} {} -> {}", resourceType, resourceName, argument);
                            StatefulSet value = copyResource(argument);
                            value.setStatus(new StatefulSetStatus());
                            ssDb.put(resourceName, value);
                            for (int i = 0; i < argument.getSpec().getReplicas(); i++) {
                                final int podNum = i;
                                String podName = argument.getMetadata().getName() + "-" + podNum;
                                LOGGER.debug("create Pod {} because it's in StatefulSet {}", podName, resourceName);
                                Pod pod = new PodBuilder().withNewMetadataLike(argument.getSpec().getTemplate().getMetadata())
                                        .withUid(UUID.randomUUID().toString())
                                        .withNamespace(argument.getMetadata().getNamespace())
                                        .withName(podName)
                                        .endMetadata()
                                        .withNewSpecLike(argument.getSpec().getTemplate().getSpec()).endSpec()
                                        .build();
                                //podDb.put(podName,
                                //        pod);
                                if (mockPods.inNamespace(argument.getMetadata().getNamespace()).withName(podName).get() == null) {
                                    mockPods.inNamespace(argument.getMetadata().getNamespace()).withName(podName).create(pod);
                                    addPodRestarter(mockPods, resourceName, podNum, podName);
                                }

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
                        when(c.delete()).thenAnswer(i -> {
                            LOGGER.info("delete {} {}", resourceType, resourceName);
                            StatefulSet removed = ssDb.remove(resourceName);
                            return removed != null;
                        });
                    }

                    private StatefulSet doPatch(String resourceName, StatefulSet argument) {
                        int oldScale = ssDb.get(resourceName).getSpec().getReplicas();
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

        for (StatefulSet ss : this.ssDb.values()) {
            for (Pod initialPod : this.podDb.values()) {
                String podName = initialPod.getMetadata().getName();
                String ssName = ss.getMetadata().getName();
                if (podName.matches(ssName + "-[0-9]+")) {
                    addPodRestarter(
                            mockPods,
                            ssName,
                            Integer.parseInt(podName.substring(podName.lastIndexOf("-") + 1)),
                            podName);
                }
            }
        }

        return result;
    }

    private void addPodRestarter(MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> mockPods,
                                 String resourceName, int podNum, String podName) {
        mockPods.inNamespace(any()).withName(podName).watch(new Watcher<Pod>() {
            public String toString() {
                return MockKube.class.getSimpleName() + " SS pod restarter";
            }

            @Override
            public void eventReceived(Action action, Pod resource) {
                if (action == Action.DELETED) {
                    //String podName = resource.getMetadata().getName();
                    String podNamespace = resource.getMetadata().getNamespace();
                    StatefulSet statefulSet = ssDb.get(resourceName);
                    if (/*podName.matches(resourceName + "-" + "[0-9]+")
                                    && */
                            statefulSet != null &&
                            podNum <
                            statefulSet.getSpec().getReplicas()) {
                        ObjectMeta templateMeta = statefulSet.getSpec().getTemplate().getMetadata();
                        Pod copy = new DoneablePod(resource)
                                .withNewMetadataLike(templateMeta)
                                .withUid(UUID.randomUUID().toString())
                                .withNamespace(podNamespace)
                                .withName(podName)
                                .endMetadata()
                                .withNewSpecLike(statefulSet.getSpec().getTemplate().getSpec()).endSpec()
                                .done();
                        LOGGER.debug("Recreating Pod {} because it's in StatefulSet {}", podName, resourceName);
                        mockPods.inNamespace(podNamespace).withName(podName).create(copy);
                    }
                }
            }

            @Override
            public void onClose(KubernetesClientException e) {

            }
        });
    }

    private MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> buildPods() {
        return new AbstractMockBuilder<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>>(
                Pod.class, PodList.class, DoneablePod.class, castClass(PodResource.class), podDb) {

            @Override
            protected void nameScopedMocks(PodResource<Pod, DoneablePod> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockWatch(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
                mockIsReady(resourceName, resource);
            }
        }.build();
    }

    private MixedOperation<Service, ServiceList, DoneableService, ServiceResource<Service, DoneableService>> buildServices() {
        return new AbstractMockBuilder<Service, ServiceList, DoneableService, ServiceResource<Service, DoneableService>>(
                Service.class, ServiceList.class, DoneableService.class, castClass(ServiceResource.class), svcDb) {

            @Override
            protected void nameScopedMocks(ServiceResource<Service, DoneableService> resource, String resourceName) {
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

    private MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim,
            Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> buildPvcs() {
        return new AbstractMockBuilder<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim,
                Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>>(
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
                mockWatch(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
            }
        }.build();
    }


    private MixedOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> buildSecrets() {
        return new AbstractMockBuilder<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>>(
                Secret.class, SecretList.class, DoneableSecret.class, castClass(Resource.class), secretDb) {
            @Override
            protected void nameScopedMocks(Resource<Secret, DoneableSecret> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
            }
        }.build();
    }

    private MixedOperation<ServiceAccount, ServiceAccountList, DoneableServiceAccount, Resource<ServiceAccount, DoneableServiceAccount>>
        buildServiceAccount() {
        return new AbstractMockBuilder<ServiceAccount, ServiceAccountList, DoneableServiceAccount, Resource<ServiceAccount, DoneableServiceAccount>>(
                ServiceAccount.class, ServiceAccountList.class, DoneableServiceAccount.class, castClass(Resource.class), serviceAccountDb) {
            @Override
            protected void nameScopedMocks(Resource<ServiceAccount, DoneableServiceAccount> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
            }
        }.build();
    }

    private MixedOperation<NetworkPolicy, NetworkPolicyList, DoneableNetworkPolicy, Resource<NetworkPolicy, DoneableNetworkPolicy>> buildNetworkPolicy() {
        return new AbstractMockBuilder<NetworkPolicy, NetworkPolicyList, DoneableNetworkPolicy, Resource<NetworkPolicy, DoneableNetworkPolicy>>(
                NetworkPolicy.class, NetworkPolicyList.class, DoneableNetworkPolicy.class, castClass(Resource.class), policyDb) {
            @Override
            protected void nameScopedMocks(Resource<NetworkPolicy, DoneableNetworkPolicy> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
            }
        }.build();
    }

    private MixedOperation<Route, RouteList, DoneableRoute, Resource<Route, DoneableRoute>> buildRoute() {
        return new AbstractMockBuilder<Route, RouteList, DoneableRoute, Resource<Route, DoneableRoute>>(
                Route.class, RouteList.class, DoneableRoute.class, castClass(Resource.class), routeDb) {
            @Override
            protected void nameScopedMocks(Resource<Route, DoneableRoute> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
            }
        }.build();
    }

    private MixedOperation<PodDisruptionBudget, PodDisruptionBudgetList, DoneablePodDisruptionBudget,
            Resource<PodDisruptionBudget, DoneablePodDisruptionBudget>> buildPdb() {
        return new AbstractMockBuilder<PodDisruptionBudget, PodDisruptionBudgetList, DoneablePodDisruptionBudget,
                Resource<PodDisruptionBudget, DoneablePodDisruptionBudget>>(
                PodDisruptionBudget.class, PodDisruptionBudgetList.class, DoneablePodDisruptionBudget.class, castClass(Resource.class), pdbDb) {
            @Override
            protected void nameScopedMocks(Resource<PodDisruptionBudget, DoneablePodDisruptionBudget> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
            }
        }.build();
    }

    private MixedOperation<RoleBinding, RoleBindingList, DoneableRoleBinding,
            Resource<RoleBinding, DoneableRoleBinding>> buildRb() {
        return new AbstractMockBuilder<RoleBinding, RoleBindingList, DoneableRoleBinding,
                Resource<RoleBinding, DoneableRoleBinding>>(
                RoleBinding.class, RoleBindingList.class, DoneableRoleBinding.class, castClass(Resource.class), pdbRb) {
            @Override
            protected void nameScopedMocks(Resource<RoleBinding, DoneableRoleBinding> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
            }
        }.build();
    }

    private MixedOperation<ClusterRoleBinding, ClusterRoleBindingList, DoneableClusterRoleBinding,
            Resource<ClusterRoleBinding, DoneableClusterRoleBinding>> buildCrb() {
        return new AbstractMockBuilder<ClusterRoleBinding, ClusterRoleBindingList,
                DoneableClusterRoleBinding, Resource<ClusterRoleBinding, DoneableClusterRoleBinding>>(
                ClusterRoleBinding.class, ClusterRoleBindingList.class, DoneableClusterRoleBinding.class,
                castClass(Resource.class), pdbCrb) {
            @Override
            protected void nameScopedMocks(Resource<ClusterRoleBinding, DoneableClusterRoleBinding> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
            }
        }.build();
    }

    private <T extends CustomResource,
            L extends KubernetesResource & KubernetesResourceList<T>,
            D extends Doneable<T>>
        MixedOperation<T, L, D, Resource<T, D>> buildCrd(MockedCrd<T, L, D> mockedCrd) {
        return new AbstractMockBuilder<T, L, D, Resource<T, D>>(
                mockedCrd.crClass, mockedCrd.crListClass, mockedCrd.crDoneableClass, castClass(Resource.class), mockedCrd.instances) {
            @Override
            protected void nameScopedMocks(Resource<T, D> resource, String resourceName) {
                mockGet(resourceName, resource);
                mockWatch(resourceName, resource);
                mockCreate(resourceName, resource);
                mockCascading(resource);
                mockPatch(resourceName, resource);
                mockDelete(resourceName, resource);
            }
        }.build();
    }

    private static <T extends HasMetadata, D extends Doneable<T>> Map<String, T> db(Collection<T> initialResources, Class<T> cls, Class<D> doneableClass) {
        return new ConcurrentHashMap<>(initialResources.stream().collect(Collectors.toMap(
            c -> c.getMetadata().getName(),
            c -> copyResource(c, cls, doneableClass))));
    }

    @SuppressWarnings("unchecked")
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

        static class PredicatedWatcher<CM extends HasMetadata> {
            private final String str;
            private final Watcher<CM> watcher;
            private final Predicate<CM> predicate;

            private PredicatedWatcher(String str, Predicate<CM> predicate, Watcher<CM> watcher) {
                this.str = str;
                this.watcher = watcher;
                this.predicate = predicate;
            }

            static <CM extends HasMetadata> PredicatedWatcher<CM> watcher(Watcher<CM> watcher) {
                return new PredicatedWatcher<>("watch on all", resource1 -> ((Predicate<CM>) resource -> true).test(resource1), watcher);
            }

            static <CM extends HasMetadata> PredicatedWatcher<CM> namedWatcher(String name, Watcher<CM> watcher) {
                return new PredicatedWatcher<>("watch on named " + name, resource1 -> ((Predicate<CM>) resource -> name.equals(resource.getMetadata().getName())).test(resource1), watcher);
            }

            static <CM extends HasMetadata> PredicatedWatcher<CM> predicatedWatcher(String desc, Predicate<CM> predicate, Watcher<CM> watcher) {
                return new PredicatedWatcher<>(desc, resource -> predicate.test(resource), watcher);
            }

            public String toString() {
                return str;
            }
        }

        protected final Collection<PredicatedWatcher<CM>> watchers = Collections.synchronizedList(new ArrayList<>(2));

        public AbstractMockBuilder(Class<CM> resourceTypeClass, Class<CML> listClass, Class<DCM> doneableClass,
                                   Class<R> resourceClass, Map<String, CM> db) {
            this.resourceTypeClass = resourceTypeClass;
            this.resourceType = resourceTypeClass.getSimpleName();
            this.doneableClass = doneableClass;
            this.resourceClass = resourceClass;
            this.db = db;
            this.listClass = listClass;
        }

        @SuppressWarnings("unchecked")
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
        @SuppressWarnings("unchecked")
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
            when(mixed.watch(any())).thenAnswer(i -> {
                Watcher watcher = i.getArgument(0);
                LOGGER.debug("Watcher {} installed on {}", watcher, mixed);
                return addWatcher(PredicatedWatcher.watcher(watcher));
            });
            when(mixed.create(any())).thenAnswer(i -> {
                CM resource = i.getArgument(0);
                String resourceName = resource.getMetadata().getName();
                return mockCreate(resourceName, resource);
            });
            when(mixed.delete(ArgumentMatchers.<CM[]>any())).thenAnswer(i -> {
                CM resource = i.getArgument(0);
                String resourceName = resource.getMetadata().getName();
                return mockDelete(resourceName);
            });
            when(mixed.withLabel(any())).thenAnswer(i -> {
                String label = i.getArgument(0);
                return mockWithLabel(label);
            });
            when(mixed.withLabel(any(), any())).thenAnswer(i -> {
                String label = i.getArgument(0);
                String value = i.getArgument(1);
                return mockWithLabels(singletonMap(label, value));
            });
            when(mixed.withLabels(any())).thenAnswer(i -> {
                Map<String, String> labels = i.getArgument(0);
                return mockWithLabels(labels);
            });
            return mixed;
        }

        @SuppressWarnings("unchecked")
        public NonNamespaceOperation<CM, CML, DCM, R> buildNonNamespaced() {
            // TODO factor out common with build(), which is more-or-less identical
            NonNamespaceOperation<CM, CML, DCM, R> mixed = mock(NonNamespaceOperation.class);

            when(mixed.list()).thenAnswer(i -> mockList(p -> true));
            when(mixed.withLabels(any())).thenAnswer(i -> {
                Map<String, String> labels = i.getArgument(0);
                return mockWithLabels(labels);
            });
            when(mixed.withName(any())).thenAnswer(invocation -> {
                String resourceName = invocation.getArgument(0);
                R resource = mock(resourceClass);
                nameScopedMocks(resource, resourceName);
                return resource;
            });
            return mixed;
        }

        MixedOperation<CM, CML, DCM, R> mockWithLabels(Map<String, String> labels) {
            return mockWithLabels(p -> {
                Map<String, String> m = new HashMap<>(p.getMetadata().getLabels());
                m.keySet().retainAll(labels.keySet());
                return labels.equals(m);
            });
        }

        MixedOperation<CM, CML, DCM, R> mockWithLabel(String label) {
            return mockWithLabels(p -> p.getMetadata().getLabels().containsKey(label));
        }

        @SuppressWarnings("unchecked")
        MixedOperation<CM, CML, DCM, R> mockWithLabels(Predicate<CM> predicate) {
            MixedOperation<CM, CML, DCM, R> mixedWithLabels = mock(MixedOperation.class);
            when(mixedWithLabels.list()).thenAnswer(i2 -> {
                return mockList(predicate);
            });
            when(mixedWithLabels.watch(any())).thenAnswer(i2 -> {
                Watcher watcher = i2.getArgument(0);
                return addWatcher(PredicatedWatcher.predicatedWatcher("watch on labeled", predicate, watcher));
            });
            return mixedWithLabels;
        }

        @SuppressWarnings("unchecked")
        private KubernetesResourceList<CM> mockList(Predicate<? super CM> predicate) {
            KubernetesResourceList<CM> l = mock(listClass);
            Collection<CM> values;
            synchronized (db) {
                values = db.values().stream().filter(predicate).map(resource -> copyResource(resource)).collect(Collectors.toList());
            }
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
                return mockDelete(resourceName);
            });
        }

        private Object mockDelete(String resourceName) {
            LOGGER.debug("delete {} {}", resourceType, resourceName);
            CM removed = db.remove(resourceName);
            if (removed != null) {
                fireWatchers(resourceName, removed, Watcher.Action.DELETED);
            }
            return removed != null;
        }

        protected void fireWatchers(String resourceName, CM removed, Watcher.Action action) {
            LOGGER.debug("Firing watchers on {}", resourceName);
            for (PredicatedWatcher<CM> watcher : watchers) {
                if (watcher.predicate.test(removed)) {
                    LOGGER.debug("Firing watcher {} with {} and resource {}", watcher, action, removed);
                    watcher.watcher.eventReceived(action, removed);
                }
            }
            LOGGER.debug("Finished firing watchers on {}", resourceName);
        }

        protected void mockPatch(String resourceName, R resource) {
            when(resource.patch(any())).thenAnswer(invocation -> {
                checkDoesExist(resourceName);
                CM argument = copyResource(invocation.getArgument(0));
                LOGGER.debug("patch {} {} -> {}", resourceType, resourceName, resource);
                db.put(resourceName, argument);
                fireWatchers(resourceName, argument, Watcher.Action.MODIFIED);
                return argument;
            });
        }

        protected void mockCascading(R resource) {
            when(resource.cascading(anyBoolean())).thenReturn(resource);
        }

        protected void mockWatch(String resourceName, R resource) {
            when(resource.watch(any())).thenAnswer(i -> {
                return mockedWatcher(resourceName, i);
            });
        }

        private Watch mockedWatcher(String resourceName, InvocationOnMock i) {
            Watcher<CM> watcher = i.getArgument(0);
            LOGGER.debug("watch {} {} ", resourceType, watcher);
            return addWatcher(PredicatedWatcher.namedWatcher(resourceName, watcher));
        }

        private Watch addWatcher(PredicatedWatcher<CM> predicatedWatcher) {
            watchers.add(predicatedWatcher);
            return () -> {
                watchers.remove(predicatedWatcher);
                LOGGER.debug("Watcher {} removed", predicatedWatcher);
            };
        }

        protected void mockCreate(String resourceName, R resource) {
            when(resource.create(any())).thenAnswer(i -> {
                CM argument = i.getArgument(0);
                return mockCreate(resourceName, argument);
            });
        }

        private CM mockCreate(String resourceName, CM argument) {
            checkNotExists(resourceName);
            LOGGER.debug("create {} {} -> {}", resourceType, resourceName, argument);
            db.put(resourceName, copyResource(argument));
            fireWatchers(resourceName, argument, Watcher.Action.ADDED);
            return copyResource(argument);
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
