/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.strimzi.controller.cluster.operations;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.DoneablePersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DeploymentList;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Patchable;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.fabric8.openshift.api.model.Build;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.BuildConfigList;
import io.fabric8.openshift.api.model.DoneableBuildConfig;
import io.fabric8.openshift.api.model.DoneableImageStream;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.api.model.ImageStreamList;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract resource creation, for a generic resource type {@code R}.
 * This class applies the template method pattern, first checking whether the resource exists,
 * and creating it if it does not. It is not an error if the resource did already exist.
 * @param <C> The type of client used to interact with kubernetes.
 * @param <T> The Kubernetes resource type.
 * @param <L> The list variant of the Kubernetes resource type.
 * @param <D> The doneable variant of the Kubernetes resource type.
 * @param <R> The resource operations.
 */
public abstract class ResourceOperation<C, T extends HasMetadata, L, D, R extends Resource<T, D>> {

    private static final Logger log = LoggerFactory.getLogger(ResourceOperation.class);
    private final Vertx vertx;
    private final C client;
    private final String resourceKind;

    public ResourceOperation(Vertx vertx, C client, String resourceKind) {
        this.vertx = vertx;
        this.client = client;
        this.resourceKind = resourceKind;
    }

    protected abstract MixedOperation<T, L, D, R> operation();

    public void create(T resource, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    if (operation().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get() == null) {
                        try {
                            log.info("Creating {} {}", resourceKind, resource);
                            operation().createOrReplace(resource);
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while creating {}", resourceKind, e);
                            future.fail(e);
                        }
                    }
                    else {
                        log.warn("{} {} already exists", resourceKind, resource);
                        future.complete();
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("{} {} has been created", resourceKind, resource);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("{} creation failed:", resourceKind, res.cause());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }

    public void delete(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {

                    if (operation().inNamespace(namespace).withName(name).get() != null) {
                        try {
                            log.info("Deleting {} {} in namespace {}", resourceKind, name, namespace);
                            operation().inNamespace(namespace).withName(name).delete();
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while deleting {}", resourceKind, e);
                            future.fail(e);
                        }
                    } else {
                        log.warn("{} {} in namespace {} doesn't exist", resourceKind, name, namespace);
                        future.complete();
                    }
                }, false,
                res -> {
                    if (res.succeeded()) {
                        log.info("{} {} in namespace {} has been deleted (or no longer exists)", resourceKind, name, namespace);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("{} deletion failed:", resourceKind, res.cause());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }

    public <P extends Patchable<T, T>> void patch(String namespace, String name, T patch, Handler<AsyncResult<Void>> handler) {
        patch(namespace, name, true, patch, handler);
    }

    public <P extends Patchable<T, T>> void patch(String namespace, String name, boolean cascading, T patch, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        log.info("Patching resource with {}", patch);
                        operation().inNamespace(namespace).withName(name).cascading(cascading).patch(patch);
                        future.complete();
                    }
                    catch (Exception e) {
                        log.error("Caught exception while patching", e);
                        future.fail(e);
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("Resource has been patched", patch);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Failed to patch resource: {}", res.cause().toString());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }


    public static ResourceOperation<KubernetesClient, Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> deployment(Vertx vertx, KubernetesClient client) {
        return new ResourceOperation<KubernetesClient, Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>>(vertx, client, "Deployment") {
            @Override
            protected MixedOperation<Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> operation() {
                return client.extensions().deployments();
            }
        };
    }

    public static ResourceOperation<KubernetesClient, ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> configMap(Vertx vertx, KubernetesClient client) {
        return new ResourceOperation<KubernetesClient, ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>>(vertx, client, "ConfigMap") {

            @Override
            protected MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> operation() {
                return client.configMaps();
            }
        };
    }

    public static ResourceOperation<KubernetesClient, StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> statefulSet(Vertx vertx, KubernetesClient client) {
        return new ResourceOperation<KubernetesClient, StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>>(vertx, client, "StatefulSet") {

            @Override
            protected MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> operation() {
                return client.apps().statefulSets();
            }

        };
    }

    public static ResourceOperation<KubernetesClient, Service, ServiceList, DoneableService, Resource<Service, DoneableService>> service(Vertx vertx, KubernetesClient client) {
        return new ResourceOperation<KubernetesClient, Service, ServiceList, DoneableService, Resource<Service, DoneableService>>(vertx, client, "Service") {

            @Override
            protected MixedOperation<Service, ServiceList, DoneableService, Resource<Service, DoneableService>> operation() {
                return client.services();
            }
        };
    }

    public static ResourceOperation<OpenShiftClient, BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>> buildConfig(Vertx vertx, OpenShiftClient client) {
        return new ResourceOperation<OpenShiftClient, BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>>(vertx, client, "BuildConfig") {
            @Override
            protected MixedOperation<BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>> operation() {
                return client.buildConfigs();
            }
        };
    }

    public static ResourceOperation<OpenShiftClient, ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> imageStream(Vertx vertx, OpenShiftClient client) {
        return new ResourceOperation<OpenShiftClient, ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>>(vertx, client, "ImageStream") {
            @Override
            protected MixedOperation<ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> operation() {
                return client.imageStreams();
            }
        };
    }

    public static ResourceOperation<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> persistentVolumeClaim(Vertx vertx, KubernetesClient client) {
        return new ResourceOperation<KubernetesClient, PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>>(vertx, client, "PersistentVolumeClaim") {
            @Override
            protected MixedOperation<PersistentVolumeClaim, PersistentVolumeClaimList, DoneablePersistentVolumeClaim, Resource<PersistentVolumeClaim, DoneablePersistentVolumeClaim>> operation() {
                return client.persistentVolumeClaims();
            }
            @Override
            public void create(PersistentVolumeClaim resource, Handler<AsyncResult<Void>> handler) {
                throw new UnsupportedOperationException();// should never happen
            }
            @Override
            public <P extends Patchable<PersistentVolumeClaim, PersistentVolumeClaim>> void patch(String namespace, String name, PersistentVolumeClaim patch, Handler<AsyncResult<Void>> handler) {
                throw new UnsupportedOperationException();// should never happen
            }
        };
    }
}
