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
import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.HasMetadata;
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
 * @param <U> The {@code *Utils} instance used to interact with kubernetes.
 */
public abstract class CreateOperation<U, T extends HasMetadata, L, D, R2 extends Resource<T, D>> {

    private static final Logger log = LoggerFactory.getLogger(CreateOperation.class);
    private final Vertx vertx;
    private final U client;
    private final String resourceKind;

    public CreateOperation(Vertx vertx, U client, String resourceKind) {
        this.vertx = vertx;
        this.client = client;
        this.resourceKind = resourceKind;
    }

    protected abstract MixedOperation<T, L, D, R2> operation();

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


    public static CreateOperation<KubernetesClient, Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> createDeployment(Vertx vertx, KubernetesClient client) {
        return new CreateOperation<KubernetesClient, Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>>(vertx, client, "Deployment") {
            @Override
            protected MixedOperation<Deployment, DeploymentList, DoneableDeployment, ScalableResource<Deployment, DoneableDeployment>> operation() {
                return client.extensions().deployments();
            }
        };
    }

    public static CreateOperation<KubernetesClient, ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> createConfigMap(Vertx vertx, KubernetesClient client) {
        return new CreateOperation<KubernetesClient, ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>>(vertx, client, "ConfigMap") {

            @Override
            protected MixedOperation<ConfigMap, ConfigMapList, DoneableConfigMap, Resource<ConfigMap, DoneableConfigMap>> operation() {
                return client.configMaps();
            }
        };
    }

    public static CreateOperation<KubernetesClient, StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> createStatefulSet(Vertx vertx, KubernetesClient client) {
        return new CreateOperation<KubernetesClient, StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>>(vertx, client, "StatefulSet") {

            @Override
            protected MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> operation() {
                return client.apps().statefulSets();
            }

        };
    }

    public static CreateOperation<KubernetesClient, Service, ServiceList, DoneableService, Resource<Service, DoneableService>> createService(Vertx vertx, KubernetesClient client) {
        return new CreateOperation<KubernetesClient, Service, ServiceList, DoneableService, Resource<Service, DoneableService>>(vertx, client, "Service") {

            @Override
            protected MixedOperation<Service, ServiceList, DoneableService, Resource<Service, DoneableService>> operation() {
                return client.services();
            }
        };
    }

    public static CreateOperation<OpenShiftClient, BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>> createBuildConfig(Vertx vertx, OpenShiftClient client) {
        return new CreateOperation<OpenShiftClient, BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>>(vertx, client, "BuildConfig") {
            @Override
            protected MixedOperation<BuildConfig, BuildConfigList, DoneableBuildConfig, BuildConfigResource<BuildConfig, DoneableBuildConfig, Void, Build>> operation() {
                return client.buildConfigs();
            }
        };
    }

    public static CreateOperation<OpenShiftClient, ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> createImageStream(Vertx vertx, OpenShiftClient client) {
        return new CreateOperation<OpenShiftClient, ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>>(vertx, client, "ImageStream") {
            @Override
            protected MixedOperation<ImageStream, ImageStreamList, DoneableImageStream, Resource<ImageStream, DoneableImageStream>> operation() {
                return client.imageStreams();
            }
        };
    }
}
