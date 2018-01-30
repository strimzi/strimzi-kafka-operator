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
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.client.OpenShiftClient;
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
 * @param <R> The type of resource created
 */
public abstract class CreateOperation<U, R extends HasMetadata> {

    private static final Logger log = LoggerFactory.getLogger(CreateOperation.class);
    private final Vertx vertx;
    private final U client;
    private final String resourceKind;

    public CreateOperation(Vertx vertx, U client, String resourceKind) {
        this.vertx = vertx;
        this.client = client;
        this.resourceKind = resourceKind;
    }

    public void create(R resource, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    if (!exists(resource.getMetadata().getNamespace(), resource.getMetadata().getName())) {
                        try {
                            log.info("Creating {} {}", resourceKind, resource);
                            create(resource);
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

    protected abstract void create(R resource);

    protected abstract boolean exists(String namespace, String name);

    public static CreateOperation<KubernetesClient, Deployment> createDeployment(Vertx vertx, KubernetesClient client) {
        return new CreateOperation<KubernetesClient, Deployment>(vertx, client, "Deployment") {

            @Override
            protected void create(Deployment resource) {
                log.info("Creating deployment {}", resource.getMetadata().getName());
                client.extensions().deployments().createOrReplace(resource);
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.extensions().deployments().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static CreateOperation<KubernetesClient, ConfigMap> createConfigMap(Vertx vertx, KubernetesClient client) {
        return new CreateOperation<KubernetesClient, ConfigMap>(vertx, client, "ConfigMap") {

            @Override
            protected void create(ConfigMap resource) {
                log.info("Creating configmap {}", resource.getMetadata().getName());
                client.configMaps().createOrReplace(resource);
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.configMaps().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static CreateOperation<KubernetesClient, StatefulSet> createStatefulSet(Vertx vertx, KubernetesClient client) {
        return new CreateOperation<KubernetesClient, StatefulSet>(vertx, client, "StatefulSet") {

            @Override
            protected void create(StatefulSet resource) {
                log.info("Creating stateful set {}", resource.getMetadata().getName());
                client.apps().statefulSets().createOrReplace(resource);
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.apps().statefulSets().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static CreateOperation<KubernetesClient, Service> createService(Vertx vertx, KubernetesClient client) {
        return new CreateOperation<KubernetesClient, Service>(vertx, client, "Service") {

            @Override
            protected void create(Service resource) {
                log.info("Creating service {}", resource.getMetadata().getName());
                client.services().createOrReplace(resource);
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.services().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static CreateOperation<OpenShiftClient, BuildConfig> createBuildConfig(Vertx vertx, OpenShiftClient client) {
        return new CreateOperation<OpenShiftClient, BuildConfig>(vertx, client, "BuildConfig") {
            @Override
            protected void create(BuildConfig resource) {
                client.buildConfigs().createOrReplace(resource);
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.buildConfigs().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static CreateOperation<OpenShiftClient, ImageStream> createImageStream(Vertx vertx, OpenShiftClient client) {
        return new CreateOperation<OpenShiftClient, ImageStream>(vertx, client, "ImageStream") {
            @Override
            protected void create(ImageStream resource) {
                client.imageStreams().createOrReplace(resource);
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.imageStreams().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }
}
