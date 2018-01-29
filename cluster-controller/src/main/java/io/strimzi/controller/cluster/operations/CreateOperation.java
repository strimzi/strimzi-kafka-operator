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
import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.OpenShiftUtils;
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
public abstract class CreateOperation<U, R extends HasMetadata> implements Operation<U> {

    private static final Logger log = LoggerFactory.getLogger(CreateOperation.class);

    private final R resource;
    private final String resourceKind;

    public CreateOperation(String resourceKind, R resource) {
        this.resourceKind = resourceKind;
        this.resource = resource;
    }

    @Override
    public void execute(Vertx vertx, U utils, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    if (!exists(utils, resource.getMetadata().getNamespace(), resource.getMetadata().getName())) {
                        try {
                            log.info("Creating {} {}", resourceKind, resource);
                            create(utils, resource);
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

    protected abstract void create(U utils, R resource);

    protected abstract boolean exists(U k8s, String namespace, String name);

    public static CreateOperation<KubernetesClient, Deployment> createDeployment(Deployment dep) {
        return new CreateOperation<KubernetesClient, Deployment>("Deployment", dep) {

            @Override
            protected void create(KubernetesClient client, Deployment resource) {
                log.info("Creating deployment {}", resource.getMetadata().getName());
                client.extensions().deployments().createOrReplace(resource);
            }

            @Override
            protected boolean exists(KubernetesClient client, String namespace, String name) {
                return client.extensions().deployments().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static CreateOperation<KubernetesClient, ConfigMap> createConfigMap(ConfigMap cm) {
        return new CreateOperation<KubernetesClient, ConfigMap>("ConfigMap", cm) {

            @Override
            protected void create(KubernetesClient client, ConfigMap resource) {
                log.info("Creating configmap {}", resource.getMetadata().getName());
                client.configMaps().createOrReplace(resource);
            }

            @Override
            protected boolean exists(KubernetesClient client, String namespace, String name) {
                return client.configMaps().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static CreateOperation<KubernetesClient, StatefulSet> createStatefulSet(StatefulSet cm) {
        return new CreateOperation<KubernetesClient, StatefulSet>("StatefulSet", cm) {

            @Override
            protected void create(KubernetesClient client, StatefulSet resource) {
                log.info("Creating stateful set {}", resource.getMetadata().getName());
                client.apps().statefulSets().createOrReplace(resource);
            }

            @Override
            protected boolean exists(KubernetesClient client, String namespace, String name) {
                return client.apps().statefulSets().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static CreateOperation<KubernetesClient, Service> createService(Service service) {
        return new CreateOperation<KubernetesClient, Service>("Service", service) {

            @Override
            protected void create(KubernetesClient k8s, Service resource) {
                log.info("Creating service {}", resource.getMetadata().getName());
                k8s.services().createOrReplace(resource);
            }

            @Override
            protected boolean exists(KubernetesClient client, String namespace, String name) {
                return client.services().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static CreateOperation<OpenShiftClient, BuildConfig> createBuildConfig(BuildConfig config) {
        return new CreateOperation<OpenShiftClient, BuildConfig>("BuildConfig", config) {
            @Override
            protected void create(OpenShiftClient client, BuildConfig resource) {
                client.buildConfigs().createOrReplace(resource);
            }

            @Override
            protected boolean exists(OpenShiftClient client, String namespace, String name) {
                return client.buildConfigs().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static CreateOperation<OpenShiftClient, ImageStream> createImageStream(ImageStream is) {
        return new CreateOperation<OpenShiftClient, ImageStream>("ImageStream", is) {
            @Override
            protected void create(OpenShiftClient client, ImageStream resource) {
                client.imageStreams().createOrReplace(resource);
            }

            @Override
            protected boolean exists(OpenShiftClient client, String namespace, String name) {
                return client.imageStreams().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }
}
