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
import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.OpenShiftUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                        log.error("{} creation failed: {}", resourceKind, res.cause().toString());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }

    protected abstract void create(U utils, R resource);

    protected abstract boolean exists(U k8s, String namespace, String name);

    public static CreateOperation<K8SUtils, Deployment> createDeployment(Deployment dep) {
        return new CreateOperation<K8SUtils, Deployment>("Deployment", dep) {

            @Override
            protected void create(K8SUtils k8s, Deployment resource) {
                k8s.createDeployment(resource);
            }

            @Override
            protected boolean exists(K8SUtils k8s, String namespace, String name) {
                return k8s.deploymentExists(namespace, name);
            }
        };
    }

    public static CreateOperation<K8SUtils, ConfigMap> createConfigMap(ConfigMap cm) {
        return new CreateOperation<K8SUtils, ConfigMap>("ConfigMap", cm) {

            @Override
            protected void create(K8SUtils k8s, ConfigMap resource) {
                k8s.createConfigMap(resource);
            }

            @Override
            protected boolean exists(K8SUtils k8s, String namespace, String name) {
                return k8s.configMapExists(namespace, name);
            }
        };
    }

    public static CreateOperation<K8SUtils, StatefulSet> createStatefulSet(StatefulSet cm) {
        return new CreateOperation<K8SUtils, StatefulSet>("StatefulSet", cm) {

            @Override
            protected void create(K8SUtils k8s, StatefulSet resource) {
                k8s.createStatefulSet(resource);
            }

            @Override
            protected boolean exists(K8SUtils k8s, String namespace, String name) {
                return k8s.statefulSetExists(namespace, name);
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

    public static CreateOperation<OpenShiftUtils, BuildConfig> createBuildConfig(BuildConfig config) {
        return new CreateOperation<OpenShiftUtils, BuildConfig>("BuildConfig", config) {
            @Override
            protected void create(OpenShiftUtils os, BuildConfig resource) {
                os.create(resource);
            }

            @Override
            protected boolean exists(OpenShiftUtils os, String namespace, String name) {
                return os.exists(namespace, name, BuildConfig.class);
            }
        };
    }

    public static CreateOperation<OpenShiftUtils, ImageStream> createImageStream(ImageStream is) {
        return new CreateOperation<OpenShiftUtils, ImageStream>("ImageStream", is) {
            @Override
            protected void create(OpenShiftUtils os, ImageStream resource) {
                os.create(resource);
            }

            @Override
            protected boolean exists(OpenShiftUtils os, String namespace, String name) {
                return os.exists(namespace, name, ImageStream.class);
            }
        };
    }
}
