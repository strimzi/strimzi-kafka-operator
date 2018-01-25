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
import io.strimzi.controller.cluster.K8SUtils;
import io.strimzi.controller.cluster.operations.kubernetes.K8sOperation;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CreateOperation<R extends HasMetadata> extends K8sOperation {

    private static final Logger log = LoggerFactory.getLogger(CreateOperation.class);

    private final R resource;
    private final String resourceKind;



    public CreateOperation(String resourceKind, R resource) {
        this.resourceKind = resourceKind;
        this.resource = resource;
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    if (!exists(k8s, resource.getMetadata().getNamespace(), resource.getMetadata().getName())) {
                        try {
                            log.info("Creating {} {}", resourceKind, resource);
                            create(k8s, resource);
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

    protected abstract void create(K8SUtils k8s, R resource);

    protected abstract boolean exists(K8SUtils k8s, String namespace, String name);

    public static CreateOperation<Deployment> createDeployment(Deployment dep) {
        return new CreateOperation<Deployment>("Deployment", dep) {

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

    public static CreateOperation<ConfigMap> createConfigMap(ConfigMap cm) {
        return new CreateOperation<ConfigMap>("ConfigMap", cm) {

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

    public static CreateOperation<StatefulSet> createStatefulSet(StatefulSet cm) {
        return new CreateOperation<StatefulSet>("StatefulSet", cm) {

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

    public static CreateOperation<Service> createService(Service service) {
        return new CreateOperation<Service>("Service", service) {

            @Override
            protected void create(K8SUtils k8s, Service resource) {
                k8s.createService(resource);
            }

            @Override
            protected boolean exists(K8SUtils k8s, String namespace, String name) {
                return k8s.serviceExists(namespace, name);
            }
        };
    }
}
