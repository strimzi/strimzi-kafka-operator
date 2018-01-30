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
 * Abstract resource deletion.
 * This class applies the template method pattern, first checking whether the resource exists,
 * and creating it if it does not. It is not an error if the resource did already exist.
 * @param <U> The {@code *Utils} instance used to interact with kubernetes.
 */
public abstract class DeleteOperation<U> {

    private static final Logger log = LoggerFactory.getLogger(DeleteOperation.class);

    private final String resourceKind;
    private final Vertx vertx;
    private final U client;

    public DeleteOperation(Vertx vertx, U client, String resourceKind) {
        this.vertx = vertx;
        this.client = client;
        this.resourceKind = resourceKind;
    }

    protected abstract boolean exists(String namespace, String name);

    protected abstract void delete(String namespace, String name);

    public void delete(String namespace, String name, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {

                    if (exists(namespace, name)) {
                        try {
                            log.info("Deleting {} {} in namespace {}", resourceKind, name, namespace);
                            delete(namespace, name);
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

    public static DeleteOperation<KubernetesClient> deleteDeployment(Vertx vertx, KubernetesClient client) {
        return new DeleteOperation<KubernetesClient>(vertx, client, "Deployment") {

            @Override
            protected void delete(String namespace, String name) {
                log.debug("Deleting deployment {}", name);
                client.extensions().deployments().inNamespace(namespace).withName(name).delete();
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.extensions().deployments().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static DeleteOperation<KubernetesClient> deleteConfigMap(Vertx vertx, KubernetesClient client) {
        return new DeleteOperation<KubernetesClient>(vertx, client, "ConfigMap") {

            @Override
            protected void delete(String namespace, String name) {
                log.debug("Deleting configmap {}", name);
                client.configMaps().inNamespace(namespace).withName(name).delete();
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.configMaps().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static DeleteOperation<KubernetesClient> deleteStatefulSet(Vertx vertx, KubernetesClient client) {
        return new DeleteOperation<KubernetesClient>(vertx, client, "StatefulSet") {

            @Override
            protected void delete(String namespace, String name) {
                log.debug("Deleting stateful set {}", name);
                client.apps().statefulSets().inNamespace(namespace).withName(name).delete();
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.apps().statefulSets().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static DeleteOperation<KubernetesClient> deleteService(Vertx vertx, KubernetesClient client) {
        return new DeleteOperation<KubernetesClient>(vertx, client, "Service") {

            @Override
            protected void delete(String namespace, String name) {
                log.info("Deleting service {}", name);
                client.services().inNamespace(namespace).withName(name).delete();
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.services().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static DeleteOperation<OpenShiftClient> deleteBuildConfig(Vertx vertx, OpenShiftClient client) {
        return new DeleteOperation<OpenShiftClient>(vertx, client, "BuildConfig") {
            @Override
            protected void delete(String namespace, String name) {
                client.buildConfigs().inNamespace(namespace).withName(name).delete();
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.buildConfigs().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static DeleteOperation<OpenShiftClient> deleteImageStream(Vertx vertx, OpenShiftClient client) {
        return new DeleteOperation<OpenShiftClient>(vertx, client, "ImageStream") {
            @Override
            protected void delete(String namespace, String name) {
                client.imageStreams().inNamespace(namespace).withName(name).delete();
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.imageStreams().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static DeleteOperation<KubernetesClient> deletePersistentVolumeClaim(Vertx vertx, KubernetesClient client) {
        return new DeleteOperation<KubernetesClient>(vertx, client, "PersistentVolumeClaim") {
            @Override
            protected void delete(String namespace, String name) {
                log.debug("Deleting persistentvolumeclaim {}", name);
                client.persistentVolumeClaims().inNamespace(namespace).withName(name).delete();
            }

            @Override
            protected boolean exists(String namespace, String name) {
                return client.persistentVolumeClaims().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }
}
