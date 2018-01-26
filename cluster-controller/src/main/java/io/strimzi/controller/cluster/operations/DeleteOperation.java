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
public abstract class DeleteOperation<U> implements Operation<U> {

    private static final Logger log = LoggerFactory.getLogger(DeleteOperation.class);

    private final String resourceKind;
    private final String namespace;
    private final String name;

    public DeleteOperation(String resourceKind, String namespace, String name) {
        this.resourceKind = resourceKind;
        this.namespace = namespace;
        this.name = name;
    }

    protected abstract boolean exists(U utils, String namespace, String name);

    protected abstract void delete(U utils, String namespace, String name);

    @Override
    public void execute(Vertx vertx, U utils, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {

                    if (exists(utils, namespace, name)) {
                        try {
                            log.info("Deleting {} {} in namespace {}", resourceKind, name, namespace);
                            delete(utils, namespace, name);
                            future.complete();
                        } catch (Exception e) {
                            log.error("Caught exception while deleting {}", resourceKind, e);
                            future.fail(e);
                        }
                    } else {
                        log.warn("{} {} in namespace {} doesn't exists", resourceKind, name, namespace);
                        future.complete();
                    }
                }, false,
                res -> {
                    if (res.succeeded()) {
                        log.info("{} {} in namespace {} has been deleted", resourceKind, name, namespace);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("{} deletion failed:", resourceKind, res.cause());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }

    public static DeleteOperation<K8SUtils> deleteDeployment(String namespace, String name) {
        return new DeleteOperation<K8SUtils>("Deployment", namespace, name) {

            @Override
            protected void delete(K8SUtils k8s, String namespace, String name) {
                k8s.deleteDeployment(namespace, name);
            }

            @Override
            protected boolean exists(K8SUtils k8s, String namespace, String name) {
                return k8s.deploymentExists(namespace, name);
            }
        };
    }

    public static DeleteOperation<K8SUtils> deleteConfigMap(String namespace, String name) {
        return new DeleteOperation<K8SUtils>("ConfigMap", namespace, name) {

            @Override
            protected void delete(K8SUtils k8s, String namespace, String name) {
                k8s.deleteConfigMap(namespace, name);
            }

            @Override
            protected boolean exists(K8SUtils k8s, String namespace, String name) {
                return k8s.configMapExists(namespace, name);
            }
        };
    }

    public static DeleteOperation<K8SUtils> deleteStatefulSet(String namespace, String name) {
        return new DeleteOperation<K8SUtils>("StatefulSet", namespace, name) {

            @Override
            protected void delete(K8SUtils k8s, String namespace, String name) {
                k8s.deleteStatefulSet(namespace, name);
            }

            @Override
            protected boolean exists(K8SUtils k8s, String namespace, String name) {
                return k8s.statefulSetExists(namespace, name);
            }
        };
    }

    public static DeleteOperation<KubernetesClient> deleteService(String namespace, String name) {
        return new DeleteOperation<KubernetesClient>("Service", namespace, name) {

            @Override
            protected void delete(KubernetesClient k8s, String namespace, String name) {
                log.info("Deleting service {}", name);
                k8s.services().inNamespace(namespace).withName(name).delete();
            }

            @Override
            protected boolean exists(KubernetesClient client, String namespace, String name) {
                return client.services().inNamespace(namespace).withName(name).get() != null;
            }
        };
    }

    public static DeleteOperation<OpenShiftUtils> deleteBuildConfig(String namespace, String name) {
        return new DeleteOperation<OpenShiftUtils>("BuildConfig", namespace, name) {
            @Override
            protected void delete(OpenShiftUtils os, String namespace, String name) {
                os.delete(namespace, name, BuildConfig.class);
            }

            @Override
            protected boolean exists(OpenShiftUtils os, String namespace, String name) {
                return os.exists(namespace, name, BuildConfig.class);
            }
        };
    }

    public static DeleteOperation<OpenShiftUtils> deleteImageStream(String namespace, String name) {
        return new DeleteOperation<OpenShiftUtils>("ImageStream", namespace, name) {
            @Override
            protected void delete(OpenShiftUtils os, String namespace, String name) {
                os.delete(namespace, name, ImageStream.class);
            }

            @Override
            protected boolean exists(OpenShiftUtils os, String namespace, String name) {
                return os.exists(namespace, name, ImageStream.class);
            }
        };
    }
}
