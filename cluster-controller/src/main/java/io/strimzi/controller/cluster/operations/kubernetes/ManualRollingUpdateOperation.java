package io.strimzi.controller.cluster.operations.kubernetes;

import io.strimzi.controller.cluster.K8SUtils;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManualRollingUpdateOperation extends K8sOperation {
    private static final Logger log = LoggerFactory.getLogger(ManualRollingUpdateOperation.class.getName());
    private final String namespace;
    private final String name;
    private final int replicas;

    public ManualRollingUpdateOperation(String namespace, String name, int replicas) {
        this.namespace = namespace;
        this.name = name;
        this.replicas = replicas;
    }

    @Override
    public void execute(Vertx vertx, K8SUtils k8s, Handler<AsyncResult<Void>> handler) {
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
                future -> {
                    try {
                        log.info("Doing rolling update of stateful set {} in namespace {}", name, namespace);

                        for (int i = 0; i < replicas; i++) {
                            String podName = name + "-" + i;
                            log.info("Rolling pod {}", podName);
                            Future deleted = Future.future();
                            Watcher<Pod> watcher = new RollingUpdateWatcher<Pod>(deleted);

                            Watch watch = k8s.createPodWatch(namespace, podName, watcher);
                            k8s.deletePod(namespace, podName);

                            while (!deleted.isComplete()) {
                                log.info("Waiting for pod {} to be deleted", podName);
                                Thread.sleep(1000);
                            }

                            watch.close();

                            while (!k8s.isPodReady(namespace, podName)) {
                                log.info("Waiting for pod {} to get ready", podName);
                                Thread.sleep(1000);
                            };

                            log.info("Pod {} rolling update complete", podName);
                        }

                        future.complete();
                    }
                    catch (Exception e) {
                        log.error("Caught exception while doing manual rolling update of stateful set {} in namespace {}", name, namespace);
                        future.fail(e);
                    }
                },
                false,
                res -> {
                    if (res.succeeded()) {
                        log.info("Stateful set {} in namespace {} has been rolled", name, namespace);
                        handler.handle(Future.succeededFuture());
                    }
                    else {
                        log.error("Failed to do rolling update of stateful set {} in namespace {}: {}", name, namespace, res.cause().toString());
                        handler.handle(Future.failedFuture(res.cause()));
                    }
                }
        );
    }

    class RollingUpdateWatcher<T> implements Watcher<T> {
        //private static final Logger log = LoggerFactory.getLogger(RollingUpdateWatcher.class.getName());
        private final Future deleted;

        public RollingUpdateWatcher(Future deleted) {
            this.deleted = deleted;
        }

        @Override
        public void eventReceived(Action action, T pod) {
            switch (action) {
                case DELETED:
                    log.info("Pod has been deleted");
                    deleted.complete();
                    break;
                case ADDED:
                case MODIFIED:
                    log.info("Ignored action {} while waiting for Pod deletion", action);
                    break;
                case ERROR:
                    log.error("Error while waiting for Pod deletion");
                    break;
                default:
                    log.error("Unknown action {} while waiting for pod deletion", action);
            }
        }

        @Override
        public void onClose(KubernetesClientException e) {
            if (e != null && !deleted.isComplete()) {
                log.error("Kubernetes watcher has been closed with exception!", e);
                deleted.fail(e);
            }
            else {
                log.info("Kubernetes watcher has been closed!");
            }
        }
    }
}


