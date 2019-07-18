/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.OperationSupport;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaUser;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class CrdOperator<C extends KubernetesClient,
            T extends CustomResource,
            L extends CustomResourceList<T>,
            D extends Doneable<T>>
        extends AbstractWatchableResourceOperator<C, T, L, D, Resource<T, D>> {

    private final Class<T> cls;
    private final Class<L> listCls;
    private final Class<D> doneableCls;
    protected final String plural;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     * @param cls The class of the CR
     * @param listCls The class of the list.
     * @param doneableCls The class of the CR's "doneable".
     */
    public CrdOperator(Vertx vertx, C client, Class<T> cls, Class<L> listCls, Class<D> doneableCls) {
        super(vertx, client, Crds.kind(cls));
        this.cls = cls;
        this.listCls = listCls;
        this.doneableCls = doneableCls;

        if (cls.equals(Kafka.class)) {
            this.plural = Kafka.RESOURCE_PLURAL;
        } else if (cls.equals(KafkaUser.class)) {
            this.plural = KafkaUser.RESOURCE_PLURAL;
        } else if (cls.equals(KafkaConnect.class)) {
            this.plural = KafkaConnect.RESOURCE_PLURAL;
        } else if (cls.equals(KafkaConnectS2I.class)) {
            this.plural = KafkaConnectS2I.RESOURCE_PLURAL;
        } else {
            this.plural = null;
        }
    }

    @Override
    protected MixedOperation<T, L, D, Resource<T, D>> operation() {
        return Crds.operation(client, cls, listCls, doneableCls);
    }

    public Future<Void> updateStatusAsync(T resource) {
        Future<Void> blockingFuture = Future.future();

        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(future -> {
            try {

                OkHttpClient client = this.client.adapt(OkHttpClient.class);
                RequestBody postBody = RequestBody.create(OperationSupport.JSON, new ObjectMapper().writeValueAsString(resource));

                Request request = new Request.Builder().put(postBody).url(
                        this.client.getMasterUrl().toString() + "apis/" + resource.getApiVersion() + "/namespaces/" + resource.getMetadata().getNamespace()
                                + "/" + this.plural + "/" + resource.getMetadata().getName() + "/status").build();

                String method = request.method();
                Response response = client.newCall(request).execute();

                try {
                    final int code = response.code();

                    if (code == 422)    {
                        Status status = OperationSupport.createStatus(response);

                        if (status != null
                                && status.getDetails() != null
                                && status.getDetails().getCauses() != null
                                && status.getDetails().getCauses().size() > 0
                                && status.getDetails().getCauses().stream().filter(cause -> "FieldValueInvalid".equals(cause.getReason()) && "apiVersion".equals(cause.getField())).findAny().orElse(null) != null)  {
                            log.debug("Got semi-expected {} status code {}: {}", method, code, status);
                            log.warn("Cannot update status of resource {} named {}. The resource needs to be updated to newer apiVersion first.", resource.getKind(), resource.getMetadata().getName());
                        } else {
                            log.debug("Got unexpected {} status code {}: {}", method, code, status);
                            throw OperationSupport.requestFailure(request, status);
                        }
                    } else if (code != 200) {
                        Status status = OperationSupport.createStatus(response);
                        log.debug("Got unexpected {} status code {}: {}", method, code, status);
                        throw OperationSupport.requestFailure(request, status);
                    }
                } catch (Exception e) {
                    throw e;
                } finally {
                    // Only messages with body should be closed
                    if (response.body() != null) {
                        response.close();
                    }
                }

                future.complete();
            } catch (Exception e) {
                log.debug("Updating status failed", e);
                future.fail(e);
            }
        }, true, blockingFuture);

        return blockingFuture;
    }
}
