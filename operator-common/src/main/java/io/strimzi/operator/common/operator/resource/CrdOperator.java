/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.OperationSupport;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.Kafka;
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
    protected final String baseUrl;
    protected final String plural;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public CrdOperator(Vertx vertx, C client, Class<T> cls, Class<L> listCls, Class<D> doneableCls) {
        super(vertx, client, Crds.kind(cls));
        this.baseUrl = this.client.getMasterUrl().toString();
        this.cls = cls;
        this.listCls = listCls;
        this.doneableCls = doneableCls;

        if (cls.equals(Kafka.class)) {
            this.plural = Kafka.RESOURCE_PLURAL;
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
                        this.baseUrl + "apis/" + resource.getApiVersion() + "/namespaces/" + resource.getMetadata().getNamespace()
                                + "/" + this.plural + "/" + resource.getMetadata().getName() + "/status").build();

                String method = request.method();
                log.debug("Making {} request {}", method, request);
                Response response = client.newCall(request).execute();

                try {
                    log.debug("Got {} response {}", method, response);
                    final int code = response.code();

                    if (code == 200) {
                        future.complete();
                    } else {
                        log.debug("Got unexpected {} status code {}: {}", method, code, response.message());
                        future.fail(new KubernetesClientException("Got unexpected " + method + " status code " + code + ": " + response.message(),
                                code, OperationSupport.createStatus(response)));
                    }
                } finally {
                    if (response.body() != null) {
                        response.close();
                    }
                }
            } catch (Exception e) {
                log.debug("Updating status failed", e);
                future.fail(e);
            }
        }, res -> {
                if (res.succeeded()) {
                    blockingFuture.complete();
                } else {
                    blockingFuture.fail(res.cause());
                }
            });

        return blockingFuture;
    }
}
