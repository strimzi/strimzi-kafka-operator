/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.client.CustomResourceList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.OperationSupport;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.vertx.core.Vertx;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;

public class CrdOperator<C extends KubernetesClient,
            T extends CustomResource,
            L extends CustomResourceList<T>,
            D extends Doneable<T>>
        extends AbstractWatchableResourceOperator<C, T, L, D, Resource<T, D>> {

    private final Class<T> cls;
    private final Class<L> listCls;
    private final Class<D> doneableCls;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public CrdOperator(Vertx vertx, C client, Class<T> cls, Class<L> listCls, Class<D> doneableCls) {
        super(vertx, client, Crds.kind(cls));
        this.cls = cls;
        this.listCls = listCls;
        this.doneableCls = doneableCls;
    }

    @Override
    protected MixedOperation<T, L, D, Resource<T, D>> operation() {
        return Crds.operation(client, cls, listCls, doneableCls);
    }

    public void updateStatus(T resource) {
        // get resource plural
        try {
            String baseUrl = this.client.getMasterUrl().toString();
            OkHttpClient client = this.client.adapt(OkHttpClient.class);
            RequestBody postBody = RequestBody.create(OperationSupport.JSON, new ObjectMapper().writeValueAsString(resource));
            Request request = new Request.Builder().put(postBody).url(
                    baseUrl + "apis/kafka.strimzi.io/v1beta1/namespaces/" + resource.getMetadata().getNamespace()
                    + "/kafkas/" + resource.getMetadata().getName() + "/status").build();
            String method = request.method();
            log.debug("Making {} request {}", method, request);
            Response response = client.newCall(request).execute();
            try {
                log.debug("Got {} response {}", method, response);
                final int code = response.code();
                if (code != 200) {
                    throw new KubernetesClientException("Got unexpected " + request.method() + " status code " + code + ": " + response.message(),
                            code, OperationSupport.createStatus(response));
                }

            } finally {
                if (response.body() != null) {
                    response.close();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        CrdOperator<KubernetesClient, Kafka, KafkaList, DoneableKafka> crdOp = new CrdOperator(Vertx.vertx(), new DefaultKubernetesClient(),
                Kafka.class, KafkaList.class, DoneableKafka.class);

        Kafka myproject = crdOp.get("myproject", "my-cluster");

        crdOp.updateStatus(new KafkaBuilder(myproject).withNewStatus().withState("foo").endStatus().build());
    }
}
