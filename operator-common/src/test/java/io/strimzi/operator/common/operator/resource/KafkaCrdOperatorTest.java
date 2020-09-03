/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.dsl.base.OperationSupport;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaList;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxTestContext;
import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaCrdOperatorTest extends AbstractResourceOperatorTest<KubernetesClient, Kafka, KafkaList, DoneableKafka, Resource<Kafka, DoneableKafka>> {

    @Override
    protected Class<KubernetesClient> clientType() {
        return KubernetesClient.class;
    }

    @Override
    protected Class<Resource> resourceType() {
        return Resource.class;
    }

    @Override
    protected Kafka resource() {
        return new KafkaBuilder()
                .withApiVersion(Kafka.RESOURCE_GROUP + "/" + Kafka.V1BETA1)
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(1)
                        .withNewListeners()
                            .withNewPlain()
                            .endPlain()
                        .endListeners()
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(1)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .withNewStatus()
                .endStatus()
                .build();
    }

    @Override
    protected void mocker(KubernetesClient mockClient, MixedOperation op) {
        when(mockClient.customResources(any(CustomResourceDefinitionContext.class), any(), any(), any())).thenReturn(op);
    }

    @Override
    protected CrdOperator createResourceOperations(Vertx vertx, KubernetesClient mockClient) {
        return new CrdOperator(vertx, mockClient, Kafka.class, KafkaList.class, DoneableKafka.class, Crds.kafka());
    }

    @Test
    public void testUpdateStatusAsync(VertxTestContext context) throws IOException {
        KubernetesClient mockClient = mock(KubernetesClient.class);

        OkHttpClient mockOkHttp = mock(OkHttpClient.class);
        when(mockClient.adapt(eq(OkHttpClient.class))).thenReturn(mockOkHttp);
        URL fakeUrl = new URL("http", "my-host", 9443, "/");
        when(mockClient.getMasterUrl()).thenReturn(fakeUrl);
        Call mockCall = mock(Call.class);
        when(mockOkHttp.newCall(any(Request.class))).thenReturn(mockCall);
        ResponseBody body = ResponseBody.create(OperationSupport.JSON, "{ }");
        Response response = new Response.Builder().code(200).request(new Request.Builder().url(fakeUrl).build()).body(body).message("Created").protocol(Protocol.HTTP_1_1).build();
        when(mockCall.execute()).thenReturn(response);

        Checkpoint async = context.checkpoint();

        createResourceOperations(vertx, mockClient)
            .updateStatusAsync(resource())
            .onComplete(context.succeeding(kafka -> async.flag()));
    }

    @Test
    public void testUpdateStatusWorksAfterUpgradeWithHttp422ResponseAboutApiVersionField(VertxTestContext context) throws IOException {
        KubernetesClient mockClient = mock(KubernetesClient.class);

        OkHttpClient mockOkHttp = mock(OkHttpClient.class);
        when(mockClient.adapt(eq(OkHttpClient.class))).thenReturn(mockOkHttp);
        URL fakeUrl = new URL("http", "my-host", 9443, "/");
        when(mockClient.getMasterUrl()).thenReturn(fakeUrl);
        Call mockCall = mock(Call.class);
        when(mockOkHttp.newCall(any(Request.class))).thenReturn(mockCall);
        ResponseBody body = ResponseBody.create(OperationSupport.JSON, "{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"Kafka." + Constants.RESOURCE_GROUP_NAME + " \\\"my-cluster\\\" is invalid: apiVersion: Invalid value: \\\"" + Constants.RESOURCE_GROUP_NAME + "/" + Constants.V1ALPHA1 + "\\\": must be " + Constants.RESOURCE_GROUP_NAME + "/" + Constants.V1BETA1 + "\",\"reason\":\"Invalid\",\"details\":{\"name\":\"my-cluster\",\"group\":\"" + Constants.RESOURCE_GROUP_NAME + "\",\"kind\":\"Kafka\",\"causes\":[{\"reason\":\"FieldValueInvalid\",\"message\":\"Invalid value: \\\"" + Constants.RESOURCE_GROUP_NAME + "/" + Constants.V1ALPHA1 + "\\\": must be " + Constants.RESOURCE_GROUP_NAME + "/" + Constants.V1BETA1 + "\",\"field\":\"apiVersion\"}]},\"code\":422}");
        Response response = new Response.Builder().code(422).request(new Request.Builder().url(fakeUrl).build()).body(body).message("Unprocessable Entity").protocol(Protocol.HTTP_1_1).build();
        when(mockCall.execute()).thenReturn(response);

        Checkpoint async = context.checkpoint();
        createResourceOperations(vertx, mockClient)
            .updateStatusAsync(resource())
            .onComplete(context.succeeding(kafka -> async.flag()));

    }

    @Test
    public void testUpdateStatusThrowsWhenHttp422ResponseWithOtherField(VertxTestContext context) throws IOException {
        KubernetesClient mockClient = mock(KubernetesClient.class);

        OkHttpClient mockOkHttp = mock(OkHttpClient.class);
        when(mockClient.adapt(eq(OkHttpClient.class))).thenReturn(mockOkHttp);
        URL fakeUrl = new URL("http", "my-host", 9443, "/");
        when(mockClient.getMasterUrl()).thenReturn(fakeUrl);
        Call mockCall = mock(Call.class);
        when(mockOkHttp.newCall(any(Request.class))).thenReturn(mockCall);
        ResponseBody body = ResponseBody.create(OperationSupport.JSON, "{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"metadata\":{},\"status\":\"Failure\",\"message\":\"Kafka." + Constants.RESOURCE_GROUP_NAME + " \\\"my-cluster\\\" is invalid: apiVersion: Invalid value: \\\"" + Constants.RESOURCE_GROUP_NAME + "/" + Constants.V1ALPHA1 + "\\\": must be " + Constants.RESOURCE_GROUP_NAME + "/" + Constants.V1BETA1 + "\",\"reason\":\"Invalid\",\"details\":{\"name\":\"my-cluster\",\"group\":\"" + Constants.RESOURCE_GROUP_NAME + "\",\"kind\":\"Kafka\",\"causes\":[{\"reason\":\"FieldValueInvalid\",\"message\":\"Invalid value: \\\"" + Constants.RESOURCE_GROUP_NAME + "/" + Constants.V1ALPHA1 + "\\\": must be " + Constants.RESOURCE_GROUP_NAME + "/" + Constants.V1BETA1 + "\",\"field\":\"someOtherField\"}]},\"code\":422}");
        Response response = new Response.Builder().code(422).request(new Request.Builder().url(fakeUrl).build()).body(body).message("Unprocessable Entity").protocol(Protocol.HTTP_1_1).build();
        when(mockCall.execute()).thenReturn(response);

        Checkpoint async = context.checkpoint();
        createResourceOperations(vertx, mockClient)
            .updateStatusAsync(resource())
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(KubernetesClientException.class));
                async.flag();
            })));

    }

    @Test
    public void testUpdateStatusThrowsWhenHttp422ResponseWithNoBody(VertxTestContext context) throws IOException {
        KubernetesClient mockClient = mock(KubernetesClient.class);

        OkHttpClient mockOkHttp = mock(OkHttpClient.class);
        when(mockClient.adapt(eq(OkHttpClient.class))).thenReturn(mockOkHttp);
        URL fakeUrl = new URL("http", "my-host", 9443, "/");
        when(mockClient.getMasterUrl()).thenReturn(fakeUrl);
        Call mockCall = mock(Call.class);
        when(mockOkHttp.newCall(any(Request.class))).thenReturn(mockCall);
        ResponseBody body = ResponseBody.create(OperationSupport.JSON, "{ }");
        Response response = new Response.Builder().code(422).request(new Request.Builder().url(fakeUrl).build()).message("Unprocessable Entity").protocol(Protocol.HTTP_1_1).build();
        when(mockCall.execute()).thenReturn(response);

        Checkpoint async = context.checkpoint();
        createResourceOperations(vertx, mockClient)
            .updateStatusAsync(resource())
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(KubernetesClientException.class));
                async.flag();
            })));
    }

    @Test
    public void testUpdateStatusThrowsWhenHttp409Response(VertxTestContext context) throws IOException {
        KubernetesClient mockClient = mock(KubernetesClient.class);

        OkHttpClient mockOkHttp = mock(OkHttpClient.class);
        when(mockClient.adapt(eq(OkHttpClient.class))).thenReturn(mockOkHttp);
        URL fakeUrl = new URL("http", "my-host", 9443, "/");
        when(mockClient.getMasterUrl()).thenReturn(fakeUrl);
        Call mockCall = mock(Call.class);
        when(mockOkHttp.newCall(any(Request.class))).thenReturn(mockCall);
        ResponseBody body = ResponseBody.create(OperationSupport.JSON, "{ }");
        Response response = new Response.Builder().code(409).request(new Request.Builder().url(fakeUrl).build()).body(body).message("Conflict").protocol(Protocol.HTTP_1_1).build();
        when(mockCall.execute()).thenReturn(response);

        Checkpoint async = context.checkpoint();
        createResourceOperations(vertx, mockClient)
            .updateStatusAsync(resource())
            .onComplete(context.failing(e -> context.verify(() -> {
                assertThat(e, instanceOf(KubernetesClientException.class));
                async.flag();
            })));
    }
}
