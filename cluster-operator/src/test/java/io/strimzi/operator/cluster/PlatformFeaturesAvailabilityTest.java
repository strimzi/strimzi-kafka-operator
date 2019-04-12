/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;


import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.operator.KubernetesVersion;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class PlatformFeaturesAvailabilityTest {
    protected Vertx vertx;

    @Before
    public void before() {
        vertx = Vertx.vertx();
    }

    @After
    public void after() {
        vertx.close();
    }

    @Test
    public void networkPoliciesWithFancyCombinationTest() {
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_8);
        assertFalse(pfa.isNamespaceAndPodSelectorNetworkPolicySupported());
        pfa = new PlatformFeaturesAvailability(true, KubernetesVersion.V1_11);
        assertTrue(pfa.isNamespaceAndPodSelectorNetworkPolicySupported());
    }

    @Test
    public void testVersionDetectionOpenShift39(TestContext context)  {
        String version = "{\n" +
                "  \"major\": \"1\",\n" +
                "  \"minor\": \"9\",\n" +
                "  \"gitVersion\": \"v1.9.1+a0ce1bc657\",\n" +
                "  \"gitCommit\": \"a0ce1bc\",\n" +
                "  \"gitTreeState\": \"clean\",\n" +
                "  \"buildDate\": \"2018-06-24T01:54:00Z\",\n" +
                "  \"goVersion\": \"go1.9\",\n" +
                "  \"compiler\": \"gc\",\n" +
                "  \"platform\": \"linux/amd64\"\n" +
                "}";

        HttpServer mockHttp = startMockApi(context, version, Collections.EMPTY_LIST);

        KubernetesClient client = new DefaultKubernetesClient("127.0.0.1:" + mockHttp.actualPort());
        Future<PlatformFeaturesAvailability> futurePfa = PlatformFeaturesAvailability.create(vertx, client);

        Async async = context.async();

        futurePfa.setHandler(res -> {
            if (res.succeeded())    {
                context.assertEquals(KubernetesVersion.V1_9, res.result().getKubernetesVersion(), "Versions are not equal");
                async.complete();
            } else {
                context.fail("Failed to create PlatformFeaturesAvailability object");
                async.complete();
            }
        });

        async.awaitSuccess();
        stopMockApi(context, mockHttp);
    }

    @Test
    public void testVersionDetectionMinikube114(TestContext context)  {
        String version = "{\n" +
                "  \"major\": \"1\",\n" +
                "  \"minor\": \"14\",\n" +
                "  \"gitVersion\": \"v1.14.0\",\n" +
                "  \"gitCommit\": \"641856db18352033a0d96dbc99153fa3b27298e5\",\n" +
                "  \"gitTreeState\": \"clean\",\n" +
                "  \"buildDate\": \"2019-03-25T15:45:25Z\",\n" +
                "  \"goVersion\": \"go1.12.1\",\n" +
                "  \"compiler\": \"gc\",\n" +
                "  \"platform\": \"linux/amd64\"\n" +
                "}";

        HttpServer mockHttp = startMockApi(context, version, Collections.EMPTY_LIST);

        KubernetesClient client = new DefaultKubernetesClient("127.0.0.1:" + mockHttp.actualPort());
        Future<PlatformFeaturesAvailability> futurePfa = PlatformFeaturesAvailability.create(vertx, client);

        Async async = context.async();

        futurePfa.setHandler(res -> {
            if (res.succeeded())    {
                context.assertEquals(KubernetesVersion.V1_14, res.result().getKubernetesVersion(), "Versions are not equal");
                async.complete();
            } else {
                context.fail("Failed to create PlatformFeaturesAvailability object");
                async.complete();
            }
        });

        async.awaitSuccess();
        stopMockApi(context, mockHttp);
    }

    @Test
    public void testApiDetectionOce(TestContext context)  {
        List<String> apis = new ArrayList<>();
        apis.add("/apis/route.openshift.io/v1");
        apis.add("/apis/build.openshift.io/v1");

        HttpServer mockHttp = startMockApi(context, apis);

        KubernetesClient client = new DefaultKubernetesClient("127.0.0.1:" + mockHttp.actualPort());
        Future<PlatformFeaturesAvailability> futurePfa = PlatformFeaturesAvailability.create(vertx, client);

        Async async = context.async();

        futurePfa.setHandler(res -> {
            if (res.succeeded())    {
                PlatformFeaturesAvailability pfa = res.result();
                context.assertTrue(pfa.hasRoutes());
                context.assertTrue(pfa.hasBuilds());
                context.assertFalse(pfa.hasImages());
                context.assertFalse(pfa.hasApps());
                async.complete();
            } else {
                context.fail("Failed to create PlatformFeaturesAvailability object");
                async.complete();
            }
        });

        async.awaitSuccess();
        stopMockApi(context, mockHttp);
    }

    @Test
    public void testApiDetectionOpenshift(TestContext context)  {
        List<String> apis = new ArrayList<>();
        apis.add("/apis/route.openshift.io/v1");
        apis.add("/apis/build.openshift.io/v1");
        apis.add("/apis/apps.openshift.io/v1");
        apis.add("/apis/image.openshift.io/v1");

        HttpServer mockHttp = startMockApi(context, apis);

        KubernetesClient client = new DefaultKubernetesClient("127.0.0.1:" + mockHttp.actualPort());
        Future<PlatformFeaturesAvailability> futurePfa = PlatformFeaturesAvailability.create(vertx, client);

        Async async = context.async();

        futurePfa.setHandler(res -> {
            if (res.succeeded())    {
                PlatformFeaturesAvailability pfa = res.result();
                context.assertTrue(pfa.hasRoutes());
                context.assertTrue(pfa.hasBuilds());
                context.assertTrue(pfa.hasImages());
                context.assertTrue(pfa.hasApps());
                async.complete();
            } else {
                context.fail("Failed to create PlatformFeaturesAvailability object");
                async.complete();
            }
        });

        async.awaitSuccess();
        stopMockApi(context, mockHttp);
    }

    @Test
    public void testApiDetectionKubernetes(TestContext context)  {
        HttpServer mockHttp = startMockApi(context, Collections.EMPTY_LIST);

        KubernetesClient client = new DefaultKubernetesClient("127.0.0.1:" + mockHttp.actualPort());
        Future<PlatformFeaturesAvailability> futurePfa = PlatformFeaturesAvailability.create(vertx, client);

        Async async = context.async();

        futurePfa.setHandler(res -> {
            if (res.succeeded())    {
                PlatformFeaturesAvailability pfa = res.result();
                context.assertFalse(pfa.hasRoutes());
                context.assertFalse(pfa.hasBuilds());
                context.assertFalse(pfa.hasImages());
                context.assertFalse(pfa.hasApps());
                async.complete();
            } else {
                context.fail("Failed to create PlatformFeaturesAvailability object");
                async.complete();
            }
        });

        async.awaitSuccess();
        stopMockApi(context, mockHttp);
    }

    public HttpServer startMockApi(TestContext context, String version, List<String> apis)   {
        Async start = context.async();

        HttpServer server = vertx.createHttpServer().requestHandler(request -> {
            if (HttpMethod.GET.equals(request.method()) && apis.contains(request.uri()))   {
                request.response().setStatusCode(200).end();
            } else if (HttpMethod.GET.equals(request.method()) && "/version".equals(request.uri())) {
                request.response().setStatusCode(200).end(version);
            } else {
                request.response().setStatusCode(404).end();
            }
        }).listen(0, res -> {
            if (res.succeeded())    {
                start.complete();
            } else {
                throw new RuntimeException(res.cause());
            }
        });

        start.await();

        return server;
    }

    public HttpServer startMockApi(TestContext context, List<String> apis)    {
        String version = "{\n" +
                "  \"major\": \"1\",\n" +
                "  \"minor\": \"9\",\n" +
                "  \"gitVersion\": \"v1.9.1+a0ce1bc657\",\n" +
                "  \"gitCommit\": \"a0ce1bc\",\n" +
                "  \"gitTreeState\": \"clean\",\n" +
                "  \"buildDate\": \"2018-06-24T01:54:00Z\",\n" +
                "  \"goVersion\": \"go1.9\",\n" +
                "  \"compiler\": \"gc\",\n" +
                "  \"platform\": \"linux/amd64\"\n" +
                "}";

        return startMockApi(context, version, apis);
    }

    public void stopMockApi(TestContext context, HttpServer server) {
        Async stop = context.async();

        server.close(res -> {
            if (res.succeeded())    {
                stop.complete();
            } else {
                throw new RuntimeException("Failed to stop Mock HTTP server");
            }
        });

        stop.await();
    }
}
