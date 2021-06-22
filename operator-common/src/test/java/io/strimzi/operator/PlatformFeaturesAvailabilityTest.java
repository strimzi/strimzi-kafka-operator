/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator;

import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.text.ParseException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


@ExtendWith(VertxExtension.class)
public class PlatformFeaturesAvailabilityTest {

    private Supplier<List<String>> apis;
    private Supplier<String> version;

    private HttpServer mockHttp;

    private final String defaultVersion =   "{\n" +
                                            "  \"major\": \"1\",\n" +
                                            "  \"minor\": \"16\",\n" +
                                            "  \"gitVersion\": \"v1.9.1+a0ce1bc657\",\n" +
                                            "  \"gitCommit\": \"a0ce1bc\",\n" +
                                            "  \"gitTreeState\": \"clean\",\n" +
                                            "  \"buildDate\": \"2018-06-24T01:54:00Z\",\n" +
                                            "  \"goVersion\": \"go1.9\",\n" +
                                            "  \"compiler\": \"gc\",\n" +
                                            "  \"platform\": \"linux/amd64\"\n" +
                                            "}";
    private Vertx vertx;
    private KubernetesClient kubernetesClient;

    @BeforeEach
    void setup(Vertx vertx) throws ExecutionException, InterruptedException {
        this.vertx = vertx;

        //Default to no apis.
        apis = List::of;
        version = () -> defaultVersion;

        mockHttp = waitForMockApiServer(vertx);
        kubernetesClient = new DefaultKubernetesClient("127.0.0.1:" + mockHttp.actualPort());
    }

    @AfterEach
    void cleanup() throws ExecutionException, InterruptedException {
        mockHttp.close().toCompletionStage().toCompletableFuture().get();
    }


    @Test
    @Disabled
    void testFalsePositiveCanary(VertxTestContext context) {
        verify(context, pfa -> assertThat("Answer to the Ultimate Question of Life, the Universe, and Everything", is(42)));
    }

    @Test
    public void testVersionDetectionOpenShift(VertxTestContext context) {
        version = () ->  "{\n" +
                        "  \"major\": \"1\",\n" +
                        "  \"minor\": \"20\",\n" +
                        "  \"gitVersion\": \"v1.20.1\",\n" +
                        "  \"gitCommit\": \"c4d752765b3bbac2237bf87cf0b1c2e307844666\",\n" +
                        "  \"gitTreeState\": \"clean\",\n" +
                        "  \"buildDate\": \"2020-12-18T12:00:47Z\",\n" +
                        "  \"goVersion\": \"go1.15.5\",\n" +
                        "  \"compiler\": \"gc\",\n" +
                        "  \"platform\": \"linux/amd64\"\n" +
                        "}";

        verify(context, pfa -> assertThat("Versions are not equal", pfa.getKubernetesVersion(), is(KubernetesVersion.V1_20)));
    }

    @Test
    public void testVersionDetectionMinikube(VertxTestContext context) {
        version = () -> "{\n" +
                        "  \"major\": \"1\",\n" +
                        "  \"minor\": \"20\",\n" +
                        "  \"gitVersion\": \"v1.20.1\",\n" +
                        "  \"gitCommit\": \"c4d752765b3bbac2237bf87cf0b1c2e307844666\",\n" +
                        "  \"gitTreeState\": \"clean\",\n" +
                        "  \"buildDate\": \"2020-12-18T12:09:25Z\",\n" +
                        "  \"goVersion\": \"go1.15.5\",\n" +
                        "  \"compiler\": \"gc\",\n" +
                        "  \"platform\": \"linux/amd64\"\n" +
                        "}";

        verify(context, pfa ->  assertThat("Versions are not equal", pfa.getKubernetesVersion(), is(KubernetesVersion.V1_20)));

    }

    @Test
    public void testApiDetectionOce(VertxTestContext context) {

        apis = () -> List.of(
                "/apis/route.openshift.io/v1",
                "/apis/build.openshift.io/v1"
        );

        verify(context, pfa -> {
            assertThat(pfa.hasRoutes(), is(true));
            assertThat(pfa.hasBuilds(), is(true));
            assertThat(pfa.hasImages(), is(false));
            assertThat(pfa.hasApps(), is(false));
        });
    }

    @Test
    public void testApiDetectionOpenshift(VertxTestContext context) {

        apis = () -> List.of(
            "/apis/route.openshift.io/v1",
            "/apis/build.openshift.io/v1",
            "/apis/apps.openshift.io/v1",
            "/apis/image.openshift.io/v1"
        );

        verify(context, pfa -> {
            assertThat(pfa.hasRoutes(), is(true));
            assertThat(pfa.hasBuilds(), is(true));
            assertThat(pfa.hasImages(), is(true));
            assertThat(pfa.hasApps(), is(true));
        });
    }

    @Test
    public void testApiDetectionKubernetes(VertxTestContext context) {

        apis = List::of;

        verify(context, pfa -> {
            assertThat(pfa.hasRoutes(), is(false));
            assertThat(pfa.hasBuilds(), is(false));
            assertThat(pfa.hasImages(), is(false));
            assertThat(pfa.hasApps(), is(false));
        });
    }

    @Test
    public void versionInfoFromMap(VertxTestContext context) throws ParseException {
        String version =  "major=1\n" +
                "minor=16\n" +
                "gitVersion=v1.16.2\n" +
                "gitCommit=c97fe5036ef3df2967d086711e6c0c405941e14b\n" +
                "gitTreeState=clean\n" +
                "buildDate=2019-10-15T19:09:08Z\n" +
                "goVersion=go1.12.10\n" +
                "compiler=gc\n" +
                "platform=linux/amd64";

        VersionInfo vi = PlatformFeaturesAvailability.parseVersionInfo(version);

        context.verify(() -> {
            assertThat(vi.getMajor(), is("1"));
            assertThat(vi.getMinor(), is("16"));
        });
        context.completeNow();
    }

    private void verify(VertxTestContext context, Consumer<PlatformFeaturesAvailability> assertions) {
        Handler<PlatformFeaturesAvailability> runAssertions = pfa -> context.verify(() -> assertions.accept(pfa));

        PlatformFeaturesAvailability.create(vertx, kubernetesClient)
                                    .onComplete(context.succeeding(runAssertions))
                                    .onComplete(unused -> context.completeNow());
    }

    private HttpServer waitForMockApiServer(Vertx vertx) throws ExecutionException, InterruptedException {
        Future<HttpServer> serverFuture = vertx.createHttpServer().requestHandler(request -> {

            if (HttpMethod.GET.equals(request.method()) && apis.get().contains(request.uri())) {
                request.response().setStatusCode(200).end();
            } else if (HttpMethod.GET.equals(request.method()) && "/version".equals(request.uri())) {
                request.response().setStatusCode(200).end(version.get());
            } else {
                request.response().setStatusCode(404).end();
            }

        }).listen(0);

        return serverFuture.toCompletionStage().toCompletableFuture().get();
    }
}
