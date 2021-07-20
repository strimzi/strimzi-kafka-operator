/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.strimzi.operator.common.Util;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.util.Map;

/**
 * Gives a info about certain features availability regarding to kubernetes version
 */
public class PlatformFeaturesAvailability {
    private static final Logger LOGGER = LogManager.getLogger(PlatformFeaturesAvailability.class.getName());

    private boolean routes = false;
    private boolean builds = false;
    private boolean images = false;
    private boolean apps = false;
    private KubernetesVersion kubernetesVersion;
    private EventApiVersion eventApiVersion;

    /**
     * Identifies a supported event api version
     * events.k8s.io/v1
     * event.k8s.io/v1beta1
     * core events
     */
    public enum EventApiVersion {
        V1,
        V1BETA1,
        CORE
    }

    public static Future<PlatformFeaturesAvailability> create(Vertx vertx, KubernetesClient client) {
        Promise<PlatformFeaturesAvailability> pfaPromise = Promise.promise();
        OkHttpClient httpClient = getOkHttpClient(client);

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability();

        Future<VersionInfo> futureVersion = getVersionInfo(vertx, client);

        futureVersion.compose(versionInfo -> {
            String major = versionInfo.getMajor().equals("") ? Integer.toString(KubernetesVersion.MINIMAL_SUPPORTED_MAJOR) : versionInfo.getMajor();
            String minor = versionInfo.getMinor().equals("") ? Integer.toString(KubernetesVersion.MINIMAL_SUPPORTED_MINOR) : versionInfo.getMinor();
            pfa.setKubernetesVersion(new KubernetesVersion(Integer.parseInt(major.split("\\D")[0]), Integer.parseInt(minor.split("\\D")[0])));

            return checkApiAvailability(vertx, httpClient, client.getMasterUrl().toString(), "route.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setRoutes(supported);
            return checkApiAvailability(vertx, httpClient, client.getMasterUrl().toString(), "build.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setBuilds(supported);
            return checkApiAvailability(vertx, httpClient, client.getMasterUrl().toString(), "apps.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setApps(supported);
            return checkApiAvailability(vertx, httpClient, client.getMasterUrl().toString(), "image.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setImages(supported);
            return highestEventVersion(vertx, httpClient, client.getMasterUrl().toString());
        }).compose(eventApiVersion -> {
            pfa.setHighestEventApiVersion(eventApiVersion);
            return Future.succeededFuture(pfa);
        })
        .onComplete(pfaPromise);

        return pfaPromise.future();
    }

    /**
     * Brief rundown - the various APIs and K8s versions
     *   events.k8s.io/v1      - introduced in Kubernetes 1.19
     *   events.k8s.io/v1beta1 - introduced in Kubernetes 1.8, deprecated in 1.19, will be removed in 1.19
     *   core events           - Always around, but has significant performance issues, so a new event API was introduced.
     *
     * Preference is for v1 events.k8s.io if it exists, then v1beta1, then falling back to Core v1.Event API if necessary.
     * It's preferable to use the new API to
     *     a) avoid any performance hits and
     *     b) conform with the best practices of the K8s community
     *
     * @see <a href="https://github.com/kubernetes/community/blob/master/contributors/design-proposals/instrumentation/events-redesign.md">Redesign</a>
     * @see <a href="https://github.com/kubernetes/enhancements/tree/master/keps/sig-instrumentation/383-new-event-api-ga-graduation">KEP-383</a>
     *
     * Explicitly test for existence of APIs instead of assuming existence on basis of K8s version, as cluster managers can
     * disable APIs, and K8s 1.18 introduced a "disable all beta APIs with one handy command line argument" feature.
     * @param vertx - Vertx
     * @param httpClient - used to hit APIs
     * @param masterUrl - Cluster's root API url
     * @return highest Event API version detected
     */
    private static Future<EventApiVersion> highestEventVersion(Vertx vertx, OkHttpClient httpClient, String masterUrl) {

        return checkApiAvailability(vertx, httpClient, masterUrl, "events.k8s.io", "v1").compose(v1Supported -> {
                    if (v1Supported) {
                        return Future.succeededFuture(EventApiVersion.V1);
                    }
                    else {
                        return checkApiAvailability(vertx, httpClient, masterUrl, "events.k8s.io", "v1beta1").compose(v1Beta1Supported -> {
                            if (v1Beta1Supported) {
                                return Future.succeededFuture(EventApiVersion.V1BETA1);
                            }
                            else {
                                return Future.succeededFuture(EventApiVersion.CORE);
                            }
                        });
                    }
                });
    }

    private static OkHttpClient getOkHttpClient(KubernetesClient client)   {
        if (client.isAdaptable(OkHttpClient.class)) {
            return client.adapt(OkHttpClient.class);
        } else {
            LOGGER.error("Cannot adapt KubernetesClient to OkHttpClient");
            throw new RuntimeException("Cannot adapt KubernetesClient to OkHttpClient");
        }
    }

    /**
     * Gets the Kubernetes VersionInfo. It either used from the /version endpoint or from the STRIMZI_KUBERNETES_VERSION
     * environment variable. If defined, the environment variable will take the precedence. Otherwise the API server
     * endpoint will be used.
     *
     * And example of the STRIMZI_KUBERNETES_VERSION environment variable in Cluster Operator deployment:
     * <pre><code>
     *       env:
     *         - name: STRIMZI_KUBERNETES_VERSION
     *           value: |
     *                 major=1
     *                 minor=16
     *                 gitVersion=v1.16.2
     *                 gitCommit=c97fe5036ef3df2967d086711e6c0c405941e14b
     *                 gitTreeState=clean
     *                 buildDate=2019-10-15T19:09:08Z
     *                 goVersion=go1.12.10
     *                 compiler=gc
     *                 platform=linux/amd64
     * </code></pre>
     *
     * @param vertx Instance of Vert.x
     * @param client    Fabric8 Kubernetes client
     * @return  Future with the VersionInfo object describing the Kubernetes version
     */
    private static Future<VersionInfo> getVersionInfo(Vertx vertx, KubernetesClient client) {
        Future<VersionInfo> futureVersion;

        String kubernetesVersion = System.getenv("STRIMZI_KUBERNETES_VERSION");

        if (kubernetesVersion != null) {
            try {
                futureVersion = Future.succeededFuture(parseVersionInfo(kubernetesVersion));
            } catch (ParseException e) {
                throw new RuntimeException("Failed to parse the Kubernetes version information provided through STRIMZI_KUBERNETES_VERSION environment variable", e);
            }
        } else {
            futureVersion = getVersionInfoFromKubernetes(vertx, client);
        }

        return futureVersion;
    }

    static VersionInfo parseVersionInfo(String str) throws ParseException {
        Map<String, String> map = Util.parseMap(str);
        VersionInfo.Builder vib = new VersionInfo.Builder();
        for (Map.Entry<String, String> entry: map.entrySet()) {
            if (entry.getKey().equals("major")) {
                vib.withMajor(map.get(entry.getKey()));
            } else if (entry.getKey().equals("minor")) {
                vib.withMinor(map.get(entry.getKey()));
            } else if (entry.getKey().equals("gitVersion")) {
                vib.withGitVersion(map.get(entry.getKey()));
            } else if (entry.getKey().equals("gitCommit")) {
                vib.withGitCommit(map.get(entry.getKey()));
            } else if (entry.getKey().equals("gitTreeState")) {
                vib.withGitTreeState(map.get(entry.getKey()));
            } else if (entry.getKey().equals("buildDate")) {
                vib.withBuildDate(map.get(entry.getKey()));
            } else if (entry.getKey().equals("goVersion")) {
                vib.withGoVersion(map.get(entry.getKey()));
            } else if (entry.getKey().equals("compiler")) {
                vib.withCompiler(map.get(entry.getKey()));
            } else if (entry.getKey().equals("platform")) {
                vib.withPlatform(map.get(entry.getKey()));
            }
        }
        return vib.build();
    }

    private static Future<VersionInfo> getVersionInfoFromKubernetes(Vertx vertx, KubernetesClient client)   {
        Promise<VersionInfo> promise = Promise.promise();

        vertx.executeBlocking(request -> {
            try {
                request.complete(client.getVersion());
            } catch (Exception e) {
                LOGGER.error("Detection of Kubernetes version failed.", e);
                request.fail(e);
            }
        }, promise);

        return promise.future();
    }

    private static Future<Boolean> checkApiAvailability(Vertx vertx, OkHttpClient httpClient, String masterUrl, String api, String version)   {
        Promise<Boolean> promise = Promise.promise();

        vertx.executeBlocking(request -> {
            try {
                Boolean isSupported;

                Response resp = httpClient.newCall(new Request.Builder().get().url(masterUrl + "apis/" + api + "/" + version).build()).execute();
                if (resp.code() >= 200 && resp.code() < 300) {
                    LOGGER.debug("{} returned {}. This API is supported.", resp.request().url(), resp.code());
                    isSupported = true;
                } else {
                    LOGGER.debug("{} returned {}. This API is not supported.", resp.request().url(), resp.code());
                    isSupported = false;
                }

                resp.close();
                request.complete(isSupported);
            } catch (Exception e) {
                LOGGER.error("Detection of {}/{} API failed. This API will be disabled.", api, version, e);
                request.complete(false);
            }
        }, promise);

        return promise.future();
    }

    private PlatformFeaturesAvailability() {}

    /**
     * This constructor is used in tests. It sets all OpenShift APIs to true or false depending on the isOpenShift paremeter
     *
     * @param isOpenShift           Set all OpenShift APIs to true
     * @param kubernetesVersion     Set the Kubernetes version
     */
    /* test */public PlatformFeaturesAvailability(boolean isOpenShift, KubernetesVersion kubernetesVersion) {
        this.kubernetesVersion = kubernetesVersion;
        this.routes = isOpenShift;
        this.images = isOpenShift;
        this.builds = isOpenShift;
        this.apps = isOpenShift;
    }

    public boolean isOpenshift() {
        return this.hasRoutes();
    }

    public KubernetesVersion getKubernetesVersion() {
        return this.kubernetesVersion;
    }

    private void setKubernetesVersion(KubernetesVersion kubernetesVersion) {
        this.kubernetesVersion = kubernetesVersion;
    }

    public boolean hasRoutes() {
        return routes;
    }

    private void setRoutes(boolean routes) {
        this.routes = routes;
    }

    public boolean hasBuilds() {
        return builds;
    }

    private void setBuilds(boolean builds) {
        this.builds = builds;
    }

    public boolean hasImages() {
        return images;
    }

    private void setImages(boolean images) {
        this.images = images;
    }

    public boolean hasApps() {
        return apps;
    }

    private void setApps(boolean apps) {
        this.apps = apps;
    }

    public boolean supportsS2I() {
        return hasBuilds() && hasApps() && hasImages();
    }


    private void setHighestEventApiVersion(EventApiVersion eventApiVersion) {
        this.eventApiVersion = eventApiVersion;
    }

    public EventApiVersion getHighestEventApiVersion() {
        return eventApiVersion;
    }

    /**
     * Returns true when the Kubernetes cluster has V1 version of the Ingress resource (Kubernetes 1.19 and newer)
     *
     * @return True when Ingress V1 is supported. False otherwise.
     */
    public boolean hasIngressV1() {
        return this.kubernetesVersion.compareTo(KubernetesVersion.V1_19) >= 0;
    }

    @Override
    public String toString() {
        return "PlatformFeaturesAvailability(" +
                "KubernetesVersion=" + kubernetesVersion +
                ",OpenShiftRoutes=" + routes +
                ",OpenShiftBuilds=" + builds +
                ",OpenShiftImageStreams=" + images +
                ",OpenShiftDeploymentConfigs=" + apps +
                ")";
    }
}
