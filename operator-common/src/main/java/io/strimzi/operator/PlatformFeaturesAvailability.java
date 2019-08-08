/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Gives a info about certain features availability regarding to kubernetes version
 */
public class PlatformFeaturesAvailability {
    private static final Logger log = LogManager.getLogger(PlatformFeaturesAvailability.class.getName());

    private boolean routes = false;
    private boolean builds = false;
    private boolean images = false;
    private boolean apps = false;
    private KubernetesVersion kubernetesVersion;

    public static Future<PlatformFeaturesAvailability> create(Vertx vertx, KubernetesClient client) {
        Future<PlatformFeaturesAvailability> pfaFuture = Future.future();
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
            pfaFuture.complete(pfa);
        }, pfaFuture);

        return pfaFuture;
    }

    private static OkHttpClient getOkHttpClient(KubernetesClient client)   {
        if (client.isAdaptable(OkHttpClient.class)) {
            return client.adapt(OkHttpClient.class);
        } else {
            log.error("Cannot adapt KubernetesClient to OkHttpClient");
            throw new RuntimeException("Cannot adapt KubernetesClient to OkHttpClient");
        }
    }

    private static Future<VersionInfo> getVersionInfo(Vertx vertx, KubernetesClient client)   {
        Future<VersionInfo> fut = Future.future();

        vertx.executeBlocking(request -> {
            try {
                request.complete(client.getVersion());
            } catch (Exception e) {
                log.error("Detection of Kuberetes version failed.", e);
                request.fail(e);
            }
        }, fut);

        return fut;
    }

    private static Future<Boolean> checkApiAvailability(Vertx vertx, OkHttpClient httpClient, String masterUrl, String api, String version)   {
        Future<Boolean> fut = Future.future();

        vertx.executeBlocking(request -> {
            try {
                Boolean isSupported;

                Response resp = httpClient.newCall(new Request.Builder().get().url(masterUrl + "apis/" + api + "/" + version).build()).execute();
                if (resp.code() >= 200 && resp.code() < 300) {
                    log.debug("{} returned {}. This API is supported.", resp.request().url(), resp.code());
                    isSupported = true;
                } else {
                    log.debug("{} returned {}. This API is not supported.", resp.request().url(), resp.code());
                    isSupported = false;
                }

                resp.close();
                request.complete(isSupported);
            } catch (Exception e) {
                log.error("Detection of {}/{} API failed. This API will be disabled.", api, version, e);
                request.complete(false);
            }
        }, fut);

        return fut;
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

    public boolean isNamespaceAndPodSelectorNetworkPolicySupported() {
        if (isOpenshift()) {
            return this.kubernetesVersion.compareTo(KubernetesVersion.V1_12) >= 0;
        } else {
            return this.kubernetesVersion.compareTo(KubernetesVersion.V1_11) >= 0;
        }
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

    @Override
    public String toString() {
        return "ClusterOperatorConfig(" +
                "KubernetesVersion=" + kubernetesVersion +
                ",OpenShiftRoutes=" + routes +
                ",OpenShiftBuilds=" + builds +
                ",OpenShiftImageStreams=" + images +
                ",OpenShiftDeploymentConfigs=" + apps +
                ")";
    }
}
