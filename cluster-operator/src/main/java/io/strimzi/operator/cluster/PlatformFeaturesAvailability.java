/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.APIGroup;
import io.fabric8.kubernetes.api.model.APIResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.VersionInfo;
import io.strimzi.operator.common.Util;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.platform.PlatformFeatures;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.ParseException;
import java.util.Map;

/**
 * Provides info about certain features availability and Kubernetes version in the Kubernetes cluster
 */
public class PlatformFeaturesAvailability implements PlatformFeatures {
    private static final Logger LOGGER = LogManager.getLogger(PlatformFeaturesAvailability.class.getName());

    /**
     * Path of the OIDC discovery (well-known) endpoint exposed by the Kubernetes API server for Service Account Issuer
     * Discovery
     */
    /* test */ static final String OIDC_DISCOVERY_PATH = "/.well-known/openid-configuration";

    private boolean routes = false;
    private boolean builds = false;
    private boolean images = false;
    private boolean tlsRoutes = false;
    private KubernetesVersion kubernetesVersion;
    private OidcDiscovery oidcDiscovery = null;

    private PlatformFeaturesAvailability() {}

    /**
     * This constructor is used in tests. It sets all OpenShift APIs to true or false depending on the isOpenShift parameter
     *
     * @param isOpenShift       Set all OpenShift APIs to true
     * @param kubernetesVersion Set the Kubernetes version
     */
    public PlatformFeaturesAvailability(boolean isOpenShift, KubernetesVersion kubernetesVersion) {
        this.kubernetesVersion = kubernetesVersion;
        this.routes = isOpenShift;
        this.images = isOpenShift;
        this.builds = isOpenShift;
    }

    /**
     * This constructor is used in tests. It sets all OpenShift APIs to true or false depending on the isOpenShift parameter
     *
     * @param isOpenShift       Set all OpenShift APIs to true
     * @param hasTlsRoutes      Set TLS Routes support
     * @param kubernetesVersion Set the Kubernetes version
     */
    public PlatformFeaturesAvailability(boolean isOpenShift, boolean hasTlsRoutes, KubernetesVersion kubernetesVersion) {
        this.kubernetesVersion = kubernetesVersion;
        this.tlsRoutes = hasTlsRoutes;
        this.routes = isOpenShift;
        this.images = isOpenShift;
        this.builds = isOpenShift;
    }

    /**
     * Creates a PlatformFeaturesAvailability instance
     *
     * @param vertx     Vert.x instance
     * @param client    Kubernetes client
     *
     * @return  Instance of PlatformFeaturesAvailability
     */
    public static Future<PlatformFeaturesAvailability> create(Vertx vertx, KubernetesClient client) {
        Promise<PlatformFeaturesAvailability> pfaPromise = Promise.promise();

        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability();

        Future<VersionInfo> futureVersion = getVersionInfo(vertx, client);

        futureVersion.compose(versionInfo -> {
            String major = versionInfo.getMajor().isEmpty() ? Integer.toString(KubernetesVersion.MINIMAL_SUPPORTED_MAJOR) : versionInfo.getMajor();
            String minor = versionInfo.getMinor().isEmpty() ? Integer.toString(KubernetesVersion.MINIMAL_SUPPORTED_MINOR) : versionInfo.getMinor();
            pfa.setKubernetesVersion(new KubernetesVersion(Integer.parseInt(major.split("\\D")[0]), Integer.parseInt(minor.split("\\D")[0])));

            return checkApiAvailability(vertx, client, "route.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setRoutes(supported);
            return checkApiAvailability(vertx, client, "build.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setBuilds(supported);
            return checkApiAvailability(vertx, client, "image.openshift.io", "v1");
        }).compose(supported -> {
            pfa.setImages(supported);
            return checkApiAvailability(vertx, client, "gateway.networking.k8s.io", "v1", "TLSRoute");
        }).compose(supported -> {
            pfa.setTLSRoutes(supported);
            return detectOidcDiscovery(vertx, client);
        }).compose(oidcDiscovery -> {
            pfa.setOidcDiscovery(oidcDiscovery);
            return Future.succeededFuture(pfa);
        }).onComplete(pfaPromise);

        return pfaPromise.future();
    }

    /**
     * Gets the Kubernetes VersionInfo. It either used from the /version endpoint or from the STRIMZI_KUBERNETES_VERSION
     * environment variable. If defined, the environment variable will take the precedence. Otherwise, the API server
     * endpoint will be used.
     *
     * An example of the STRIMZI_KUBERNETES_VERSION environment variable in Cluster Operator deployment:
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
            switch (entry.getKey()) {
                case "major" -> vib.withMajor(map.get(entry.getKey()));
                case "minor" -> vib.withMinor(map.get(entry.getKey()));
                case "gitVersion" -> vib.withGitVersion(map.get(entry.getKey()));
                case "gitCommit" -> vib.withGitCommit(map.get(entry.getKey()));
                case "gitTreeState" -> vib.withGitTreeState(map.get(entry.getKey()));
                case "buildDate" -> vib.withBuildDate(map.get(entry.getKey()));
                case "goVersion" -> vib.withGoVersion(map.get(entry.getKey()));
                case "compiler" -> vib.withCompiler(map.get(entry.getKey()));
                case "platform" -> vib.withPlatform(map.get(entry.getKey()));
                default -> LOGGER.warn("Unknown key {} found", entry.getKey());
            }
        }

        return vib.build();
    }

    private static Future<VersionInfo> getVersionInfoFromKubernetes(Vertx vertx, KubernetesClient client)   {
        return vertx.executeBlocking(() -> {
            try {
                return client.getKubernetesVersion();
            } catch (Exception e) {
                LOGGER.error("Detection of Kubernetes version failed.", e);
                throw e;
            }
        });
    }

    private static Future<Boolean> checkApiAvailability(Vertx vertx, KubernetesClient client, String group, String version)   {
        return vertx.executeBlocking(() -> {
            try {
                APIGroup apiGroup = client.getApiGroup(group);
                boolean supported;

                if (apiGroup != null)   {
                    supported = apiGroup.getVersions().stream().anyMatch(v -> version.equals(v.getVersion()));
                } else {
                    supported = false;
                }

                LOGGER.debug("API Group {} is {}supported", group, supported ? "" : "not ");
                return supported;
            } catch (Exception e) {
                LOGGER.error("Detection of API availability failed.", e);
                throw e;
            }
        });
    }

    /**
     * Checks whether a specific resource kind is supported or not. This check is useful for APIs where different
     * resources use different API versions, and checking the group support is not enough (such as Gateway API).
     *
     * @param vertx     Vert.x instance
     * @param client    Kubernetes client
     * @param group     API group to check
     * @param version   API version to check
     * @param kind      Resource kind to check
     *
     * @return  Future that completes with true when the resource kind is supported in a version or false when not.
     */
    private static Future<Boolean> checkApiAvailability(Vertx vertx, KubernetesClient client, String group, String version, String kind)   {
        return vertx.executeBlocking(() -> {
            try {
                APIResourceList apiGroupResources = client.getApiResources(group + "/" + version);
                boolean supported;

                if (apiGroupResources != null)   {
                    supported = apiGroupResources.getResources().stream().anyMatch(r -> kind.equals(r.getKind()));
                } else {
                    supported = false;
                }

                LOGGER.debug("Kind {} in API Group {} is {}supported", kind, group, supported ? "" : "not ");
                return supported;
            } catch (Exception e) {
                LOGGER.error("Detection of API availability failed.", e);
                throw e;
            }
        });
    }

    /**
     * Queries the OIDC discovery (well-known) endpoint of the Kubernetes API server and extracts the issuer and JWKS
     * endpoint URLs from it. The OIDC discovery endpoint is optional -> it might be disabled or not accessible to the
     * operator. So a failure to get the OIDC discovery information does not fail the whole platform feature detection.
     * Instead, it just logs a warning and completes with null.
     *
     * @param vertx     Vert.x instance
     * @param client    Kubernetes client
     *
     * @return  Future that completes with the OIDC discovery information or with null if it is not available
     */
    private static Future<OidcDiscovery> detectOidcDiscovery(Vertx vertx, KubernetesClient client)   {
        return vertx.executeBlocking(() -> {
            try {
                OidcDiscovery oidcDiscovery = parseOidcDiscovery(client.raw(OIDC_DISCOVERY_PATH));

                if (oidcDiscovery != null) {
                    LOGGER.debug("Kubernetes OIDC discovery endpoint found with issuer {} and JWKS URI {}", oidcDiscovery.issuer(), oidcDiscovery.jwksUri());
                } else {
                    LOGGER.warn("Kubernetes OIDC discovery endpoint is not available");
                }

                return oidcDiscovery;
            } catch (Exception e) {
                LOGGER.warn("Detection of Kubernetes OIDC discovery endpoint failed.", e);
                return null;
            }
        });
    }

    /**
     * Parses the OIDC discovery document and extracts the issuer and JWKS URI from it.
     *
     * @param discoveryDocument     The OIDC discovery document (JSON)
     *
     * @return  OIDC discovery information or null if the discovery document is null or does not contain both the
     *          issuer and the JWKS URI
     *
     * @throws Exception    If the discovery document cannot be parsed
     */
    /* test */ static OidcDiscovery parseOidcDiscovery(String discoveryDocument) throws Exception {
        if (discoveryDocument == null) {
            // Endpoint returned 404
            return null;
        }

        JsonNode json = new ObjectMapper().readTree(discoveryDocument);
        String issuer = json.path("issuer").asText(null);
        String jwksUri = json.path("jwks_uri").asText(null);

        if (issuer == null || issuer.isBlank() || jwksUri == null || jwksUri.isBlank()) {
            LOGGER.warn("Kubernetes OIDC discovery document is missing the issuer or the JWKS URI: {}", discoveryDocument);
            return null;
        }

        return new OidcDiscovery(issuer, jwksUri);
    }

    @Override
    public boolean isOpenshift() {
        return this.hasRoutes();
    }

    @Override
    public KubernetesVersion getKubernetesVersion() {
        return this.kubernetesVersion;
    }

    private void setKubernetesVersion(KubernetesVersion kubernetesVersion) {
        this.kubernetesVersion = kubernetesVersion;
    }

    /**
     * Checks if OpenShift Routes are supported on this cluster.
     *
     * @return  True if OpenShift Routes are supported on this cluster
     */
    public boolean hasRoutes() {
        return routes;
    }

    private void setRoutes(boolean routes) {
        this.routes = routes;
    }

    /**
     * Checks if OpenShift Builds are supported on this cluster.
     *
     * @return  True if OpenShift Builds are supported on this cluster
     */
    public boolean hasBuilds() {
        return builds;
    }

    private void setBuilds(boolean builds) {
        this.builds = builds;
    }

    /**
     * Checks if OpenShift ImageStreams are supported on this cluster.
     *
     * @return  True if OpenShift ImageStreams are supported on this cluster
     */
    public boolean hasImages() {
        return images;
    }

    private void setImages(boolean images) {
        this.images = images;
    }

    /**
     * Checks if OpenShift S2I (Builds and Images) are supported on this cluster.
     *
     * @return  True if OpenShift S2I (Builds and Images) are supported on this cluster
     */
    public boolean supportsS2I() {
        return hasBuilds() && hasImages();
    }

    /**
     * Checks if Gateway API v1 TLSRoutes are supported on this cluster.
     *
     * @return  True if Gateway API v1 TLSRoutes are supported on this cluster
     */
    public boolean hasTLSRoutes() {
        return tlsRoutes;
    }

    private void setTLSRoutes(boolean tlsRoutes) {
        this.tlsRoutes = tlsRoutes;
    }

    /**
     * Gets the information detected from the Kubernetes OIDC discovery endpoint.
     *
     * @return  The OIDC discovery information or null if it was not detected
     */
    public OidcDiscovery getOidcDiscovery() {
        return oidcDiscovery;
    }

    private void setOidcDiscovery(OidcDiscovery oidcDiscovery) {
        this.oidcDiscovery = oidcDiscovery;
    }

    @Override
    public String toString() {
        return "PlatformFeaturesAvailability(" +
                "KubernetesVersion=" + kubernetesVersion +
                ",OpenShiftRoutes=" + routes +
                ",OpenShiftBuilds=" + builds +
                ",OpenShiftImageStreams=" + images +
                ",TLSRoutes=" + tlsRoutes +
                ",OidcDiscovery=" + oidcDiscovery +
                ")";
    }

    /**
     * Holds the information obtained from the OIDC discovery endpoint
     *
     * @param issuer    Issuer URL
     * @param jwksUri   JWKS endpoint URL
     */
    public record OidcDiscovery(String issuer, String jwksUri) { }
}
