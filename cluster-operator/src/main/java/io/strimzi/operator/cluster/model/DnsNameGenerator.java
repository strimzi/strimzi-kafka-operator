/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

/**
 * DnsNameGenerator generates DNS names for services and pods
 *
 * Kubernetes DNS documentation: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/
 */
public class DnsNameGenerator {

    private final String namespace;
    private final String serviceName;

    // cluster.local is the default DNS domain for Kubernetes, if modified a user must provide the custom domain
    // via the KUBERNETES_SERVICE_DNS_DOMAIN environment variable
    private static final String KUBERNETES_SERVICE_DNS_DOMAIN =
            System.getenv().getOrDefault("KUBERNETES_SERVICE_DNS_DOMAIN", "cluster.local");

    private DnsNameGenerator(String namespace, String serviceName) {
        this.namespace = namespace;
        this.serviceName = serviceName;
    }

    /**
     * Creates the DnsNameGenerator instance from namespace and service name
     *
     * @param namespace     Namespace of the service
     * @param serviceName   Name of the service
     *
     * @return  DnsNameGenerator instance
     */
    public static DnsNameGenerator of(String namespace, String serviceName) {
        if (namespace == null || namespace.isEmpty() || serviceName == null || serviceName.isEmpty()) {
            throw new IllegalArgumentException();
        }
        return new DnsNameGenerator(namespace, serviceName);
    }

    /**
     * Generates the DNS name of the pod including the cluster suffix
     * (i.e. usually with the cluster.local - but can be different on different clusters)
     * Example: my-pod-1.my-service.my-ns.svc.cluster.local
     *
     * Note: Conventionally this would only be used for pods with deterministic names such as StrimziPodSet pods
     *
     * @param podName       Name of the pod
     *
     * @return              DNS name of the pod
     */
    public String podDnsName(String podName) {
        return String.format("%s.%s",
                podName,
                serviceDnsName());
    }

    /**
     * Generates a DNS name of a Pod
     *
     * @param namespace     Namespace of the Pod
     * @param serviceName   Name of the headless service
     * @param podName       Name of the Pod
     *
     * @return  Pod DNS name
     */
    public static String podDnsName(String namespace, String serviceName, String podName) {
        return DnsNameGenerator.of(namespace, serviceName)
                .podDnsName(podName);
    }

    /**
     * Generates the DNS name of the pod without the cluster domain suffix
     * (i.e. usually without the cluster.local - but can be different on different clusters)
     * Example: my-cluster-pod-1.my-cluster-service.my-ns.svc
     *
     * Note: Conventionally this would only be used for pods with deterministic names such as StrimziPodSet pods
     *
     * @param podName       Name of the pod
     *
     * @return              DNS name of the pod without the cluster domain suffix
     */
    public String podDnsNameWithoutClusterDomain(String podName) {
        return String.format("%s.%s",
                podName,
                serviceDnsNameWithoutClusterDomain());

    }

    /**
     * Generates the pod DNS name without the cluster domain suffix
     *
     * @param namespace     Namespace of the service
     * @param serviceName   Name of the service
     * @param podName       Name of the pod
     * @return              Pod DNS name without the suffix
     */
    public static String podDnsNameWithoutClusterDomain(String namespace, String serviceName, String podName) {
        return DnsNameGenerator.of(namespace, serviceName)
                .podDnsNameWithoutClusterDomain(podName);
    }

    /**
     * Generates the pod DNS name with the cluster domain suffix
     *
     * @param namespace     Namespace of the service
     * @param serviceName   Name of the service
     * @param podName       Name of the pod
     * @return              Pod DNS name including the suffix
     */
    public static String podDnsNameWithClusterDomain(String namespace, String serviceName, String podName) {
        return DnsNameGenerator.of(namespace, serviceName)
                .podDnsName(podName);
    }

    /**
     * Generates the DNS name of the service including the cluster suffix
     * (i.e. usually with the cluster.local - but can be different on different clusters)
     * Example: my-service.my-ns.svc.cluster.local
     *
     * @return              DNS name of the service
     */
    public String serviceDnsName() {
        return String.format("%s.%s.svc.%s",
                serviceName,
                namespace,
                KUBERNETES_SERVICE_DNS_DOMAIN);
    }

    /**
     * Generates the wildcard DNS name of the service without the cluster domain suffix
     * (i.e. usually without the cluster.local - but can be different on different clusters)
     * Example: *.my-service.my-ns.svc
     *
     * @return              Wildcard DNS name of the service without the cluster domain suffix
     */
    public String wildcardServiceDnsNameWithoutClusterDomain() {
        return String.format("*.%s.%s.svc",
                serviceName,
                namespace);
    }

    /**
     * Generates the wildcard DNS name of the service including the cluster suffix
     * (i.e. usually with the cluster.local - but can be different on different clusters)
     * Example: *.my-service.my-ns.svc.cluster.local
     *
     * @return              Wildcard DNS name of the service
     */
    public String wildcardServiceDnsName() {
        return String.format("*.%s.%s.svc.%s",
                serviceName,
                namespace,
                KUBERNETES_SERVICE_DNS_DOMAIN);
    }

    /**
     * Generates the DNS name of the service without the cluster domain suffix
     * (i.e. usually without the cluster.local - but can be different on different clusters)
     * Example: my-service.my-ns.svc
     *
     * @return              DNS name of the service without the cluster domain suffix
     */
    public String serviceDnsNameWithoutClusterDomain() {
        return String.format("%s.%s.svc",
                serviceName,
                namespace);
    }

    /**
     * Generates the service name without the cluster domain suffix
     *
     * @param namespace     Namespace of the service
     * @param serviceName   Name of the service
     * @return              Service DNS name without the suffix
     */
    public static String serviceDnsNameWithoutClusterDomain(String namespace, String serviceName) {
        return DnsNameGenerator.of(namespace, serviceName)
                .serviceDnsNameWithoutClusterDomain();
    }

    /**
     * Generates the service name with the cluster domain suffix
     *
     * @param namespace     Namespace of the service
     * @param serviceName   Name of the service
     * @return              Fully qualified DNS name including the suffix
     */
    public static String serviceDnsNameWithClusterDomain(String namespace, String serviceName) {
        return DnsNameGenerator.of(namespace, serviceName)
                .serviceDnsName();
    }
}
