/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;

public class ClusterCa extends Ca {

    private final String clusterName;
    private Secret entityTopicOperatorSecret;
    private Secret entityUserOperatorSecret;
    private Secret clusterOperatorSecret;
    private Secret kafkaExporterSecret;
    private Secret cruiseControlSecret;

    private Secret brokersSecret;
    private Secret zkNodesSecret;

    private final Pattern ipv4Address = Pattern.compile("[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}");

    public ClusterCa(Reconciliation reconciliation, CertManager certManager, PasswordGenerator passwordGenerator, String clusterName, Secret caCertSecret, Secret caKeySecret) {
        this(reconciliation, certManager, passwordGenerator, clusterName, caCertSecret, caKeySecret, 365, 30, true, null);
    }

    public ClusterCa(Reconciliation reconciliation, CertManager certManager,
                     PasswordGenerator passwordGenerator,
                     String clusterName,
                     Secret clusterCaCert,
                     Secret clusterCaKey,
                     int validityDays,
                     int renewalDays,
                     boolean generateCa,
                     CertificateExpirationPolicy policy) {
        super(reconciliation, certManager, passwordGenerator,
                "cluster-ca",
                AbstractModel.clusterCaCertSecretName(clusterName),
                forceRenewal(clusterCaCert, clusterCaKey, "cluster-ca.key"),
                AbstractModel.clusterCaKeySecretName(clusterName),
                adapt060ClusterCaSecret(clusterCaKey), validityDays, renewalDays, generateCa, policy);
        this.clusterName = clusterName;
    }

    /**
     * In Strimzi 0.6.0 the Secrets and keys used a different convention.
     * Here we adapt the keys in the {@code *-cluster-ca} Secret to match what
     * 0.7.0 expects.
     * @param clusterCaKey The cluster CA key Secret
     * @return The same Secret.
     */
    public static Secret adapt060ClusterCaSecret(Secret clusterCaKey) {
        if (clusterCaKey != null && clusterCaKey.getData() != null) {
            String key = clusterCaKey.getData().get("cluster-ca.key");
            if (key != null) {
                clusterCaKey.getData().put("ca.key", key);
            }
        }
        return clusterCaKey;
    }


    @Override
    public String toString() {
        return "cluster-ca";
    }

    public void initCaSecrets(List<Secret> secrets) {
        for (Secret secret: secrets) {
            String name = secret.getMetadata().getName();
            if (KafkaCluster.brokersSecretName(clusterName).equals(name)) {
                brokersSecret = secret;
            } else if (EntityTopicOperator.secretName(clusterName).equals(name)) {
                entityTopicOperatorSecret = secret;
            } else if (EntityUserOperator.secretName(clusterName).equals(name)) {
                entityUserOperatorSecret = secret;
            } else if (ZookeeperCluster.nodesSecretName(clusterName).equals(name)) {
                zkNodesSecret = secret;
            } else if (ClusterOperator.secretName(clusterName).equals(name)) {
                clusterOperatorSecret = secret;
            } else if (KafkaExporter.secretName(clusterName).equals(name)) {
                kafkaExporterSecret = secret;
            } else if (CruiseControl.secretName(clusterName).equals(name)) {
                cruiseControlSecret = secret;
            }
        }
    }

    public Secret entityTopicOperatorSecret() {
        return entityTopicOperatorSecret;
    }

    public Secret entityUserOperatorSecret() {
        return entityUserOperatorSecret;
    }

    public Secret clusterOperatorSecret() {
        return clusterOperatorSecret;
    }

    public Secret kafkaExporterSecret() {
        return kafkaExporterSecret;
    }

    public Secret cruiseControlSecret() {
        return cruiseControlSecret;
    }

    public Map<String, CertAndKey> generateCcCerts(Kafka kafka, boolean isMaintenanceTimeWindowsSatisfied) throws IOException {
        String cluster = kafka.getMetadata().getName();
        String namespace = kafka.getMetadata().getNamespace();

        DnsNameGenerator ccDnsGenerator = DnsNameGenerator.of(namespace, CruiseControl.serviceName(cluster));

        Function<Integer, Subject> subjectFn = i -> {
            Subject.Builder subject = new Subject.Builder()
                    .withOrganizationName("io.strimzi")
                    .withCommonName(CruiseControl.serviceName(cluster));

            subject.addDnsName(CruiseControl.serviceName(cluster));
            subject.addDnsName(String.format("%s.%s",  CruiseControl.serviceName(cluster), namespace));
            subject.addDnsName(ccDnsGenerator.serviceDnsNameWithoutClusterDomain());
            subject.addDnsName(ccDnsGenerator.serviceDnsName());
            subject.addDnsName(CruiseControl.serviceName(cluster));
            subject.addDnsName("localhost");
            return subject.build();
        };

        LOGGER.debugCr(reconciliation, "{}: Reconciling Cruise Control certificates", this);
        return maybeCopyOrGenerateCerts(
            reconciliation,
           1,
            subjectFn,
            cruiseControlSecret,
            podNum -> "cruise-control",
            isMaintenanceTimeWindowsSatisfied);
    }

    public Map<String, CertAndKey> generateZkCerts(Kafka kafka, boolean isMaintenanceTimeWindowsSatisfied) throws IOException {
        String cluster = kafka.getMetadata().getName();
        String namespace = kafka.getMetadata().getNamespace();

        DnsNameGenerator zkDnsGenerator = DnsNameGenerator.of(namespace, ZookeeperCluster.serviceName(cluster));
        DnsNameGenerator zkHeadlessDnsGenerator = DnsNameGenerator.of(namespace, ZookeeperCluster.headlessServiceName(cluster));

        Function<Integer, Subject> subjectFn = i -> {
            Subject.Builder subject = new Subject.Builder()
                    .withOrganizationName("io.strimzi")
                    .withCommonName(ZookeeperCluster.zookeeperClusterName(cluster));
            subject.addDnsName(ZookeeperCluster.serviceName(cluster));
            subject.addDnsName(String.format("%s.%s", ZookeeperCluster.serviceName(cluster), namespace));
            subject.addDnsName(zkDnsGenerator.serviceDnsNameWithoutClusterDomain());
            subject.addDnsName(zkDnsGenerator.serviceDnsName());
            subject.addDnsName(ZookeeperCluster.podDnsName(namespace, cluster, i));
            subject.addDnsName(ZookeeperCluster.podDnsNameWithoutSuffix(namespace, cluster, i));
            subject.addDnsName(zkDnsGenerator.wildcardServiceDnsNameWithoutClusterDomain());
            subject.addDnsName(zkDnsGenerator.wildcardServiceDnsName());
            subject.addDnsName(zkHeadlessDnsGenerator.wildcardServiceDnsNameWithoutClusterDomain());
            subject.addDnsName(zkHeadlessDnsGenerator.wildcardServiceDnsName());
            return subject.build();
        };

        LOGGER.debugCr(reconciliation, "{}: Reconciling zookeeper certificates", this);
        return maybeCopyOrGenerateCerts(
            reconciliation,
            kafka.getSpec().getZookeeper().getReplicas(),
            subjectFn,
            zkNodesSecret,
            podNum -> ZookeeperCluster.zookeeperPodName(cluster, podNum),
            isMaintenanceTimeWindowsSatisfied);
    }

    public Map<String, CertAndKey> generateBrokerCerts(Kafka kafka, Set<String> externalBootstrapAddresses,
                                                       Map<Integer, Set<String>> externalAddresses, boolean isMaintenanceTimeWindowsSatisfied) throws IOException {
        String cluster = kafka.getMetadata().getName();
        String namespace = kafka.getMetadata().getNamespace();

        DnsNameGenerator kafkaDnsGenerator = DnsNameGenerator.of(namespace, KafkaCluster.serviceName(cluster));
        DnsNameGenerator kafkaHeadlessDnsGenerator = DnsNameGenerator.of(namespace, KafkaCluster.headlessServiceName(cluster));

        Function<Integer, Subject> subjectFn = i -> {
            Subject.Builder subject = new Subject.Builder()
                    .withOrganizationName("io.strimzi")
                    .withCommonName(KafkaCluster.kafkaClusterName(cluster));

            subject.addDnsName(KafkaCluster.serviceName(cluster));
            subject.addDnsName(String.format("%s.%s", KafkaCluster.serviceName(cluster), namespace));
            subject.addDnsName(kafkaDnsGenerator.serviceDnsNameWithoutClusterDomain());
            subject.addDnsName(kafkaDnsGenerator.serviceDnsName());
            subject.addDnsName(KafkaCluster.headlessServiceName(cluster));
            subject.addDnsName(String.format("%s.%s", KafkaCluster.headlessServiceName(cluster), namespace));
            subject.addDnsName(kafkaHeadlessDnsGenerator.serviceDnsNameWithoutClusterDomain());
            subject.addDnsName(kafkaHeadlessDnsGenerator.serviceDnsName());
            subject.addDnsName(KafkaCluster.podDnsName(namespace, cluster, i));
            subject.addDnsName(KafkaCluster.podDnsNameWithoutClusterDomain(namespace, cluster, i));

            if (externalBootstrapAddresses != null)   {
                for (String dnsName : externalBootstrapAddresses) {
                    if (!ipv4Address.matcher(dnsName).matches()) {
                        subject.addDnsName(dnsName);
                    } else {
                        subject.addIpAddress(dnsName);
                    }
                }
            }

            if (externalAddresses.get(i) != null)   {
                for (String dnsName : externalAddresses.get(i)) {
                    if (!ipv4Address.matcher(dnsName).matches()) {
                        subject.addDnsName(dnsName);
                    } else {
                        subject.addIpAddress(dnsName);
                    }
                }
            }

            return subject.build();
        };
        LOGGER.debugCr(reconciliation, "{}: Reconciling kafka broker certificates", this);
        return maybeCopyOrGenerateCerts(
            reconciliation,
            kafka.getSpec().getKafka().getReplicas(),
            subjectFn,
            brokersSecret,
            podNum -> KafkaCluster.kafkaPodName(cluster, podNum),
            isMaintenanceTimeWindowsSatisfied);
    }

    @Override
    protected String caCertGenerationAnnotation() {
        return ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION;
    }

    @SuppressWarnings("BooleanExpressionComplexity")
    @Override
    protected boolean hasCaCertGenerationChanged() {
        // at least one Secret has a different cluster CA certificate thumbprint.
        // it is useful when a renewal cluster CA certificate process needs to be recovered after an operator crash
        return hasCaCertGenerationChanged(zkNodesSecret) || hasCaCertGenerationChanged(brokersSecret) ||
                hasCaCertGenerationChanged(entityTopicOperatorSecret) || hasCaCertGenerationChanged(entityUserOperatorSecret) ||
                hasCaCertGenerationChanged(kafkaExporterSecret) || hasCaCertGenerationChanged(cruiseControlSecret) ||
                hasCaCertGenerationChanged(clusterOperatorSecret);
    }
}
