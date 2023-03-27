/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.CruiseControlResources;
import io.strimzi.api.kafka.model.KafkaExporterResources;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.IpAndDnsValidation;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;

/**
 * Represents the Cluster CA
 */
public class ClusterCa extends Ca {
    /**
     * Pattern used for the old CA certificate during CA renewal. This pattern is used to recognize this certificate
     * and delete it when it is not needed anymore.
     */
    private static final Pattern OLD_CA_CERT_PATTERN = Pattern.compile("^ca-\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2}Z.crt$");

    private final String clusterName;
    private Secret entityTopicOperatorSecret;
    private Secret entityUserOperatorSecret;
    private Secret clusterOperatorSecret;
    private Secret kafkaExporterSecret;
    private Secret cruiseControlSecret;

    private Secret brokersSecret;
    private Secret zkNodesSecret;

    /**
     * Constructor
     *
     * @param reconciliation        Reconciliation marker
     * @param certManager           Certificate manager instance
     * @param passwordGenerator     Password generator instance
     * @param clusterName           Name of the Kafka cluster
     * @param caCertSecret          Name of the CA public key secret
     * @param caKeySecret           Name of the CA private key secret
     */
    public ClusterCa(Reconciliation reconciliation, CertManager certManager, PasswordGenerator passwordGenerator, String clusterName, Secret caCertSecret, Secret caKeySecret) {
        this(reconciliation, certManager, passwordGenerator, clusterName, caCertSecret, caKeySecret, 365, 30, true, null);
    }

    /**
     * Constructor
     *
     * @param reconciliation        Reconciliation marker
     * @param certManager           Certificate manager instance
     * @param passwordGenerator     Password generator instance
     * @param clusterName           Name of the Kafka cluster
     * @param clusterCaCert         Secret with the public key
     * @param clusterCaKey          Secret with the private key
     * @param validityDays          Validity days
     * @param renewalDays           Renewal days (how many days before expiration should the CA be renewed)
     * @param generateCa            Flag indicating if Strimzi CA should be generated or custom CA is used
     * @param policy                Renewal policy
     */
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
                clusterCaKey, validityDays, renewalDays, generateCa, policy);
        this.clusterName = clusterName;
    }

    @Override
    public String toString() {
        return "cluster-ca";
    }

    /**
     * Initializes the CA Secrets inside this class
     *
     * @param secrets   List with the secrets
     */
    public void initCaSecrets(List<Secret> secrets) {
        for (Secret secret: secrets) {
            String name = secret.getMetadata().getName();
            if (KafkaResources.kafkaSecretName(clusterName).equals(name)) {
                brokersSecret = secret;
            } else if (KafkaResources.entityTopicOperatorSecretName(clusterName).equals(name)) {
                entityTopicOperatorSecret = secret;
            } else if (KafkaResources.entityUserOperatorSecretName(clusterName).equals(name)) {
                entityUserOperatorSecret = secret;
            } else if (KafkaResources.zookeeperSecretName(clusterName).equals(name)) {
                zkNodesSecret = secret;
            } else if (ClusterOperator.secretName(clusterName).equals(name)) {
                clusterOperatorSecret = secret;
            } else if (KafkaExporterResources.secretName(clusterName).equals(name)) {
                kafkaExporterSecret = secret;
            } else if (CruiseControlResources.secretName(clusterName).equals(name)) {
                cruiseControlSecret = secret;
            }
        }
    }

    protected Secret entityTopicOperatorSecret() {
        return entityTopicOperatorSecret;
    }

    protected Secret entityUserOperatorSecret() {
        return entityUserOperatorSecret;
    }

    /**
     * @return  The secret with the Cluster Operator certificate
     */
    public Secret clusterOperatorSecret() {
        return clusterOperatorSecret;
    }

    protected Secret kafkaExporterSecret() {
        return kafkaExporterSecret;
    }

    protected Map<String, CertAndKey> generateCcCerts(String namespace, String kafkaName, boolean isMaintenanceTimeWindowsSatisfied) throws IOException {
        DnsNameGenerator ccDnsGenerator = DnsNameGenerator.of(namespace, CruiseControlResources.serviceName(kafkaName));

        Function<Integer, Subject> subjectFn = i -> {
            Subject.Builder subject = new Subject.Builder()
                    .withOrganizationName("io.strimzi")
                    .withCommonName(CruiseControlResources.serviceName(kafkaName));

            subject.addDnsName(CruiseControlResources.serviceName(kafkaName));
            subject.addDnsName(String.format("%s.%s", CruiseControlResources.serviceName(kafkaName), namespace));
            subject.addDnsName(ccDnsGenerator.serviceDnsNameWithoutClusterDomain());
            subject.addDnsName(ccDnsGenerator.serviceDnsName());
            subject.addDnsName(CruiseControlResources.serviceName(kafkaName));
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

    protected Map<String, CertAndKey> generateZkCerts(String namespace, String kafkaName, int replicas, boolean isMaintenanceTimeWindowsSatisfied) throws IOException {
        DnsNameGenerator zkDnsGenerator = DnsNameGenerator.of(namespace, KafkaResources.zookeeperServiceName(kafkaName));
        DnsNameGenerator zkHeadlessDnsGenerator = DnsNameGenerator.of(namespace, KafkaResources.zookeeperHeadlessServiceName(kafkaName));

        Function<Integer, Subject> subjectFn = i -> {
            Subject.Builder subject = new Subject.Builder()
                    .withOrganizationName("io.strimzi")
                    .withCommonName(KafkaResources.zookeeperStatefulSetName(kafkaName));
            subject.addDnsName(KafkaResources.zookeeperServiceName(kafkaName));
            subject.addDnsName(String.format("%s.%s", KafkaResources.zookeeperServiceName(kafkaName), namespace));
            subject.addDnsName(zkDnsGenerator.serviceDnsNameWithoutClusterDomain());
            subject.addDnsName(zkDnsGenerator.serviceDnsName());
            subject.addDnsName(DnsNameGenerator.podDnsName(namespace, KafkaResources.zookeeperHeadlessServiceName(kafkaName), KafkaResources.zookeeperPodName(kafkaName, i)));
            subject.addDnsName(DnsNameGenerator.podDnsNameWithoutClusterDomain(namespace, KafkaResources.zookeeperHeadlessServiceName(kafkaName), KafkaResources.zookeeperPodName(kafkaName, i)));
            subject.addDnsName(zkDnsGenerator.wildcardServiceDnsNameWithoutClusterDomain());
            subject.addDnsName(zkDnsGenerator.wildcardServiceDnsName());
            subject.addDnsName(zkHeadlessDnsGenerator.wildcardServiceDnsNameWithoutClusterDomain());
            subject.addDnsName(zkHeadlessDnsGenerator.wildcardServiceDnsName());
            return subject.build();
        };

        LOGGER.debugCr(reconciliation, "{}: Reconciling zookeeper certificates", this);
        return maybeCopyOrGenerateCerts(
            reconciliation,
            replicas,
            subjectFn,
            zkNodesSecret,
            podNum -> KafkaResources.zookeeperPodName(kafkaName, podNum),
            isMaintenanceTimeWindowsSatisfied);
    }

    protected Map<String, CertAndKey> generateBrokerCerts(String namespace, String cluster, int replicas, Set<String> externalBootstrapAddresses,
                                                       Map<Integer, Set<String>> externalAddresses, boolean isMaintenanceTimeWindowsSatisfied) throws IOException {
        Function<Integer, Subject> subjectFn = i -> {
            Subject.Builder subject = new Subject.Builder()
                    .withOrganizationName("io.strimzi")
                    .withCommonName(KafkaResources.kafkaStatefulSetName(cluster));

            subject.addDnsNames(ModelUtils.generateAllServiceDnsNames(namespace, KafkaResources.bootstrapServiceName(cluster)));
            subject.addDnsNames(ModelUtils.generateAllServiceDnsNames(namespace, KafkaResources.brokersServiceName(cluster)));

            subject.addDnsName(DnsNameGenerator.podDnsName(namespace, KafkaResources.brokersServiceName(cluster), KafkaResources.kafkaPodName(cluster, i)));
            subject.addDnsName(DnsNameGenerator.podDnsNameWithoutClusterDomain(namespace, KafkaResources.brokersServiceName(cluster), KafkaResources.kafkaPodName(cluster, i)));

            if (externalBootstrapAddresses != null)   {
                for (String dnsName : externalBootstrapAddresses) {
                    if (IpAndDnsValidation.isValidIpAddress(dnsName))   {
                        subject.addIpAddress(dnsName);
                    } else {
                        subject.addDnsName(dnsName);
                    }
                }
            }

            if (externalAddresses.get(i) != null)   {
                for (String dnsName : externalAddresses.get(i)) {
                    if (IpAndDnsValidation.isValidIpAddress(dnsName))   {
                        subject.addIpAddress(dnsName);
                    } else {
                        subject.addDnsName(dnsName);
                    }
                }
            }

            return subject.build();
        };
        LOGGER.debugCr(reconciliation, "{}: Reconciling kafka broker certificates", this);
        return maybeCopyOrGenerateCerts(
            reconciliation,
            replicas,
            subjectFn,
            brokersSecret,
            podNum -> KafkaResources.kafkaPodName(cluster, podNum),
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

    /**
     * Copy already existing certificates from provided Secret based on number of effective replicas
     * and maybe generate new ones for new replicas (i.e. scale-up).
     *
     * @param reconciliation                        Reconciliation marker
     * @param replicas                              Number of replicas
     * @param subjectFn                             Function to generate certificate subject for given node / pod
     * @param secret                                Secret with certificates
     * @param podNameFn                             Function to generate pod name for given node ID
     * @param isMaintenanceTimeWindowsSatisfied     Flag indicating if we are inside an maintenance window or not
     *
     * @return  Returns map with node certificates which can be used to create or update the certificate secret
     *
     * @throws IOException  Throws IOException when working with files fails
     */
    /* test */ Map<String, CertAndKey> maybeCopyOrGenerateCerts(
            Reconciliation reconciliation,
            int replicas,
            Function<Integer, Subject> subjectFn,
            Secret secret,
            Function<Integer, String> podNameFn,
            boolean isMaintenanceTimeWindowsSatisfied) throws IOException {
        int replicasInSecret;
        if (secret == null || secret.getData() == null || this.certRenewed())   {
            replicasInSecret = 0;
        } else {
            replicasInSecret = (int) secret.getData().keySet().stream().filter(k -> k.contains(".crt")).count();
        }

        File brokerCsrFile = Files.createTempFile("tls", "broker-csr").toFile();
        File brokerKeyFile = Files.createTempFile("tls", "broker-key").toFile();
        File brokerCertFile = Files.createTempFile("tls", "broker-cert").toFile();
        File brokerKeyStoreFile = Files.createTempFile("tls", "broker-p12").toFile();

        int replicasInNewSecret = Math.min(replicasInSecret, replicas);
        Map<String, CertAndKey> certs = new HashMap<>(replicasInNewSecret);
        // copying the minimum number of certificates already existing in the secret
        // scale up -> it will copy all certificates
        // scale down -> it will copy just the requested number of replicas
        for (int i = 0; i < replicasInNewSecret; i++) {
            String podName = podNameFn.apply(i);
            LOGGER.debugCr(reconciliation, "Certificate for {} already exists", podName);
            Subject subject = subjectFn.apply(i);

            CertAndKey certAndKey;

            if (isNewVersion(secret, podName)) {
                certAndKey = asCertAndKey(secret, podName);
            } else {
                // coming from an older operator version, the secret exists but without keystore and password
                certAndKey = addKeyAndCertToKeyStore(subject.commonName(),
                        Base64.getDecoder().decode(secretEntryDataForPod(secret, podName, SecretEntry.KEY)),
                        Base64.getDecoder().decode(secretEntryDataForPod(secret, podName, SecretEntry.CRT)));
            }

            List<String> reasons = new ArrayList<>(2);

            if (certSubjectChanged(certAndKey, subject, podName))   {
                reasons.add("DNS names changed");
            }

            if (isExpiring(secret, podName + ".crt") && isMaintenanceTimeWindowsSatisfied)  {
                reasons.add("certificate is expiring");
            }

            if (renewalType.equals(RenewalType.CREATE)) {
                reasons.add("certificate added");
            }

            if (!reasons.isEmpty())  {
                LOGGER.infoCr(reconciliation, "Certificate for pod {} need to be regenerated because: {}", podName, String.join(", ", reasons));

                CertAndKey newCertAndKey = generateSignedCert(subject, brokerCsrFile, brokerKeyFile, brokerCertFile, brokerKeyStoreFile);
                certs.put(podName, newCertAndKey);
            }   else {
                certs.put(podName, certAndKey);
            }
        }

        // generate the missing number of certificates
        // scale up -> generate new certificates for added replicas
        // scale down -> does nothing
        for (int i = replicasInSecret; i < replicas; i++) {
            String podName = podNameFn.apply(i);

            LOGGER.debugCr(reconciliation, "Certificate for pod {} to generate", podName);
            CertAndKey k = generateSignedCert(subjectFn.apply(i),
                    brokerCsrFile, brokerKeyFile, brokerCertFile, brokerKeyStoreFile);
            certs.put(podName, k);
        }
        delete(reconciliation, brokerCsrFile);
        delete(reconciliation, brokerKeyFile);
        delete(reconciliation, brokerCertFile);
        delete(reconciliation, brokerKeyStoreFile);

        return certs;
    }

    /**
     * Check if this secret is coming from newer versions of the operator or older ones. Secrets from an older version
     * don't have a keystore and password.
     *
     * @param secret    Secret resource to check
     * @param podName   Name of the pod with certificate and key entries in the secret
     *
     * @return  True if this secret was created by a newer version of the operator and false otherwise.
     */
    private boolean isNewVersion(Secret secret, String podName) {
        String store = secretEntryDataForPod(secret, podName, SecretEntry.P12_KEYSTORE);
        String password = secretEntryDataForPod(secret, podName, SecretEntry.P12_KEYSTORE_PASSWORD);

        return store != null && !store.isEmpty() && password != null && !password.isEmpty();
    }

    /**
     * Return given secret for pod as a CertAndKey object
     *
     * @param secret    Kubernetes Secret
     * @param podName   Name of the pod
     *
     * @return  CertAndKey instance
     */
    private static CertAndKey asCertAndKey(Secret secret, String podName) {
        return asCertAndKey(secret, secretEntryNameForPod(podName, SecretEntry.KEY),
                secretEntryNameForPod(podName, SecretEntry.CRT),
                secretEntryNameForPod(podName, SecretEntry.P12_KEYSTORE),
                secretEntryNameForPod(podName, SecretEntry.P12_KEYSTORE_PASSWORD));
    }

    /**
     * Checks whether subject alternate names changed and certificate needs a renewal
     *
     * @param certAndKey        Current certificate
     * @param desiredSubject    Desired subject alternate names
     * @param podName           Name of the pod to which this certificate belongs (used for log messages)
     *
     * @return  True if the subjects are different, false otherwise
     */
    /* test */ boolean certSubjectChanged(CertAndKey certAndKey, Subject desiredSubject, String podName)    {
        Collection<String> desiredAltNames = desiredSubject.subjectAltNames().values();
        Collection<String> currentAltNames = getSubjectAltNames(certAndKey.cert());

        if (currentAltNames != null && desiredAltNames.containsAll(currentAltNames) && currentAltNames.containsAll(desiredAltNames))   {
            LOGGER.traceCr(reconciliation, "Alternate subjects match. No need to refresh cert for pod {}.", podName);
            return false;
        } else {
            LOGGER.infoCr(reconciliation, "Alternate subjects for pod {} differ", podName);
            LOGGER.infoCr(reconciliation, "Current alternate subjects: {}", currentAltNames);
            LOGGER.infoCr(reconciliation, "Desired alternate subjects: {}", desiredAltNames);
            return true;
        }
    }

    /**
     * Extracts the alternate subject names out of existing certificate
     *
     * @param certificate   Existing X509 certificate as a byte array
     *
     * @return  List of certificate Subject Alternate Names
     */
    private List<String> getSubjectAltNames(byte[] certificate) {
        List<String> subjectAltNames = null;

        try {
            X509Certificate cert = x509Certificate(certificate);
            Collection<List<?>> altNames = cert.getSubjectAlternativeNames();
            subjectAltNames = altNames.stream()
                    .filter(name -> name.get(1) instanceof String)
                    .map(item -> (String) item.get(1))
                    .collect(Collectors.toList());
        } catch (CertificateException | RuntimeException e) {
            // TODO: We should mock the certificates properly so that this doesn't fail in tests (not now => long term :-o)
            LOGGER.debugCr(reconciliation, "Failed to parse existing certificate", e);
        }

        return subjectAltNames;
    }

    /**
     * Retrieve a specific secret entry for pod from the given Secret.
     *
     * @param secret    Kubernetes Secret containing desired entry
     * @param podName   Name of the pod which secret entry is looked for
     * @param entry     The SecretEntry type
     *
     * @return  The data of the secret entry if found or null otherwise
     */
    private static String secretEntryDataForPod(Secret secret, String podName, SecretEntry entry) {
        return secret.getData().get(secretEntryNameForPod(podName, entry));
    }

    /**
     * Get the name of secret entry of given SecretEntry type for podName
     *
     * @param podName   Name of the pod which secret entry is looked for
     * @param entry     The SecretEntry type
     *
     * @return  The name of the secret entry
     */
    public static String secretEntryNameForPod(String podName, SecretEntry entry) {
        return podName + entry.suffix;
    }

    /**
     * Remove old certificates that are stored in the CA Secret matching the "ca-YYYY-MM-DDTHH-MM-SSZ.crt" naming pattern.
     * NOTE: mostly used when a CA certificate is renewed by replacing the key
     */
    public void maybeDeleteOldCerts() {
        // the operator doesn't have to touch Secret provided by the user with his own custom CA certificate
        if (this.generateCa) {
            this.caCertsRemoved = removeCerts(this.caCertSecret.getData(), entry -> OLD_CA_CERT_PATTERN.matcher(entry.getKey()).matches()) > 0;
            if (this.caCertsRemoved) {
                LOGGER.infoCr(reconciliation, "{}: Old CA certificates removed", this);
            }
        }
    }
}
