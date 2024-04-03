/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserTlsClientAuthentication;
import io.strimzi.api.kafka.model.user.KafkaUserTlsExternalClientAuthentication;
import io.strimzi.api.kafka.model.user.acl.AclRule;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.user.model.acl.SimpleAclRule;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Model of the Kafka user which is created based on the KafkaUser custom resource
 */
public class KafkaUserModel {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaUserModel.class.getName());

    /**
     * Key under which the password is stored in the Kubernetes Secret
     */
    public static final String KEY_PASSWORD = "password";

    /**
     * Key under which the SASL configuration is stored in the Kubernetes Secret
     */
    public static final String KEY_SASL_JAAS_CONFIG = "sasl.jaas.config";

    protected final String namespace;
    protected final String name;
    protected final Labels labels;

    protected KafkaUserAuthentication authentication;
    protected String caCert;
    protected CertAndKey userCertAndKey;
    protected String scramSha512Password;
    protected Set<SimpleAclRule> simpleAclRules = null;

    /**
     * Name of the USer Operator used for the Kubernetes labels
     */
    public static final String KAFKA_USER_OPERATOR_NAME = "strimzi-user-operator";

    // Owner Reference information
    private String ownerApiVersion;
    private String ownerKind;
    private String ownerUid;

    private KafkaUserQuotas quotas;
    private Map<String, String> templateSecretLabels;
    private Map<String, String> templateSecretAnnotations;

    private final String secretPrefix;

    /**
     * Constructor
     *
     * @param namespace Kubernetes namespace where Kafka Connect cluster resources are going to be created
     * @param name   User name
     * @param labels   Labels
     */
    protected KafkaUserModel(String namespace, String name, Labels labels, String secretPrefix) {
        this.namespace = namespace;
        this.name = name;
        this.labels = labels.withKubernetesName(KAFKA_USER_OPERATOR_NAME)
            .withKubernetesInstance(name)
            .withKubernetesPartOf(name)
            .withKubernetesManagedBy(KAFKA_USER_OPERATOR_NAME);
        this.secretPrefix = secretPrefix;
    }

    /**
     * Creates instance of KafkaUserModel from CRD definition.
     *
     * @param kafkaUser                 The Custom Resource based on which the model should be created.
     * @param secretPrefix              The prefix used to add to the name of the Secret generated from the KafkaUser resource.
     * @param aclsAdminApiSupported     Indicates whether Kafka Admin API can be used to manage ACL rights
     *
     * @return The user model.
     */
    public static KafkaUserModel fromCrd(KafkaUser kafkaUser,
                                         String secretPrefix,
                                         boolean aclsAdminApiSupported) {
        KafkaUserModel result = new KafkaUserModel(kafkaUser.getMetadata().getNamespace(),
                kafkaUser.getMetadata().getName(),
                Labels.fromResource(kafkaUser).withStrimziKind(kafkaUser.getKind()),
                secretPrefix);

        validateTlsUsername(kafkaUser);
        validateDesiredPassword(kafkaUser);

        result.setOwnerReference(kafkaUser);
        result.setAuthentication(kafkaUser.getSpec().getAuthentication());

        if (kafkaUser.getSpec().getAuthorization() != null && kafkaUser.getSpec().getAuthorization().getType().equals(KafkaUserAuthorizationSimple.TYPE_SIMPLE)) {
            if (aclsAdminApiSupported) {
                KafkaUserAuthorizationSimple simple = (KafkaUserAuthorizationSimple) kafkaUser.getSpec().getAuthorization();
                result.setSimpleAclRules(simple.getAcls());
            } else {
                throw new InvalidResourceException("Simple authorization ACL rules are configured but not supported in the Kafka cluster configuration.");
            }
        }

        result.setQuotas(kafkaUser.getSpec().getQuotas());

        if (kafkaUser.getSpec().getTemplate() != null
                && kafkaUser.getSpec().getTemplate().getSecret() != null
                && kafkaUser.getSpec().getTemplate().getSecret().getMetadata() != null)  {
            result.templateSecretLabels = kafkaUser.getSpec().getTemplate().getSecret().getMetadata().getLabels();
            result.templateSecretAnnotations = kafkaUser.getSpec().getTemplate().getSecret().getMetadata().getAnnotations();
        }

        return result;
    }

    /**
     * Validates if a user with TLS authentication doesn't have too long name. This has to be done because OpenSSL has
     * a limit how long the CN of a certificate can be.
     *
     * @param user  The KafkaUser which should be validated
     */
    private static void validateTlsUsername(KafkaUser user)  {
        if (user.getSpec().getAuthentication() instanceof KafkaUserTlsClientAuthentication) {
            if (user.getMetadata().getName().length() > OpenSslCertManager.MAXIMUM_CN_LENGTH)    {
                throw new InvalidResourceException("Users with TLS client authentication can have a username (name of the KafkaUser custom resource) only up to 64 characters long.");
            }
        }
    }

    /**
     * Validates if a user with desired password contains a full specification to the password
     *
     * @param user  The KafkaUser which should be validated
     */
    private static void validateDesiredPassword(KafkaUser user)  {
        if (user.getSpec().getAuthentication() instanceof KafkaUserScramSha512ClientAuthentication scramAuth) {
            if (scramAuth.getPassword() != null)    {
                if (scramAuth.getPassword().getValueFrom() == null
                        || scramAuth.getPassword().getValueFrom().getSecretKeyRef() == null
                        || scramAuth.getPassword().getValueFrom().getSecretKeyRef().getName() == null
                        || scramAuth.getPassword().getValueFrom().getSecretKeyRef().getKey() == null) {
                    throw new InvalidResourceException("Resource requests custom SCRAM-SHA-512 password but doesn't specify the secret name and/or key");
                }
            }
        }
    }

    /**
     * Generates secret containing the certificate for TLS client auth when TLS client auth is enabled for this user.
     * Returns null otherwise.
     *
     * @return The secret.
     */
    public Secret generateSecret()  {
        if (authentication instanceof KafkaUserTlsClientAuthentication) {
            Map<String, String> data = new HashMap<>(5);
            data.put("ca.crt", caCert);
            data.put("user.key", userCertAndKey.keyAsBase64String());
            data.put("user.crt", userCertAndKey.certAsBase64String());
            data.put("user.p12", userCertAndKey.keyStoreAsBase64String());
            data.put("user.password", userCertAndKey.storePasswordAsBase64String());
            return createSecret(data);
        } else if (authentication instanceof KafkaUserScramSha512ClientAuthentication) {
            Map<String, String> data = new HashMap<>(2);
            data.put(KafkaUserModel.KEY_PASSWORD, Base64.getEncoder().encodeToString(this.scramSha512Password.getBytes(StandardCharsets.US_ASCII)));
            data.put(KafkaUserModel.KEY_SASL_JAAS_CONFIG, Base64.getEncoder().encodeToString(getSaslJsonConfig().getBytes(StandardCharsets.US_ASCII)));
            return createSecret(data);
        } else {
            return null;
        }
    }

    /**
     * Manage certificates generation based on those already present in the Secrets
     *
     * @param reconciliation The reconciliation
     * @param certManager CertManager instance for handling certificates creation
     * @param passwordGenerator PasswordGenerator instance for generating passwords
     * @param clientsCaCertSecret The clients CA certificate Secret.
     * @param clientsCaKeySecret The clients CA key Secret.
     * @param userSecret Secret with the user certificate
     * @param validityDays The number of days the certificate should be valid for.
     * @param renewalDays The renewal days.
     * @param maintenanceWindows List of configured maintenance windows
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *              That time is used for checking maintenance windows
     */
    @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
    public void maybeGenerateCertificates(Reconciliation reconciliation, CertManager certManager, PasswordGenerator passwordGenerator,
                                          Secret clientsCaCertSecret, Secret clientsCaKeySecret, Secret userSecret, int validityDays,
                                          int renewalDays, List<String> maintenanceWindows, Clock clock) {
        validateCACertificates(clientsCaCertSecret, clientsCaKeySecret);

        ClientsCa clientsCa = new ClientsCa(
                reconciliation,
                certManager,
                passwordGenerator,
                clientsCaCertSecret.getMetadata().getName(),
                clientsCaCertSecret,
                clientsCaCertSecret.getMetadata().getName(),
                clientsCaKeySecret,
                validityDays,
                renewalDays,
                false,
                null);
        this.caCert = clientsCa.currentCaCertBase64();

        if (userSecret != null) {
            // Secret already exists -> lets verify if it has keys from the same CA
            String originalCaCrt = clientsCaCertSecret.getData().get("ca.crt");
            String caCrt = userSecret.getData().get("ca.crt");
            String userCrt = userSecret.getData().get("user.crt");
            String userKey = userSecret.getData().get("user.key");

            if (originalCaCrt != null
                    && originalCaCrt.equals(caCrt)
                    && userCrt != null
                    && !userCrt.isEmpty()
                    && userKey != null
                    && !userKey.isEmpty()) {
                if (clientsCa.isExpiring(userSecret, "user.crt"))   {
                    // The certificate exists but is expiring
                    if (Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()))   {
                        // => if we are in compliance with maintenance window, we renew it
                        LOGGER.infoCr(reconciliation, "Certificate for user {} in namespace {} is within the renewal period and will be renewed", name, namespace);
                        this.userCertAndKey = generateNewCertificate(reconciliation, clientsCa);
                    } else {
                        // => if we are outside of maintenance window, we reuse it
                        LOGGER.infoCr(reconciliation, "Certificate for user {} in namespace {} is within the renewal period and will be renewed in the next maintenance window", name, namespace);
                        this.userCertAndKey = reuseCertificate(reconciliation, clientsCa, userSecret);
                    }
                } else {
                    // The certificate exists and isn't expiring => we just reuse it
                    this.userCertAndKey = reuseCertificate(reconciliation, clientsCa, userSecret);
                }
            } else {
                // User secret exists, but does not seem to contain the complete user certificate => we have to generate a new user certificate
                this.userCertAndKey = generateNewCertificate(reconciliation, clientsCa);
            }
        } else {
            // User secret does not exist yet => we have to generate a new user certificate
            this.userCertAndKey = generateNewCertificate(reconciliation, clientsCa);
        }
    }

    CertAndKey generateNewCertificate(Reconciliation reconciliation, Ca clientsCa) {
        try {
            return clientsCa.generateSignedCert(name);
        } catch (IOException e) {
            LOGGER.errorCr(reconciliation, "Error generating signed certificate for user {}", name, e);
            return null;
        }
    }

    CertAndKey reuseCertificate(Reconciliation reconciliation, Ca clientsCa, Secret userSecret) {
        String userKeyStore = userSecret.getData().get("user.p12");
        String userKeyStorePassword = userSecret.getData().get("user.password");

        if (userKeyStore != null
                && !userKeyStore.isEmpty()
                && userKeyStorePassword != null
                && !userKeyStorePassword.isEmpty()) {
            return new CertAndKey(
                    decodeFromSecret(userSecret, "user.key"),
                    decodeFromSecret(userSecret, "user.crt"),
                    null,
                    decodeFromSecret(userSecret, "user.p12"),
                    new String(decodeFromSecret(userSecret, "user.password"), StandardCharsets.US_ASCII));
        } else {
            // coming from an older operator version, the user secret exists but without keystore and password
            try {
                return clientsCa.addKeyAndCertToKeyStore(name,
                        decodeFromSecret(userSecret, "user.key"),
                        decodeFromSecret(userSecret, "user.crt"));
            } catch (IOException e) {
                LOGGER.errorCr(reconciliation, "Error generating the keystore for user {}", name, e);
                return null;
            }
        }
    }

    void validateCACertificates(Secret clientsCaCertSecret, Secret clientsCaKeySecret)   {
        if (clientsCaCertSecret == null) {
            // CA certificate secret does not exist
            throw new InvalidCertificateException("The Clients CA Cert Secret is missing");
        } else if (clientsCaCertSecret.getData() == null || clientsCaCertSecret.getData().get("ca.crt") == null)    {
            // CA certificate secret exists, but does not have the ca.crt key
            throw new InvalidCertificateException("The Clients CA Cert Secret is missing the ca.crt file");
        } else if (clientsCaKeySecret == null) {
            // CA certificate secret does not exist
            throw new InvalidCertificateException("The Clients CA Key Secret is missing");
        } else if (clientsCaKeySecret.getData() == null || clientsCaKeySecret.getData().get("ca.key") == null)    {
            // CA private key secret exists, but does not have the ca.crt key
            throw new InvalidCertificateException("The Clients CA Key Secret is missing the ca.key file");
        }
    }

    /**
     * Prepares password for further use. It either takes the password specified by the user, re-uses the existing
     * password or generates a new one.
     *
     * @param reconciliation The reconciliation.
     * @param generator The password generator.
     * @param userSecret The Secret containing any existing password.
     * @param desiredPasswordSecret The Secret with the desired password specified by the user
     */
    public void maybeGeneratePassword(Reconciliation reconciliation, PasswordGenerator generator, Secret userSecret, Secret desiredPasswordSecret) {
        if (isUserWithDesiredPassword())  {
            // User requested custom secret
            if (desiredPasswordSecret == null)  {
                throw new InvalidResourceException("Secret " + desiredPasswordSecretName() + " with requested user password does not exist.");
            }

            String password = desiredPasswordSecret.getData().get(desiredPasswordSecretKey());

            if (password == null) {
                throw new InvalidResourceException("Secret " + desiredPasswordSecretName() + " does not contain the key " + desiredPasswordSecretKey() + " with requested user password.");
            } else if (password.isEmpty()) {
                throw new InvalidResourceException("The requested user password is empty.");
            }

            LOGGER.debugCr(reconciliation, "Loading request password from Kubernetes Secret {}", desiredPasswordSecretName());
            this.scramSha512Password = Util.decodeFromBase64(password);
            return;
        } else if (userSecret != null) {
            // Secret already exists -> lets verify if it has a password
            String password = userSecret.getData().get(KEY_PASSWORD);
            if (password != null && !password.isEmpty()) {
                LOGGER.debugCr(reconciliation, "Re-using password which already exists");
                this.scramSha512Password = Util.decodeFromBase64(password);
                return;
            }
        }

        LOGGER.debugCr(reconciliation, "Generating user password");
        this.scramSha512Password = generator.generate();
    }

    /**
     * Decode from Base64 a keyed value from a Secret
     *
     * @param secret Secret from which decoding the value
     * @param key Key of the value to decode
     * @return decoded value
     */
    protected byte[] decodeFromSecret(Secret secret, String key) {
        return Util.decodeBytesFromBase64(secret.getData().get(key));
    }

    /**
     * Creates secret with the data
     *
     * @param data Map with the Secret content
     * @return The secret.
     */
    protected Secret createSecret(Map<String, String> data) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(getSecretName())
                    .withNamespace(namespace)
                    .withLabels(Util.mergeLabelsOrAnnotations(labels.toMap(), templateSecretLabels))
                    .withAnnotations(Util.mergeLabelsOrAnnotations(null, templateSecretAnnotations))
                    .withOwnerReferences(createOwnerReference())
                .endMetadata()
                .withType("Opaque")
                .withData(data)
                .build();
    }

    /**
     * Generate the OwnerReference object to link newly created objects to their parent (the custom resource)
     *
     * @return The owner reference.
     */
    protected OwnerReference createOwnerReference() {
        return new OwnerReferenceBuilder()
                .withApiVersion(ownerApiVersion)
                .withKind(ownerKind)
                .withName(name)
                .withUid(ownerUid)
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();
    }

    /**
     * Set fields needed to generate the OwnerReference object
     *
     * @param parent The resource which should be used as parent. It will be used to gather the date needed for generating OwnerReferences.
     */
    protected void setOwnerReference(HasMetadata parent)  {
        this.ownerApiVersion = parent.getApiVersion();
        this.ownerKind = parent.getKind();
        this.ownerUid = parent.getMetadata().getUid();
    }

    /**
     * Decodes the name of the User secret based on the username
     *
     * @param username The username.
     * @return The decoded username.
     */
    public static String decodeUsername(String username) {
        if (username.contains("CN="))   {
            try {
                return new LdapName(username).getRdns().stream()
                        .filter(rdn -> rdn.getType().equalsIgnoreCase("cn"))
                        .map(rdn -> rdn.getValue().toString()).collect(Collectors.joining());
            } catch (InvalidNameException e)    {
                throw new IllegalArgumentException(e);
            }
        } else  {
            return username;
        }
    }

    /**
     * Generates the name of the User secret based on the username
     *
     * @param username The username.
     * @return The TLS username.
     */
    public static String getTlsUserName(String username)    {
        return "CN=" + username;
    }

    /**
     * Generates the name of the User secret based on the username
     *
     * @param username The username.
     * @return The SCRAM username.
     */
    public static String getScramUserName(String username)    {
        return username;
    }

    /**
     * Gets the Username
     *
     * @return The user name.
     */
    public String getUserName()    {
        if (isTlsUser() || isTlsExternalUser()) {
            return getTlsUserName(name);
        } else if (isScramUser()) {
            return getScramUserName(name);
        } else {
            return getName();
        }
    }

    /**
     * @return The name of the user.
     */
    public String getName() {
        return name;
    }

    /**
     * Generates the name of the User secret based on the username.
     *
     * @param secretPrefix The secret prefix
     * @param username The username.
     * @return The name of the user.
     */
    public static String getSecretName(String secretPrefix, String username)    {
        return secretPrefix + username;
    }

    /**
     * Creates the JAAS configuration string for SASL SCRAM-SHA-512 authentication.
     *
     * @param username The SCRAM username.
     * @param password The SCRAM password.
     * @return The JAAS configuration string.
     */
    public static String getSaslJsonConfig(String username, String password) {
        return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"" + username + "\" password=\"" + password + "\";";
    }

    /**
     * Gets the name of the User secret.
     *
     * @return The name of the user secret.
     */
    public String getSecretName()    {
        return KafkaUserModel.getSecretName(secretPrefix, name);
    }

    /**
     * Sets the authentication method.
     *
     * @param authentication Authentication method.
     */
    public void setAuthentication(KafkaUserAuthentication authentication) {
        this.authentication = authentication;
    }

    /**
     * Sets the quotas to the user.
     *
     * @param quotas KafkaUserQuotas to be set.
     */
    public void setQuotas(KafkaUserQuotas quotas) {
        this.quotas = quotas;
    }

    /**
     * Gets the quotas.
     *
     * @return User Quotas.
     */
    public KafkaUserQuotas getQuotas()    {
        return quotas;
    }

    /**
     * Get the list of ACL rules for Simple Authorization which should apply to this user.
     *
     * @return The ACL rules.
     */
    public Set<SimpleAclRule> getSimpleAclRules() {
        return simpleAclRules;
    }

    /**
     * Sets the list of ACL rules for Simple authorization.
     *
     * @param rules List of ACL rules which should be applied to this user.
     */
    public void setSimpleAclRules(List<AclRule> rules) {
        Set<SimpleAclRule> simpleAclRules = new HashSet<>();

        for (AclRule rule : rules)  {
            simpleAclRules.addAll(SimpleAclRule.fromCrd(rule));
        }

        this.simpleAclRules = simpleAclRules;
    }

    /**
     * Returns true if the user is using TLS authentication.
     *
     * @return true if the user is using TLS authentication.
     */
    public boolean isTlsUser()  {
        return authentication instanceof KafkaUserTlsClientAuthentication;
    }

    /**
     * Returns true if the user is using TLS-EXTERNAL authentication.
     *
     * @return true if the user is using TLS-EXTERNAL authentication.
     */
    public boolean isTlsExternalUser()  {
        return authentication instanceof KafkaUserTlsExternalClientAuthentication;
    }

    /**
     * Returns true if the user is using SCRAM-SHA-512 authentication.
     *
     * @return true if the user is using SCRAM-SHA-512 authentication.
     */
    public boolean isScramUser()  {
        return authentication instanceof KafkaUserScramSha512ClientAuthentication;
    }

    /**
     * Returns true if the user is using SCRAM-SHA-512 authentication and requested some specific password.
     *
     * @return true if the user is using SCRAM-SHA-512 authentication  and requested some specific password.
     */
    public boolean isUserWithDesiredPassword()  {
        return desiredPasswordSecretName() != null;
    }

    /**
     * Returns the name of the secret with provided password for SCRAM-SHA-512 users.
     *
     * @return Name of the Kubernetes Secret with the password or null if not set
     */
    public String desiredPasswordSecretName()  {
        if (isScramUser()) {
            KafkaUserScramSha512ClientAuthentication scramAuth = (KafkaUserScramSha512ClientAuthentication) authentication;
            if (scramAuth.getPassword() != null
                    && scramAuth.getPassword().getValueFrom() != null
                    && scramAuth.getPassword().getValueFrom().getSecretKeyRef() != null
                    && scramAuth.getPassword().getValueFrom().getSecretKeyRef().getName() != null) {
                return scramAuth.getPassword().getValueFrom().getSecretKeyRef().getName();
            }
        }

        return null;
    }

    /**
     * Returns the name of the secret with provided password for SCRAM-SHA-512 users.
     *
     * @return Name of the Kubernetes Secret with the password or null if not set
     */
    public String desiredPasswordSecretKey()  {
        if (isScramUser()) {
            KafkaUserScramSha512ClientAuthentication scramAuth = (KafkaUserScramSha512ClientAuthentication) authentication;
            if (scramAuth.getPassword() != null
                    && scramAuth.getPassword().getValueFrom() != null
                    && scramAuth.getPassword().getValueFrom().getSecretKeyRef() != null
                    && scramAuth.getPassword().getValueFrom().getSecretKeyRef().getKey() != null) {
                return scramAuth.getPassword().getValueFrom().getSecretKeyRef().getKey();
            }
        }

        return null;
    }

    /**
     * @return  Returns the SCRAM-SHA-512 user password
     */
    public String getScramSha512Password() {
        return scramSha512Password;
    }

    /**
     * Returns true if the user is configured without authentication section and is not using any authentication.
     * Such user might be used for example for things such as OAUTH authentication where the users are not managed by
     * the User Operator.
     *
     * @return true if the user is not using any authentication.
     */
    public boolean isNoneUser()  {
        return authentication == null;
    }

    /**
     * Creates the JAAS configuration string for SASL SCRAM-SHA-512 authentication.
     *
     * @return The JAAS configuration string.
     */
    public String getSaslJsonConfig() {
        return getSaslJsonConfig(getScramUserName(name), scramSha512Password);
    }

}
