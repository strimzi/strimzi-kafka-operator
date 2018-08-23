/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.AclRule;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserAuthentication;
import io.strimzi.api.kafka.model.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.KafkaUserScramSha512ClientAuthentication;
import io.strimzi.api.kafka.model.KafkaUserTlsClientAuthentication;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.operator.user.operator.PasswordGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class KafkaUserModel {
    private static final Logger log = LogManager.getLogger(KafkaUserModel.class.getName());

    private final static int CERTS_EXPIRATION_DAYS = 356;
    public static final String KEY_PASSWORD = "password";

    protected final String namespace;
    protected final String name;
    protected final Labels labels;

    protected KafkaUserAuthentication authentication;
    protected CertAndKey caCertAndKey;
    protected CertAndKey userCertAndKey;
    protected String scramSha512Password;

    protected Set<SimpleAclRule> simpleAclRules = null;

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Connect cluster resources are going to be created
     * @param name   User name
     * @param labels   Labels
     */
    protected KafkaUserModel(String namespace, String name, Labels labels) {
        this.namespace = namespace;
        this.name = name;
        this.labels = labels;
    }

    /**
     * Creates instance of KafkaUserModel from CRD definition
     *
     * @param certManager   CertManager instance for work with certificates
     * @param passwordGenerator A password generator
     * @param kafkaUser     The Custom Resource based on which the model should be created
     * @param clientsCa     Kubernetes secret with the clients certification authority
     * @param userSecret    Kubernetes secret with existing user certificate
     * @return
     */
    public static KafkaUserModel fromCrd(CertManager certManager,
                                         PasswordGenerator passwordGenerator,
                                         KafkaUser kafkaUser,
                                         Secret clientsCa, Secret userSecret) {
        KafkaUserModel result = new KafkaUserModel(kafkaUser.getMetadata().getNamespace(),
                kafkaUser.getMetadata().getName(),
                Labels.fromResource(kafkaUser).withKind(kafkaUser.getKind()));
        result.setAuthentication(kafkaUser.getSpec().getAuthentication());

        if (kafkaUser.getSpec().getAuthentication() instanceof KafkaUserTlsClientAuthentication) {
            result.maybeGenerateCertificates(certManager, clientsCa, userSecret);
        } else if (kafkaUser.getSpec().getAuthentication() instanceof KafkaUserScramSha512ClientAuthentication) {
            result.maybeGeneratePassword(passwordGenerator, userSecret);
        }

        if (kafkaUser.getSpec().getAuthorization() != null && kafkaUser.getSpec().getAuthorization().getType().equals(KafkaUserAuthorizationSimple.TYPE_SIMPLE)) {
            KafkaUserAuthorizationSimple simple = (KafkaUserAuthorizationSimple) kafkaUser.getSpec().getAuthorization();
            result.setSimpleAclRules(simple.getAcls());
        }

        return result;
    }

    /**
     * Generates secret containing the certificate for TLS client auth when TLS client auth is enabled for this user.
     * Returns null otherwise.
     *
     * @return
     */
    public Secret generateSecret()  {
        if (authentication instanceof KafkaUserTlsClientAuthentication) {
            Map<String, String> data = new HashMap<>();
            data.put("ca.crt", caCertAndKey.certAsBase64String());
            data.put("user.key", userCertAndKey.keyAsBase64String());
            data.put("user.crt", userCertAndKey.certAsBase64String());
            return createSecret(data);
        } else if (authentication instanceof KafkaUserScramSha512ClientAuthentication) {
            Map<String, String> data = new HashMap<>();
            data.put(KafkaUserModel.KEY_PASSWORD, Base64.getEncoder().encodeToString(scramSha512Password.getBytes(Charset.forName("US-ASCII"))));
            return createSecret(data);
        } else {
            return null;
        }
    }

    /**
     * Manage certificates generation based on those already present in the Secrets
     *
     * @param certManager CertManager instance for handling certificates creation
     * @param clientsCa Secret with the CA
     * @param userSecret Secret with the user certificate
     */
    public void maybeGenerateCertificates(CertManager certManager, Secret clientsCa, Secret userSecret) {
        try {
            if (clientsCa != null) {
                this.caCertAndKey = new CertAndKey(
                        decodeFromSecret(clientsCa, "clients-ca.key"),
                        decodeFromSecret(clientsCa, "clients-ca.crt")
                );

                if (userSecret != null) {
                    // Secret already exists -> lets verify if it has keys from the same CA
                    String originalCaCrt = clientsCa.getData().get("clients-ca.crt");
                    String caCrt = userSecret.getData().get("ca.crt");
                    String userCrt = userSecret.getData().get("user.crt");
                    String userKey = userSecret.getData().get("user.key");

                    if (originalCaCrt != null
                            && originalCaCrt.equals(caCrt)
                            && userCrt != null
                            && !userCrt.isEmpty()
                            && userKey != null
                            && !userKey.isEmpty())    {
                        // User certificate already exists and and is from the right CA -> no need to generate new certificate
                        log.debug("Reusing existing user certificate");
                        this.userCertAndKey = new CertAndKey(
                                decodeFromSecret(userSecret, "user.key"),
                                decodeFromSecret(userSecret, "user.crt")
                        );
                        return;
                    }
                }

                log.debug("Generating user certificate");

                File userCsrFile = File.createTempFile("tls", name + ".csr");
                File userKeyFile = File.createTempFile("tls", name + ".key");
                File userCrtFile = File.createTempFile("tls", name + ".crt");

                Subject userSubject = new Subject();
                userSubject.setCommonName(name);

                certManager.generateCsr(userKeyFile, userCsrFile, userSubject);
                certManager.generateCert(userCsrFile, caCertAndKey.key(), caCertAndKey.cert(), userCrtFile,
                        userSubject, CERTS_EXPIRATION_DAYS);
                this.userCertAndKey = new CertAndKey(Files.readAllBytes(userKeyFile.toPath()), Files.readAllBytes(userCrtFile.toPath()));

                if (!userCsrFile.delete()) {
                    log.warn("{} cannot be deleted", userCsrFile.getName());
                }
                if (!userKeyFile.delete()) {
                    log.warn("{} cannot be deleted", userKeyFile.getName());
                }
                if (!userCrtFile.delete()) {
                    log.warn("{} cannot be deleted", userCrtFile.getName());
                }

                log.debug("End generating user certificate");
            } else {
                throw new NoCertificateSecretException("The Clients CA Secret is missing");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void maybeGeneratePassword(PasswordGenerator generator, Secret userSecret) {
        if (userSecret != null) {
            // Secret already exists -> lets verify if it has a password
            String password = userSecret.getData().get(KEY_PASSWORD);
            if (password != null && !password.isEmpty()) {
                this.scramSha512Password = new String(Base64.getDecoder().decode(password), Charset.forName("US-ASCII"));
                return;
            }
        }
        log.debug("Generating user password");
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
        return Base64.getDecoder().decode(secret.getData().get(key));
    }

    /**
     * Creates secret with the data
     * @param data Map with the Secret content
     * @return
     */
    protected Secret createSecret(Map<String, String> data) {
        Secret s = new SecretBuilder()
                .withNewMetadata()
                    .withName(getSecretName())
                    .withNamespace(namespace)
                    .withLabels(labels.toMap())
                .endMetadata()
                .withData(data)
                .build();

        return s;
    }

    /**
     * Generates the name of the User secret based on the username
     *
     * @return
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
     * @return
     */
    public static String getUserName(String username)    {
        return "CN=" + username;
    }

    /**
     * Gets the Username
     *
     * @return
     */
    public String getUserName()    {
        return getUserName(name);
    }

    public String getName() {
        return name;
    }

    /**
     * Generates the name of the USer secret based on the username
     *
     * @return
     */
    public static String getSecretName(String username)    {
        return username;
    }

    /**
     * Gets the name of the User secret
     *
     * @return
     */
    public String getSecretName()    {
        return KafkaUserModel.getSecretName(name);
    }

    /**
     * Sets authentication method
     *
     * @param authentication Authentication method
     */
    public void setAuthentication(KafkaUserAuthentication authentication) {
        this.authentication = authentication;
    }

    public KafkaUserAuthentication getAuthentication() {
        return this.authentication;
    }

    /**
     * Get list of ACL rules for Simple Authorization which should apply to this user
     *
     * @return
     */
    public Set<SimpleAclRule> getSimpleAclRules() {
        return simpleAclRules;
    }

    /**
     * Sets list of ACL rules for Simple authorization
     *
     * @param rules List of ACL rules which should be applied to this user
     */
    public void setSimpleAclRules(List<AclRule> rules) {
        Set<SimpleAclRule> simpleAclRules = new HashSet<SimpleAclRule>();

        for (AclRule rule : rules)  {
            simpleAclRules.add(SimpleAclRule.fromCrd(rule));
        }

        this.simpleAclRules = simpleAclRules;
    }
}
