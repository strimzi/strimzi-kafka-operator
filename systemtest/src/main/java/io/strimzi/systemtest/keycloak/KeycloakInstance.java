/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.keycloak;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Pattern;

public class KeycloakInstance {

    private static final Logger LOGGER = LogManager.getLogger(KeycloakInstance.class);
    public static final String KEYCLOAK_SECRET_NAME = "example-tls-secret";
    public static final String KEYCLOAK_SECRET_CERT = "tls.crt";

    private final Pattern keystorePattern = Pattern.compile("<tls>\\s*<key-stores>\\s*<key-store name=\"kcKeyStore\">\\s*<credential-reference clear-text=\".*\"\\/>");
    private final Pattern keystorePasswordPattern = Pattern.compile("\\\".*\\\"");
    private final int jwksExpireSeconds = 500;
    private final int jwksRefreshSeconds = 400;
    private final String username;
    private final String password;
    private final String namespace;
    private final String httpsUri;
    private final String httpUri;
    private final String userNameClaim;

    private String validIssuerUri;
    private String jwksEndpointUri;
    private String oauthTokenEndpointUri;
    private String introspectionEndpointUri;

    public KeycloakInstance(String namespace, String password, String username) {

        this.username = username;
        this.password = password;
        this.namespace = namespace;
        this.httpsUri = "keycloak-service." + namespace + ".svc.cluster.local:8443";
        this.httpUri = "keycloak-discovery." + namespace + ".svc.cluster.local:9595";
        this.validIssuerUri = "https://" + httpsUri + "/realms/internal";
        this.jwksEndpointUri = "https://" + httpsUri + "/realms/internal/protocol/openid-connect/certs";
        this.oauthTokenEndpointUri = "https://" + httpsUri + "/realms/internal/protocol/openid-connect/token";
        this.introspectionEndpointUri = "https://" + httpsUri + "/realms/internal/protocol/openid-connect/token/introspect";
        this.userNameClaim = "preferred_username";
    }

    public void setRealm(String realmName, boolean tlsEnabled) {
        LOGGER.info("Replacing validIssuerUri: {} to pointing to {} realm", validIssuerUri, realmName);
        LOGGER.info("Replacing jwksEndpointUri: {} to pointing to {} realm", jwksEndpointUri, realmName);
        LOGGER.info("Replacing oauthTokenEndpointUri: {} to pointing to {} realm", oauthTokenEndpointUri, realmName);

        if (tlsEnabled) {
            LOGGER.info("Using HTTPS endpoints");
            validIssuerUri = "https://" + httpsUri + "/realms/" + realmName;
            jwksEndpointUri = "https://" + httpsUri + "/realms/" + realmName + "/protocol/openid-connect/certs";
            oauthTokenEndpointUri = "https://" + httpsUri + "/realms/" + realmName + "/protocol/openid-connect/token";
        } else {
            LOGGER.info("Using HTTP endpoints");
            validIssuerUri = "http://" + httpUri + "/realms/" + realmName;
            jwksEndpointUri = "http://" + httpUri + "/realms/" + realmName + "/protocol/openid-connect/certs";
            oauthTokenEndpointUri = "http://" + httpUri + "/realms/" + realmName + "/protocol/openid-connect/token";
        }
    }

    public String getUsername() {
        return username;
    }
    public String getPassword() {
        return password;
    }
    public String getNamespace() {
        return namespace;
    }
    public String getHttpsUri() {
        return httpsUri;
    }
    public String getValidIssuerUri() {
        return validIssuerUri;
    }
    public String getJwksEndpointUri() {
        return jwksEndpointUri;
    }
    public String getOauthTokenEndpointUri() {
        return oauthTokenEndpointUri;
    }
    public String getIntrospectionEndpointUri() {
        return introspectionEndpointUri;
    }
    public void setIntrospectionEndpointUri(String introspectionEndpointUri) {
        this.introspectionEndpointUri = introspectionEndpointUri;
    }
    public String getUserNameClaim() {
        return userNameClaim;
    }
    public int getJwksExpireSeconds() {
        return jwksExpireSeconds;
    }
    public int getJwksRefreshSeconds() {
        return jwksRefreshSeconds;
    }

    @Override
    public String toString() {
        return "KeycloakInstance{" +
            "jwksExpireSeconds=" + jwksExpireSeconds +
            ", jwksRefreshSeconds=" + jwksRefreshSeconds +
            ", username='" + username + '\'' +
            ", password='" + password + '\'' +
            ", httpsUri='" + httpsUri + '\'' +
            ", httpUri='" + httpUri + '\'' +
            ", validIssuerUri='" + validIssuerUri + '\'' +
            ", jwksEndpointUri='" + jwksEndpointUri + '\'' +
            ", oauthTokenEndpointUri='" + oauthTokenEndpointUri + '\'' +
            ", introspectionEndpointUri='" + introspectionEndpointUri + '\'' +
            ", userNameClaim='" + userNameClaim + '\'' +
            ", keystorePattern=" + keystorePattern +
            ", keystorePasswordPattern=" + keystorePasswordPattern +
            '}';
    }
}
