/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.agent;

import io.strimzi.kafka.oauth.common.FileBasedTokenProvider;
import io.strimzi.kafka.oauth.common.PrincipalExtractor;
import io.strimzi.kafka.oauth.common.SSLUtil;
import io.strimzi.kafka.oauth.common.TokenInfo;
import io.strimzi.kafka.oauth.validator.JWTSignatureValidator;
import io.strimzi.kafka.oauth.validator.TokenValidator;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.handler.ConditionalHandler;
import org.eclipse.jetty.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handler that authenticates the requests using Kubernetes Service Account tokens. The tokens are expected in the
 * Authorization header as a Bearer token. They are validated against the public keys downloaded from the JWKS endpoint
 * of the Kubernetes API server. This uses the same code from the Strimzi OAuth library and the same validation rules as
 * the Kafka brokers use for the Kafka protocol connections.
 *
 * Only the requests arriving through the external connector are authenticated. The requests arriving through the
 * internal connector are passed through because the internal connector is bound to localhost and is used
 * only by the health checks of this Pod. (Jetty does not allow us to configure the handler only on one of the
 * connectors.)
 */
class ServiceAccountAuthenticationHandler extends ConditionalHandler.ElseNext {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceAccountAuthenticationHandler.class);

    private static final String BEARER_PREFIX = "Bearer ";
    // The regular Service Account token that is used to authenticate against the JWKS endpoint of the Kubernetes API server.
    private static final String KUBERNETES_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token";
    // The various timing configurations related to the JWKS keys used to verify the tokens
    private static final int JWKS_REFRESH_SECONDS = 300;
    private static final int JWKS_REFRESH_MIN_PAUSE_SECONDS = 1;
    private static final int JWKS_EXPIRY_SECONDS = 360;
    private static final int CONNECT_TIMEOUT_SECONDS = 10;
    private static final int READ_TIMEOUT_SECONDS = 10;

    private final TokenValidator validator;
    private final Set<String> allowedUsers;

    /**
     * Constructor of the ServiceAccountAuthenticationHandler
     *
     * @param handler           Handler to which the authenticated requests are passed
     * @param connectorName     Name of the connector on which the requests have to be authenticated
     * @param config            Map with Kafka Agent configurations
     */
    ServiceAccountAuthenticationHandler(Handler handler, String connectorName, Map<String, String> config) {
        this(handler,
                connectorName,
                createValidator(config.get("tokenJwksUri"), config.get("tokenJwksCaPath"), config.get("tokenIssuer"), config.get("tokenAudience"), config.getOrDefault("tokenPath", KUBERNETES_TOKEN_PATH)),
                parseAllowedUsers(config.get("tokenAllowedUsers")));
    }

    /**
     * Constructor of the ServiceAccountAuthenticationHandler
     *
     * @param handler           Handler to which the authenticated requests are passed
     * @param connectorName     Name of the connector on which the requests have to be authenticated
     * @param validator         Validator used to validate the tokens
     * @param allowedUsers      Users which are allowed to access the endpoints
     */
    /* test */ ServiceAccountAuthenticationHandler(Handler handler, String connectorName, TokenValidator validator, Set<String> allowedUsers) {
        super(handler);

        this.validator = validator;
        this.allowedUsers = allowedUsers;

        // Only the requests arriving through the configured connector are authenticated. The other requests are passed
        // to the next handler by the onConditionsNotMet method of the ElseNext superclass. This separates the internal
        // and external connectors and makes sure the internal connector is skipped.
        include(new ConnectorPredicate(connectorName));
    }

    @Override
    protected boolean onConditionsMet(Request request, Response response, Callback callback) throws Exception {
        String authorization = request.getHeaders().get(HttpHeader.AUTHORIZATION);

        if (authorization == null || !authorization.startsWith(BEARER_PREFIX)) {
            LOGGER.warn("Request to {} is missing the Bearer token", request.getHttpURI());
            return unauthorized(response, callback, "Missing Bearer token");
        }

        String principal;

        try {
            TokenInfo token = validator.validate(authorization.substring(BEARER_PREFIX.length()).trim());
            principal = token.principal();
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to validate the token of the request to {}", request.getHttpURI(), e);
            return unauthorized(response, callback, "Invalid Bearer token");
        }

        if (!allowedUsers.contains(principal)) {
            LOGGER.warn("User {} is not allowed to access {}", principal, request.getHttpURI());
            response.getHeaders().put(HttpHeader.CONTENT_TYPE, "text/plain; charset=UTF-8");
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            response.write(true, StandardCharsets.UTF_8.encode("User is not allowed to access this endpoint"), callback);
            return true;
        }

        LOGGER.trace("Request to {} was authenticated as user {}", request.getHttpURI(), principal);

        return nextHandler(request, response, callback);
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        validator.close();
    }

    private static boolean unauthorized(Response response, Callback callback, String message) {
        response.getHeaders().put(HttpHeader.CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.getHeaders().put(HttpHeader.WWW_AUTHENTICATE, "Bearer");
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.write(true, StandardCharsets.UTF_8.encode(message), callback);
        return true;
    }

    /**
     * Creates the validator which validates the signature and the claims of the Service Account tokens. The keys used
     * to validate the signatures are downloaded from the JWKS endpoint of the Kubernetes API server and periodically
     * refreshed in the background.
     *
     * @param jwksUri               URI of the JWKS endpoint of the Kubernetes API server
     * @param jwksCaPath            Path to the PEM file with the certificates trusted when connecting to the JWKS
     *                              endpoint. When null, the default trust of the JVM is used.
     * @param issuer                Expected issuer of the tokens
     * @param audience              Expected audience of the tokens
     * @param kubernetesTokenPath   Path to the Service Account token of this Pod which is used to authenticate against
     *                              the JWKS endpoint
     *
     * @return  Token validator
     */
    /* test */ static TokenValidator createValidator(String jwksUri, String jwksCaPath, String issuer, String audience, String kubernetesTokenPath) {
        return new JWTSignatureValidator(
                "kafka-agent", // ID of this validator
                null, // Client ID is not used, we authenticate with the token of this Pod
                null, // Client secret is not used, we authenticate with the token of this Pod
                new FileBasedTokenProvider(kubernetesTokenPath), // The JWKS endpoint of the Kubernetes API server requires authentication
                jwksUri, // The JWKS endpoint of the Kubernetes API server
                SSLUtil.createSSLFactory(jwksCaPath, null, null, "PEM", null), // TLS trust used when connecting to the JWKS endpoint
                null, // Default hostname verifier is used
                new PrincipalExtractor("sub"), // The username is taken from the sub claim
                null, // Groups are not extracted from the token
                null, // Groups are not extracted from the token
                issuer, // Expected issuer of the token
                JWKS_REFRESH_SECONDS,
                JWKS_REFRESH_MIN_PAUSE_SECONDS,
                JWKS_EXPIRY_SECONDS,
                false, // Only the keys marked for signing are used
                false, // Service Account tokens do not have the token type claim
                audience, // Expected audience of the token
                null, // No custom claim check is needed
                CONNECT_TIMEOUT_SECONDS,
                READ_TIMEOUT_SECONDS,
                false, // Metrics are not collected by the Kafka Agent
                false, // The Kafka Agent should not prevent the broker from starting when the keys cannot be downloaded
                false); // The Kubernetes API server does not handle the Accept header well
    }

    /**
     * Parses the comma-separated list of the users which are allowed to access the Kafka Agent endpoints.
     *
     * @param allowedUsers  Comma-separated list of the allowed users
     *
     * @return  Set with the allowed users
     */
    private static Set<String> parseAllowedUsers(String allowedUsers) {
        if (allowedUsers == null) {
            return Set.of();
        }

        return Arrays.stream(allowedUsers.split(","))
                .map(String::trim)
                .filter(user -> !user.isEmpty())
                .collect(Collectors.toSet());
    }
}
