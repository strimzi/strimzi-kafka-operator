/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.agent;

import com.sun.net.httpserver.HttpServer;
import io.strimzi.kafka.oauth.common.TokenInfo;
import io.strimzi.kafka.oauth.validator.TokenValidationException;
import io.strimzi.kafka.oauth.validator.TokenValidator;
import jakarta.servlet.http.HttpServletResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.Callback;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServiceAccountAuthenticationHandlerTest {
    private static final String EXTERNAL_CONNECTOR_NAME = "external";
    private static final String INTERNAL_CONNECTOR_NAME = "internal";
    private static final String ALLOWED_USER = "system:serviceaccount:my-namespace:my-cluster-cluster-operator";
    private static final Set<String> ALLOWED_USERS = Set.of(ALLOWED_USER);
    private static final String ISSUER = "https://kubernetes.default.svc.cluster.local";
    private static final String AUDIENCE = "strimzi.io/kafka/my-namespace/my-cluster";
    private static final String KEY_ID = "my-signing-key";
    private static final String KUBERNETES_TOKEN = "my-kubernetes-token";

    @TempDir
    static Path tempDir;

    private static KeyPair signingKey;
    private static HttpServer jwksServer;
    private static String jwksAuthorization;
    private static TokenValidator validator;

    private Server server;
    private int externalPort;
    private int internalPort;

    /**
     * Starts a JWKS endpoint with a single signing key and creates the validator which uses it. This is the same
     * validator as the one used by the Kafka Agent at runtime.
     */
    @BeforeAll
    public static void setUpValidator() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        signingKey = generator.generateKeyPair();

        jwksServer = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        jwksServer.createContext("/openid/v1/jwks", exchange -> {
            jwksAuthorization = exchange.getRequestHeaders().getFirst("Authorization");

            byte[] body = jwks((RSAPublicKey) signingKey.getPublic()).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(HttpServletResponse.SC_OK, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        jwksServer.start();

        Path tokenPath = tempDir.resolve("token");
        Files.writeString(tokenPath, KUBERNETES_TOKEN);

        validator = ServiceAccountAuthenticationHandler.createValidator(
                "http://localhost:" + jwksServer.getAddress().getPort() + "/openid/v1/jwks",
                null,
                ISSUER,
                AUDIENCE,
                tokenPath.toString());
    }

    @AfterAll
    public static void tearDownValidator() {
        if (validator != null) {
            validator.close();
        }

        if (jwksServer != null) {
            jwksServer.stop(0);
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    /**
     * Starts a server with two connectors and with the authentication handler in front of a handler which always
     * returns HTTP 200. Both connectors are bound to a random port on localhost.
     *
     * @param validator Validator which should be used to validate the tokens
     */
    private void startServer(TokenValidator validator) throws Exception {
        server = new Server();

        ServerConnector externalConnector = new ServerConnector(server);
        externalConnector.setName(EXTERNAL_CONNECTOR_NAME);
        externalConnector.setHost("localhost");

        ServerConnector internalConnector = new ServerConnector(server);
        internalConnector.setName(INTERNAL_CONNECTOR_NAME);
        internalConnector.setHost("localhost");

        server.setConnectors(new Connector[] {externalConnector, internalConnector});
        server.setHandler(new ServiceAccountAuthenticationHandler(new OkHandler(), EXTERNAL_CONNECTOR_NAME, validator, ALLOWED_USERS));
        server.start();

        externalPort = externalConnector.getLocalPort();
        internalPort = internalConnector.getLocalPort();
    }

    private HttpResponse<String> get(int port, String authorization) throws IOException, InterruptedException {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v1/broker-state"))
                .GET();

        if (authorization != null) {
            builder.header(HttpHeader.AUTHORIZATION.asString(), authorization);
        }

        return HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }

    private TokenValidator mockValidator(String principal) {
        TokenValidator validator = mock(TokenValidator.class);
        when(validator.validate("my-token")).thenReturn(new TokenInfo("my-token", (String) null, principal, null, 0L, Long.MAX_VALUE));
        return validator;
    }

    @Test
    public void testAllowedUserIsAuthenticated() throws Exception {
        TokenValidator validator = mockValidator(ALLOWED_USER);
        startServer(validator);

        HttpResponse<String> response = get(externalPort, "Bearer my-token");

        assertThat(response.statusCode(), is(HttpServletResponse.SC_OK));
        assertThat(response.body(), is("OK"));
        verify(validator).validate(eq("my-token"));
    }

    @Test
    public void testUserWhichIsNotAllowedIsRejected() throws Exception {
        startServer(mockValidator("system:serviceaccount:my-namespace:my-user"));

        HttpResponse<String> response = get(externalPort, "Bearer my-token");

        assertThat(response.statusCode(), is(HttpServletResponse.SC_FORBIDDEN));
        assertThat(response.body(), is("User is not allowed to access this endpoint"));
    }

    @Test
    public void testMissingAuthorizationHeaderIsRejected() throws Exception {
        startServer(mockValidator(ALLOWED_USER));

        HttpResponse<String> response = get(externalPort, null);

        assertThat(response.statusCode(), is(HttpServletResponse.SC_UNAUTHORIZED));
        assertThat(response.body(), is("Missing Bearer token"));
        assertThat(response.headers().firstValue(HttpHeader.WWW_AUTHENTICATE.asString()).orElse(null), is("Bearer"));
    }

    @Test
    public void testAuthorizationHeaderWithoutBearerTokenIsRejected() throws Exception {
        startServer(mockValidator(ALLOWED_USER));

        HttpResponse<String> response = get(externalPort, "Basic dXNlcjpwYXNzd29yZA==");

        assertThat(response.statusCode(), is(HttpServletResponse.SC_UNAUTHORIZED));
        assertThat(response.body(), is("Missing Bearer token"));
    }

    @Test
    public void testInvalidTokenIsRejected() throws Exception {
        TokenValidator validator = mock(TokenValidator.class);
        when(validator.validate("my-token")).thenThrow(new TokenValidationException("Token validation failed"));
        startServer(validator);

        HttpResponse<String> response = get(externalPort, "Bearer my-token");

        assertThat(response.statusCode(), is(HttpServletResponse.SC_UNAUTHORIZED));
        assertThat(response.body(), is("Invalid Bearer token"));
        assertThat(response.headers().firstValue(HttpHeader.WWW_AUTHENTICATE.asString()).orElse(null), is("Bearer"));
    }

    @Test
    public void testRequestsOnInternalConnectorAreNotAuthenticated() throws Exception {
        TokenValidator validator = mockValidator(ALLOWED_USER);
        startServer(validator);

        HttpResponse<String> response = get(internalPort, null);

        assertThat(response.statusCode(), is(HttpServletResponse.SC_OK));
        assertThat(response.body(), is("OK"));
        verify(validator, never()).validate(anyString());
    }

    @Test
    public void testValidTokenIsAccepted() {
        TokenInfo token = validator.validate(token(signingKey.getPrivate(), KEY_ID, ISSUER, ALLOWED_USER, AUDIENCE, 3600));

        assertThat(token.principal(), is(ALLOWED_USER));
    }

    @Test
    public void testJwksEndpointIsCalledWithTheKubernetesToken() {
        assertThat(jwksAuthorization, is("Bearer " + KUBERNETES_TOKEN));
    }

    @Test
    public void testTokenWithWrongIssuerIsRejected() {
        String token = token(signingKey.getPrivate(), KEY_ID, "https://my-other-issuer.io", ALLOWED_USER, AUDIENCE, 3600);

        assertThrows(TokenValidationException.class, () -> validator.validate(token));
    }

    @Test
    public void testTokenWithWrongAudienceIsRejected() {
        String token = token(signingKey.getPrivate(), KEY_ID, ISSUER, ALLOWED_USER, "strimzi.io/kafka/my-namespace/my-other-cluster", 3600);

        assertThrows(TokenValidationException.class, () -> validator.validate(token));
    }

    @Test
    public void testExpiredTokenIsRejected() {
        String token = token(signingKey.getPrivate(), KEY_ID, ISSUER, ALLOWED_USER, AUDIENCE, -3600);

        assertThrows(TokenValidationException.class, () -> validator.validate(token));
    }

    @Test
    public void testTokenSignedWithAnotherKeyIsRejected() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        String token = token(generator.generateKeyPair().getPrivate(), KEY_ID, ISSUER, ALLOWED_USER, AUDIENCE, 3600);

        assertThrows(TokenValidationException.class, () -> validator.validate(token));
    }

    @Test
    public void testTokenSignedWithUnknownKeyIdIsRejected() {
        String token = token(signingKey.getPrivate(), "my-unknown-key", ISSUER, ALLOWED_USER, AUDIENCE, 3600);

        assertThrows(TokenValidationException.class, () -> validator.validate(token));
    }

    /**
     * Creates the JWKS response with the public part of the signing key
     *
     * @param key   Public key which should be advertised in the JWKS response
     *
     * @return  The JWKS response as a String
     */
    private static String jwks(RSAPublicKey key) {
        return "{\"keys\":[{\"use\":\"sig\",\"kty\":\"RSA\",\"alg\":\"RS256\",\"kid\":\"" + KEY_ID + "\","
                + "\"n\":\"" + base64Url(unsigned(key.getModulus())) + "\","
                + "\"e\":\"" + base64Url(unsigned(key.getPublicExponent())) + "\"}]}";
    }

    /**
     * Creates a signed JWT token which looks like a Kubernetes Service Account token
     *
     * @param key               Private key used to sign the token
     * @param keyId             ID of the key which is set in the token header
     * @param issuer            Issuer of the token
     * @param subject           Subject of the token
     * @param audience          Audience of the token
     * @param expirySeconds     For how many seconds from now is the token valid. Negative value creates an expired token.
     *
     * @return  The signed token
     */
    private static String token(PrivateKey key, String keyId, String issuer, String subject, String audience, long expirySeconds) {
        String header = "{\"alg\":\"RS256\",\"kid\":\"" + keyId + "\"}";
        String payload = "{\"iss\":\"" + issuer + "\",\"sub\":\"" + subject + "\",\"aud\":[\"" + audience + "\"],"
                + "\"exp\":" + ((System.currentTimeMillis() / 1000) + expirySeconds) + "}";
        String signingInput = base64Url(header.getBytes(StandardCharsets.UTF_8)) + "." + base64Url(payload.getBytes(StandardCharsets.UTF_8));

        try {
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(key);
            signature.update(signingInput.getBytes(StandardCharsets.US_ASCII));
            return signingInput + "." + base64Url(signature.sign());
        } catch (Exception e) {
            throw new RuntimeException("Failed to sign the token", e);
        }
    }

    private static String base64Url(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /**
     * Converts the number into its unsigned big-endian representation as required by the JWKS format. The two's
     * complement representation used by BigInteger might have an extra leading zero byte.
     *
     * @param value     Number which should be converted
     *
     * @return  The unsigned big-endian bytes of the number
     */
    private static byte[] unsigned(BigInteger value) {
        byte[] bytes = value.toByteArray();

        if (bytes.length > 1 && bytes[0] == 0) {
            byte[] stripped = new byte[bytes.length - 1];
            System.arraycopy(bytes, 1, stripped, 0, stripped.length);
            return stripped;
        }

        return bytes;
    }

    /**
     * Handler which always returns HTTP 200 and is used to check whether the request passed the authentication
     */
    private static class OkHandler extends Handler.Abstract {
        @Override
        public boolean handle(Request request, Response response, Callback callback) {
            response.setStatus(HttpServletResponse.SC_OK);
            response.write(true, StandardCharsets.UTF_8.encode("OK"), callback);
            return true;
        }
    }
}
