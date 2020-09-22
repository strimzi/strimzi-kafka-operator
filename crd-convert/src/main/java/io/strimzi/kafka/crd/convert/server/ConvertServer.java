/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.kafka.crd.convert.converter.Converter;
import io.strimzi.kafka.crd.convert.converter.KafkaConverter;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PfxOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.lang.Integer.parseInt;

public class ConvertServer {

    public static final JsonMapper JSON_MAPPER = new JsonMapper();
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String APPLICATION_JSON = "application/json";
    public static final int SC_OK = 200;
    public static final int SC_INTERNAL_SERVER_ERROR = 500;
    public static final String KEYSTORE = "KEYSTORE";
    public static final String KEYSTORE_PASSWORD = "KEYSTORE_PASSWORD";
    public static final String CERTSTORE = "CERTSTORE";
    public static final String TLS_PROTOCOLS = "TLS_PROTOCOLS";
    private static final Logger LOGGER = LogManager.getLogger(ConvertServer.class);
    public static final String PLAIN_PORT = "PLAIN_PORT";
    public static final String TLS_PORT = "TLS_PORT";
    public static final String HOST = "HOST";
    private final Map<String, Binding<?>> bindings;

    public ConvertServer(Map<String, Binding<?>> bindings) {
        this.bindings = bindings;
    }

    private void doConvert(HttpServerRequest request, long startTime, HttpServerResponse response) {
        String contentType = request.headers().get(CONTENT_TYPE);
        if (!APPLICATION_JSON.equals(contentType)) {
            LOGGER.warn("Unexpected request {} {}", CONTENT_TYPE, contentType);
            end(startTime, response, 415, Buffer.buffer()); // unsupported media type
        }
        request.bodyHandler(body -> {
            int sc = SC_INTERNAL_SERVER_ERROR;
            Buffer buffer = null;
            try {
                ConversionReview conversionReview = JSON_MAPPER.readValue(body.getBytes(), ConversionReview.class);
                Request reviewRequest = conversionReview.getRequest();
                ResponseResult result = new ResponseResult();
                List<Object> converted;
                try {
                    converted = convert(reviewRequest);
                    result.setStatus("Success");
                } catch (Exception e) {
                    converted = null;
                    result.setStatus("Failed");
                    result.setMessage(e.getMessage());
                    LOGGER.warn("Error converting {}", reviewRequest, e);
                    response.setStatusCode(SC_INTERNAL_SERVER_ERROR);
                }
                conversionReview.setRequest(null);
                Response reviewResponse = new Response();
                reviewResponse.setUid(reviewRequest.uid);
                reviewResponse.setConvertedObjects(converted);
                reviewResponse.setResult(result);
                conversionReview.setResponse(reviewResponse);
                response.putHeader(CONTENT_TYPE, APPLICATION_JSON);
                buffer = serialize(request, reviewResponse);
                sc = SC_OK;
            } catch (Exception e) {
                LOGGER.error("Error handling request to " + request.path(), e);
                buffer = null;
                sc = SC_INTERNAL_SERVER_ERROR;
            } finally {
                end(startTime, response, sc, buffer);
            }
        });
    }

    private void end(long startTime, HttpServerResponse response, int sc, Buffer buffer) {
        response.setStatusCode(sc);
        response.end(buffer);
        LOGGER.info("Returned {} byte {} response in {}ms", buffer.length(), sc, (System.nanoTime() - startTime) / 1_000_000);
    }

    private static Buffer serialize(HttpServerRequest request, Response reviewResponse) throws JsonProcessingException {
        ObjectMapper mapper;
        if (Boolean.parseBoolean(request.params().get("pretty"))) {
            mapper = JSON_MAPPER.copy().enable(SerializationFeature.INDENT_OUTPUT);
        } else {
            mapper = JSON_MAPPER;
        }
        return Buffer.buffer(mapper.writeValueAsBytes(reviewResponse));
    }

    private void doUnexpectedRequested(HttpServerRequest request,
                                      long startTime,
                                      HttpServerResponse response,
                                      int sc) {
        LOGGER.info("Unexpected request {}, {}", request.method(), request.path());
        response.headers().add("Content-Type", "text/plain");
        end(startTime, response, sc, Buffer.buffer("I support GET /ready, GET /healthy & POST /convert\n"));
    }

    private static void doProbe(HttpServerRequest request, HttpServerResponse response) {
        LOGGER.trace("{}", request.path());
        response.setStatusCode(200);
        response.end();
    }

    private List<Object> convert(Request reviewRequest) {
        String desiredAPIVersion = reviewRequest.getDesiredAPIVersion();
        String desiredGroup = desiredAPIVersion.substring(0, desiredAPIVersion.indexOf('/'));
        ApiVersion desiredVersion = ApiVersion.parse(desiredAPIVersion.substring(desiredAPIVersion.indexOf('/') + 1));
        List<JsonNode> objects = reviewRequest.getObjects();
        List<Object> result = new ArrayList<>(objects.size());
        for (JsonNode obj : objects) {
            String kind = obj.get("kind").asText();
            Binding<?> binding = bindings.get(kind);
            if (binding == null) {
                throw new RuntimeException("Could find binding for kind " + kind);
            }
            try {
                result.add(convert(binding, obj, desiredVersion));
            } catch (RuntimeException | IOException e) {
                throw new RuntimeException("Could not convert object of kind " + kind, e);
            }
        }
        return result;
    }

    private static <T extends HasMetadata> T convert(Binding<T> binding, JsonNode obj, ApiVersion desiredAPIVersion) throws java.io.IOException {
        T cr = JSON_MAPPER.readerFor(binding.crClass).readValue(obj);
        binding.converter.convertTo(cr, desiredAPIVersion);
        return cr;
    }

    private static String getString(String envVar, String defaultValue) {
        String result = System.getenv(envVar);
        if (result == null) {
            result = defaultValue;
        }
        return result;
    }

    private static int getInt(String envVar, int defaultValue) {
        String str = System.getenv(envVar);
        int result;
        if (str == null) {
            result = defaultValue;
        } else {
            try {
                result = parseInt(str);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Env var " + envVar + " is not an int");
            }
        }
        return result;
    }

    public static void main(String[] args) {
        Map<String, Binding<?>> bindings = new HashMap<>();
        bindings.put("Kafka", new Binding<>(new KafkaConverter().crClass(), new KafkaConverter()));

        ConvertServer server = new ConvertServer(bindings);

        String host = getString(HOST, "0.0.0.0");
        int tlsPort = getInt(TLS_PORT, -1);
        int plainPort = getInt(PLAIN_PORT, -1);
        HttpServerOptions options = new HttpServerOptions().setLogActivity(true);
        if (tlsPort > 0 || plainPort > 0) {
            if (plainPort > 0) {
                server.createServer(host, plainPort, options);
            }
            if (tlsPort > 0) {
                server.createServer(host, tlsPort, configureHttpsOptions(options));
            }
        } else {
            exit("Either {} or {} must be specified", TLS_PORT, PLAIN_PORT);
        }
    }

    private static void exit(String message, Object... parameters) {
        LOGGER.error(message, parameters);
        System.exit(1);
    }

    private void createServer(String host, int post, HttpServerOptions options) {
        Vertx.vertx()
            .createHttpServer(options)
            .requestHandler(request -> {
                requestHandler(request);
            })
            .listen(post, host, ar -> {
                if (ar.succeeded()) {
                    LOGGER.debug("Bound {} listener on host {}, port {}",
                            options.isSsl() ? "TLS" : "plain", host, post);
                } else {
                    exit("Error binding {} listener on host {}, port {}",
                            options.isSsl() ? "TLS" : "plain", host, post, ar.cause());
                }
            });
    }

    private void requestHandler(HttpServerRequest request) {
        long startTime = System.nanoTime();
        HttpServerResponse response = request.response();
        String path = request.path().replaceAll("/+$", "");
        switch (path) {
            case "/convert":
                if (HttpMethod.POST.equals(request.method())) {
                    doConvert(request, startTime, response);
                } else {
                    doUnexpectedRequested(request, startTime, response, 405); // method not supported
                }
                break;
            case "/ready":
            case "/healthy":
                if (HttpMethod.GET.equals(request.method())) {
                    doProbe(request, response);
                } else {
                    doUnexpectedRequested(request, startTime, response, 405); // method not supported
                }
                break;
            default:
                doUnexpectedRequested(request, startTime, response, 404); // not found
                break;
        }
    }

    private static HttpServerOptions configureHttpsOptions(HttpServerOptions options) {
        options = new HttpServerOptions(options);
        options.setSsl(true);
        String keystorePath = getString(KEYSTORE, null);
        if (keystorePath == null) {
            LOGGER.warn("No {} specified; running without SSL", KEYSTORE);
        } else {
            options.setSsl(true);
            if (keystorePath.endsWith(".jks")) {
                options.setKeyStoreOptions(new JksOptions().setPath(keystorePath)
                        .setPassword(getString(KEYSTORE_PASSWORD, null)));
            } else if (keystorePath.endsWith(".pfx")) {
                options.setPfxKeyCertOptions(new PfxOptions().setPath(keystorePath)
                        .setPassword(getString(KEYSTORE_PASSWORD, null)));
            } else if (keystorePath.endsWith(".pem")) {
                String certPath = getString(CERTSTORE, null);
                if (certPath == null) {
                    throw new RuntimeException(CERTSTORE + " must be specified if using a PEM keystore");
                }
                options.setPemKeyCertOptions(
                        new PemKeyCertOptions()
                                .setKeyPath(keystorePath)
                                .setCertPath(certPath));
            } else {
                throw new RuntimeException(KEYSTORE + " should be end with one of '.jks', '.pfx' or '.pem'");
            }
        }
        options.getEnabledSecureTransportProtocols().stream()
                .forEach(options::removeEnabledSecureTransportProtocol);
        Arrays.stream(getString(TLS_PROTOCOLS, "TLSv1.2,TLSv1.3").split(" *, *"))
                .forEach(options::addEnabledSecureTransportProtocol);
        return options;
    }

    static class Binding<T extends HasMetadata> {
        private final Class<T> crClass;
        private final Converter<T> converter;

        public Binding(Class<T> crClass, Converter<T> converter) {
            this.crClass = crClass;
            this.converter = converter;
        }
    }
}
