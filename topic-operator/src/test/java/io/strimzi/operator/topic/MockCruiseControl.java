/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpStatusCode;
import org.mockserver.model.JsonBody;
import org.mockserver.model.MediaType;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockserver.configuration.ConfigurationProperties.javaLoggerLogLevel;
import static org.mockserver.model.Header.header;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.Parameter.param;

/**
 * Mock Cruise Control for topic_configuration and user_tasks endpoints.
 */
public class MockCruiseControl {
    private ClientAndServer server;

    /**
     * Sets up and returns the Cruise Control MockSever.
     *
     * @param port The port number the MockServer instance should listen on.
     * @param port The port number the mock server instance should listen on.
     * @param tlsKeyFile File containing the CA key.
     * @param tlsCrtFile File containing the CA crt.
     *                   
     * @return The mock CruiseControl instance.
     */
    public MockCruiseControl(int port, File tlsKeyFile, File tlsCrtFile)  {
        try {
            ConfigurationProperties.logLevel("WARN");
            ConfigurationProperties.certificateAuthorityPrivateKey(tlsKeyFile.getAbsolutePath());
            ConfigurationProperties.certificateAuthorityCertificate(tlsCrtFile.getAbsolutePath());

            String loggingConfiguration = "" +
                "handlers=org.mockserver.logging.StandardOutConsoleHandler\n" +
                "org.mockserver.logging.StandardOutConsoleHandler.level=WARNING\n" +
                "org.mockserver.logging.StandardOutConsoleHandler.formatter=java.util.logging.SimpleFormatter\n" +
                "java.util.logging.SimpleFormatter.format=%1$tF %1$tT  %3$s  %4$s  %5$s %6$s%n\n" +
                ".level=" + javaLoggerLogLevel() + "\n" +
                "io.netty.handler.ssl.SslHandler.level=WARNING";
            LogManager.getLogManager().readConfiguration(new ByteArrayInputStream(loggingConfiguration.getBytes(UTF_8)));

            this.server = new ClientAndServer(port);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void reset() {
        server.reset();
    }

    public void stop() {
        server.stop();
    }
    
    public boolean isRunning() {
        return server.isRunning();
    }

    public void expectTopicConfigSuccessResponse(File apiUserFile, File apiPassFile) {
        // encryption and authentication disabled
        server
            .when(
                request()
                    .withMethod("POST")
                    .withQueryStringParameter(param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(jsonFromResource("cruise-control/topic-config-success.json"))
                    .withHeader(header("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withDelay(TimeUnit.SECONDS, 0));

        // encryption and authentication enabled
        server
            .when(
                request()
                    .withMethod("POST")
                    .withQueryStringParameter(param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withHeader(new Header("Authorization", TopicOperatorUtil.buildBasicAuthValue(stringFromFile(apiUserFile), stringFromFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(jsonFromResource("cruise-control/topic-config-success.json"))
                    .withHeader(header("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withDelay(TimeUnit.SECONDS, 0));

        // encryption only
        server
            .when(
                request()
                    .withMethod("POST")
                    .withQueryStringParameter(param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withSecure(true))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(jsonFromResource("cruise-control/topic-config-success.json"))
                    .withHeader(header("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withDelay(TimeUnit.SECONDS, 0));

        // authentication only
        server
            .when(
                request()
                    .withMethod("POST")
                    .withQueryStringParameter(param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withHeader(new Header("Authorization", TopicOperatorUtil.buildBasicAuthValue(stringFromFile(apiUserFile), stringFromFile(apiPassFile)))))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(jsonFromResource("cruise-control/topic-config-success.json"))
                    .withHeader(header("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectTopicConfigErrorResponse(File apiUserFile, File apiPassFile) {
        server
            .when(
                request()
                    .withMethod("POST")
                    .withQueryStringParameter(param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withHeader(new Header("Authorization", TopicOperatorUtil.buildBasicAuthValue(stringFromFile(apiUserFile), stringFromFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.INTERNAL_SERVER_ERROR_500.code())
                    .withBody(jsonFromResource("cruise-control/topic-config-failure.json"))
                    .withHeader(header("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectTopicConfigRequestTimeout(File apiUserFile, File apiPassFile) {
        server
            .when(
                request()
                    .withMethod("POST")
                    .withQueryStringParameter(param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withHeader(new Header("Authorization", TopicOperatorUtil.buildBasicAuthValue(stringFromFile(apiUserFile), stringFromFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.REQUEST_TIMEOUT_408.code())
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectTopicConfigRequestUnauthorized(File apiUserFile, File apiPassFile) {
        server
            .when(
                request()
                    .withMethod("POST")
                    .withQueryStringParameter(param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withHeader(new Header("Authorization", TopicOperatorUtil.buildBasicAuthValue(stringFromFile(apiUserFile), stringFromFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.UNAUTHORIZED_401.code())
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectUserTasksSuccessResponse(File apiUserFile, File apiPassFile) {
        // encryption and authentication disabled
        server
            .when(
                request()
                    .withMethod("GET")
                    .withQueryStringParameter(param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString()))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(jsonFromResource("cruise-control/user-tasks-success.json"))
                    .withDelay(TimeUnit.SECONDS, 0));
        
        // encryption and authentication enabled
        server
            .when(
                request()
                    .withMethod("GET")
                    .withQueryStringParameter(param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                    .withHeader(new Header("Authorization", TopicOperatorUtil.buildBasicAuthValue(stringFromFile(apiUserFile), stringFromFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(jsonFromResource("cruise-control/user-tasks-success.json"))
                    .withDelay(TimeUnit.SECONDS, 0));

        // encryption only
        server
            .when(
                request()
                    .withMethod("GET")
                    .withQueryStringParameter(param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                    .withSecure(true))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(jsonFromResource("cruise-control/user-tasks-success.json"))
                    .withDelay(TimeUnit.SECONDS, 0));

        // authentication only
        server
            .when(
                request()
                    .withMethod("GET")
                    .withQueryStringParameter(param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                    .withHeader(new Header("Authorization", TopicOperatorUtil.buildBasicAuthValue(stringFromFile(apiUserFile), stringFromFile(apiPassFile)))))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(jsonFromResource("cruise-control/user-tasks-success.json"))
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectUserTasksErrorResponse(File apiUserFile, File apiPassFile) {
        server
            .when(
                request()
                    .withMethod("GET")
                    .withQueryStringParameter(param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                    .withHeader(new Header("Authorization", TopicOperatorUtil.buildBasicAuthValue(stringFromFile(apiUserFile), stringFromFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.INTERNAL_SERVER_ERROR_500.code())
                    .withBody(jsonFromResource("cruise-control/user-tasks-failure.json"))
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectUserTasksRequestTimeout(File apiUserFile, File apiPassFile) {
        server
            .when(
                request()
                    .withMethod("GET")
                    .withQueryStringParameter(param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                    .withHeader(new Header("Authorization", TopicOperatorUtil.buildBasicAuthValue(stringFromFile(apiUserFile), stringFromFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.REQUEST_TIMEOUT_408.code())
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectUserTasksRequestUnauthorized(File apiUserFile, File apiPassFile) {
        server
            .when(
                request()
                    .withMethod("GET")
                    .withQueryStringParameter(param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                    .withHeader(new Header("Authorization", TopicOperatorUtil.buildBasicAuthValue(stringFromFile(apiUserFile), stringFromFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                response()
                    .withStatusCode(HttpStatusCode.UNAUTHORIZED_401.code())
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    private static String stringFromFile(File filePath) {
        try {
            URI resourceURI = Objects.requireNonNull(filePath).toURI();
            Optional<String> content = Files.lines(Paths.get(resourceURI), UTF_8).reduce((x, y) -> x + y);
            if (content.isEmpty()) {
                throw new IOException(format("File %s was empty", filePath.getAbsolutePath()));
            }
            return content.get();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static JsonBody jsonFromResource(String resourcePath) {
        try {
            URI resourceURI = Objects.requireNonNull(TopicOperatorTestUtil.class.getClassLoader().getResource(resourcePath)).toURI();
            Optional<String> content = Files.lines(Paths.get(resourceURI), UTF_8).reduce((x, y) -> x + y);
            if (content.isEmpty()) {
                throw new IOException(format("File %s from resources was empty", resourcePath));
            }
            return new JsonBody(content.get());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
