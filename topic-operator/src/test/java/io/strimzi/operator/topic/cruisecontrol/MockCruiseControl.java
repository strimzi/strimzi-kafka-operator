/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.cruisecontrol;

import io.strimzi.operator.common.CruiseControlUtil;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlEndpoints;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;
import io.strimzi.test.TestUtils;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.HttpStatusCode;
import org.mockserver.model.JsonBody;
import org.mockserver.model.MediaType;
import org.mockserver.model.Parameter;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;

import static io.strimzi.operator.topic.TopicOperatorTestUtil.contentFromTextFile;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Cruise Control mock.
 */
public class MockCruiseControl {
    private ClientAndServer server;
    
    /**
     * Sets up and returns a Cruise Control mock server.
     *
     * @param serverPort   The port number the server should listen on.
     * @param tlsKeyFile   File containing the CA key.
     * @param tlsCrtFile   File containing the CA crt.
     * 
     * @return             The mock CruiseControl instance.
     */
    public MockCruiseControl(int serverPort, File tlsKeyFile, File tlsCrtFile) {
        try {
            ConfigurationProperties.logLevel("WARN");
            ConfigurationProperties.certificateAuthorityPrivateKey(tlsKeyFile.getAbsolutePath());
            ConfigurationProperties.certificateAuthorityCertificate(tlsCrtFile.getAbsolutePath());

            String loggingConfiguration = "handlers=org.mockserver.logging.StandardOutConsoleHandler\n" +
                "org.mockserver.logging.StandardOutConsoleHandler.level=WARNING\n" +
                "org.mockserver.logging.StandardOutConsoleHandler.formatter=java.util.logging.SimpleFormatter\n" +
                "java.util.logging.SimpleFormatter.format=%1$tF %1$tT  %3$s  %4$s  %5$s %6$s%n\n" +
                ".level=" + ConfigurationProperties.javaLoggerLogLevel() + "\n" +
                "io.netty.handler.ssl.SslHandler.level=WARNING";
            LogManager.getLogManager().readConfiguration(new ByteArrayInputStream(loggingConfiguration.getBytes(UTF_8)));

            this.server = new ClientAndServer(serverPort);
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
                HttpRequest.request()
                    .withMethod("POST")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(new JsonBody(TestUtils.jsonFromResource("cruise-control/topic-config-success.json")))
                    .withHeader(Header.header("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withDelay(TimeUnit.SECONDS, 0));

        // encryption and authentication enabled
        server
            .when(
                HttpRequest.request()
                    .withMethod("POST")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withHeader(new Header("Authorization",
                        CruiseControlUtil.buildBasicAuthValue(contentFromTextFile(apiUserFile), contentFromTextFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(new JsonBody(TestUtils.jsonFromResource("cruise-control/topic-config-success.json")))
                    .withHeader(Header.header("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withDelay(TimeUnit.SECONDS, 0));

        // encryption only
        server
            .when(
                HttpRequest.request()
                    .withMethod("POST")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withSecure(true))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(new JsonBody(TestUtils.jsonFromResource("cruise-control/topic-config-success.json")))
                    .withHeader(Header.header("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withDelay(TimeUnit.SECONDS, 0));

        // authentication only
        server
            .when(
                HttpRequest.request()
                    .withMethod("POST")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withHeader(new Header("Authorization",
                        CruiseControlUtil.buildBasicAuthValue(contentFromTextFile(apiUserFile), contentFromTextFile(apiPassFile)))))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(new JsonBody(TestUtils.jsonFromResource("cruise-control/topic-config-success.json")))
                    .withHeader(Header.header("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectTopicConfigErrorResponse(File apiUserFile, File apiPassFile) {
        server
            .when(
                HttpRequest.request()
                    .withMethod("POST")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withHeader(new Header("Authorization",
                        CruiseControlUtil.buildBasicAuthValue(contentFromTextFile(apiUserFile), contentFromTextFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.INTERNAL_SERVER_ERROR_500.code())
                    .withBody(new JsonBody(TestUtils.jsonFromResource("cruise-control/topic-config-failure.json")))
                    .withHeader(Header.header("User-Task-ID", "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectTopicConfigRequestTimeout(File apiUserFile, File apiPassFile) {
        server
            .when(
                HttpRequest.request()
                    .withMethod("POST")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withHeader(new Header("Authorization",
                        CruiseControlUtil.buildBasicAuthValue(contentFromTextFile(apiUserFile), contentFromTextFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.REQUEST_TIMEOUT_408.code())
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectTopicConfigRequestUnauthorized(File apiUserFile, File apiPassFile) {
        server
            .when(
                HttpRequest.request()
                    .withMethod("POST")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.SKIP_RACK_AWARENESS_CHECK.toString(), "true|false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.DRY_RUN.toString(), "false"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.TOPIC_CONFIGURATION.toString())
                    .withContentType(MediaType.APPLICATION_JSON)
                    .withHeader(new Header("Authorization",
                        CruiseControlUtil.buildBasicAuthValue(contentFromTextFile(apiUserFile), contentFromTextFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.UNAUTHORIZED_401.code())
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectUserTasksSuccessResponse(File apiUserFile, File apiPassFile) {
        // encryption and authentication disabled
        server
            .when(
                HttpRequest.request()
                    .withMethod("GET")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString()))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(new JsonBody(TestUtils.jsonFromResource("cruise-control/user-tasks-success.json")))
                    .withDelay(TimeUnit.SECONDS, 0));

        // encryption and authentication enabled
        server
            .when(
                HttpRequest.request()
                    .withMethod("GET")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                    .withHeader(new Header("Authorization",
                        CruiseControlUtil.buildBasicAuthValue(contentFromTextFile(apiUserFile), contentFromTextFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(new JsonBody(TestUtils.jsonFromResource("cruise-control/user-tasks-success.json")))
                    .withDelay(TimeUnit.SECONDS, 0));

        // encryption only
        server
            .when(
                HttpRequest.request()
                    .withMethod("GET")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                    .withSecure(true))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(new JsonBody(TestUtils.jsonFromResource("cruise-control/user-tasks-success.json")))
                    .withDelay(TimeUnit.SECONDS, 0));

        // authentication only
        server
            .when(
                HttpRequest.request()
                    .withMethod("GET")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                    .withHeader(new Header("Authorization",
                        CruiseControlUtil.buildBasicAuthValue(contentFromTextFile(apiUserFile), contentFromTextFile(apiPassFile)))))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.OK_200.code())
                    .withBody(new JsonBody(TestUtils.jsonFromResource("cruise-control/user-tasks-success.json")))
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectUserTasksErrorResponse(File apiUserFile, File apiPassFile) {
        server
            .when(
                HttpRequest.request()
                    .withMethod("GET")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                    .withHeader(new Header("Authorization",
                        CruiseControlUtil.buildBasicAuthValue(contentFromTextFile(apiUserFile), contentFromTextFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.INTERNAL_SERVER_ERROR_500.code())
                    .withBody(new JsonBody(TestUtils.jsonFromResource("cruise-control/user-tasks-failure.json")))
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectUserTasksRequestTimeout(File apiUserFile, File apiPassFile) {
        server
            .when(
                HttpRequest.request()
                    .withMethod("GET")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                    .withHeader(new Header("Authorization",
                        CruiseControlUtil.buildBasicAuthValue(contentFromTextFile(apiUserFile), contentFromTextFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.REQUEST_TIMEOUT_408.code())
                    .withDelay(TimeUnit.SECONDS, 0));
    }

    public void expectUserTasksRequestUnauthorized(File apiUserFile, File apiPassFile) {
        server
            .when(
                HttpRequest.request()
                    .withMethod("GET")
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.USER_TASK_IDS.toString(), "8911ca89-351f-888-8d0f-9aade00e098h"))
                    .withQueryStringParameter(Parameter.param(CruiseControlParameters.JSON.toString(), "true"))
                    .withPath(CruiseControlEndpoints.USER_TASKS.toString())
                    .withHeader(new Header("Authorization",
                        CruiseControlUtil.buildBasicAuthValue(contentFromTextFile(apiUserFile), contentFromTextFile(apiPassFile))))
                    .withSecure(true))
            .respond(
                HttpResponse.response()
                    .withStatusCode(HttpStatusCode.UNAUTHORIZED_401.code())
                    .withDelay(TimeUnit.SECONDS, 0));
    }
}
