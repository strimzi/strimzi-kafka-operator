/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.base.OperationSupport;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * This class is a temporary work-around for the fact that Fabric8 doesn't
 * yet support an API for manipulating Kubernetes ClusterRoles, etc.
 * @deprecated This can be removed once support for ClusterRoles and ClusterRoleBindings is in Fabric8.
 */
@Deprecated
public class WorkaroundRbacOperator<T> {

    private final Logger log = LogManager.getLogger(getClass());
    private final Vertx vertx;
    private final OkHttpClient client;
    private final String baseUrl;
    private final String group;
    private final String apiVersion;
    private final String plural;


    public WorkaroundRbacOperator(Vertx vertx, KubernetesClient client, String group, String apiVersion, String plural) {
        this.vertx = vertx;
        baseUrl = client.getMasterUrl().toString();
        if (client.isAdaptable(OkHttpClient.class)) {
            this.client = client.adapt(OkHttpClient.class);
        } else {
            throw new RuntimeException("Could not adapt the client to OkHttpClient");
        }
        this.group = group;
        this.apiVersion = apiVersion;
        this.plural = plural;
    }

    private String urlWithoutName() {
        return baseUrl + "apis/" + group + "/" + apiVersion + "/" + plural;
    }

    private String urlWithName(String name) {
        return urlWithoutName() + "/" + name;
    }

    public Future<Void> reconcile(String name, T resource) {
        Future<Void> result = Future.future();
        vertx.executeBlocking(fut -> {
            try {
                Request getRequest = new Request.Builder().get().url(urlWithName(name)).build();
                int getCode = execute(getRequest, 200, 404);
                if (getCode == 200) {
                    if (resource != null) {
                        // exists and wanted => replace
                        log.debug("Replacing");
                        replace(name, resource);
                    } else {
                        // exists but not wanted => delete
                        log.debug("Deleting");
                        delete(name);
                    }
                } else if (getCode == 404) {
                    if (resource != null) {
                        // does not exists but wanted => create
                        log.debug("Creating");
                        create(name, resource);
                    } else {
                        // does not exist and not wanted => noop
                        log.debug("No-op (deletion requested, but does not exist)");
                    }
                } else {
                    // this should be impossible because of the call to checkStatusCode()
                    throw new IllegalStateException();
                }

                fut.complete();
            } catch (Throwable e) {
                fut.fail(e);
            }
        }, result.completer());

        return result;
    }

    private void delete(String name) {
        Request postRequest = new Request.Builder().delete().url(urlWithName(name)).build();
        execute(postRequest, 200);
    }

    private void replace(String name, T resource) {
        logJson(resource);
        RequestBody postBody = RequestBody.create(OperationSupport.JSON, resource.toString());
        Request postRequest = new Request.Builder().put(postBody).url(urlWithName(name)).build();
        execute(postRequest, 200, 201);
    }

    private int execute(Request request, int... expectedCodes) {
        try {
            String method = request.method();
            log.debug("Making {} request {}", method, request);
            Response response = null;
            try {
                response = client.newCall(request).execute();
            } finally {
                if (response != null) {
                    response.close();
                }
            }
            log.debug("Got {} response {}", method, response);
            final int code = response.code();
            for (int i = 0; i < expectedCodes.length; i++) {
                if (expectedCodes[i] == code) {
                    return code;
                }
            }
            throw new KubernetesClientException("Got unexpected " + request.method() + " status code " + code + ": " + response.message(),
                    code, OperationSupport.createStatus(response));

        } catch (IOException e) {
            throw new KubernetesClientException("Executing request", e);
        }

    }

    private void logJson(T resource) {
        if (log.isDebugEnabled()) {
            try {
                ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
                log.debug(objectMapper.writeValueAsString(objectMapper.readTree(resource.toString())));
            } catch (IOException e) {
                throw new KubernetesClientException("Logging request JSON", e);
            }
        }
    }

    private void create(String name, T resource) {
        logJson(resource);
        RequestBody postBody = RequestBody.create(OperationSupport.JSON, resource.toString());
        Request postRequest = new Request.Builder().post(postBody).url(urlWithoutName()).build();
        execute(postRequest, 200, 201, 202);
    }

}
