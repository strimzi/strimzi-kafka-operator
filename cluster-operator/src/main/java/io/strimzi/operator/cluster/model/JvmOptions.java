/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JvmOptions {

    private String xmx;
    private String xms;
    private Boolean server = false;
    private Map<String, String> xx;

    @JsonProperty("-Xmx")
    public String getXmx() {
        return xmx;
    }

    public void setXmx(String xmx) {
        this.xmx = xmx;
    }

    @JsonProperty("-Xms")
    public String getXms() {
        return xms;
    }

    public void setXms(String xms) {
        this.xms = xms;
    }

    @JsonProperty("-server")
    public Boolean getServer() {
        return server;
    }

    public void setServer(Boolean server) {
        this.server = server;
    }

    @JsonProperty("-XX")
    public Map<String, String> getXx() {
        return xx;
    }

    public void setXx(Map<String, String> xx) {
        this.xx = xx;
    }

    public static JvmOptions fromJson(String json) {
        return JsonUtils.fromJson(json, JvmOptions.class);
    }
}

