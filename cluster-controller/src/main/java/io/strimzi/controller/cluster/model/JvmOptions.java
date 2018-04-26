/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.controller.cluster.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JvmOptions {

    private String xmx;
    private String xms;

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

    public static JvmOptions fromJson(String json) {
        return JsonUtils.fromJson(json, JvmOptions.class);
    }
}

