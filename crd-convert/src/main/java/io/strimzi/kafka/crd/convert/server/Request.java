/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.server;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;


class Request {
    String uid;
    String desiredAPIVersion;
    List<JsonNode> objects;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getDesiredAPIVersion() {
        return desiredAPIVersion;
    }

    public void setDesiredAPIVersion(String desiredAPIVersion) {
        this.desiredAPIVersion = desiredAPIVersion;
    }

    public List<JsonNode> getObjects() {
        return objects;
    }

    public void setObjects(List<JsonNode> objects) {
        this.objects = objects;
    }

    @Override
    public String toString() {
        return "Request(" +
                "uid='" + uid + '\'' +
                ", desiredAPIVersion='" + desiredAPIVersion + '\'' +
                ", objects=" + objects +
                ')';
    }
}
