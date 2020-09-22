/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.server;

import java.util.List;

class Response {
    String uid;
    ResponseResult result;
    List<Object> convertedObjects;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public ResponseResult getResult() {
        return result;
    }

    public void setResult(ResponseResult result) {
        this.result = result;
    }

    public List<Object> getConvertedObjects() {
        return convertedObjects;
    }

    public void setConvertedObjects(List<Object> convertedObjects) {
        this.convertedObjects = convertedObjects;
    }
}
