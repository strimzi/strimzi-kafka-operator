/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.upgrade;

/**
 * This class contains fields needed for CO upgrade using OLM install type
 */
public class OlmVersionModificationData extends CommonVersionModificationData {
    // OLM specific
    private String fromOlmChannelName;

    public String getFromOlmChannelName() {
        return fromOlmChannelName;
    }

    public void setFromOlmChannelName(String fromOlmChannelName) {
        this.fromOlmChannelName = fromOlmChannelName;
    }

    @Override
    public String toString() {
        return "\n" +
                "OlmVersionModificationData{" +
                super.toString() +
                ", fromOlmChannelName='" + fromOlmChannelName + '\'' +
                "\n}";
    }
}
