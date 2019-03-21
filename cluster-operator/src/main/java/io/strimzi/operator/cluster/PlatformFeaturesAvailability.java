/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.vertx.core.json.Json;

import java.util.Properties;

public class PlatformFeaturesAvailability {
    private Properties properties;

    static final String MAJORVERSION = "major";
    static final String MINORVERSION = "minor";
    static final String ISOPENSHIFT = "isOpenshift";
    static final String NETWORKPOLICIES = "networkPolicies";

    static final String KUBERNETES1_8 = "1.8";
    static final String KUBERNETES1_9 = "1.9";
    static final String KUBERNETES1_10 = "1.10";
    static final String KUBERNETES1_11 = "1.11";

    public PlatformFeaturesAvailability(boolean isOpenshift, String versionResponse) {
        if (versionResponse == null) {
            throw new IllegalArgumentException("Kubernetes version could not be determined.");
        }
        this.properties = new Properties();
        Properties versionResponseProperties = Json.decodeValue(versionResponse, Properties.class);

        // we need to get versions first
        properties.put(MAJORVERSION, versionResponseProperties.get(MAJORVERSION));
        properties.put(MINORVERSION, versionResponseProperties.get(MINORVERSION));
        properties.put(ISOPENSHIFT, Boolean.toString(isOpenshift));

        // then we can compare
        properties.put(NETWORKPOLICIES, Boolean.toString(this.isEqualOrNewerVersionThan(KUBERNETES1_11)));
    }

    public boolean isOpenshift() {
        return Boolean.parseBoolean(this.properties.getProperty(ISOPENSHIFT));
    }

    public int getMinorVersion() {
        return Integer.parseInt(this.properties.getProperty(MINORVERSION));
    }

    public int getMajorVersion() {
        return Integer.parseInt(this.properties.getProperty(MAJORVERSION));
    }

    public boolean isNetworkPolicyAvailable() {
        return Boolean.parseBoolean(this.properties.getProperty(NETWORKPOLICIES));
    }

    public boolean isEqualOrNewerVersionThan(String version) {
        String[] parts = version.split("\\.");
        if (parts.length < 2) {
            throw new IllegalArgumentException("The Kubernetes version has to have format of X.Y where X and Y are integers.");
        }
        return this.isEqualOrNewerVersionThan(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
    }

    public boolean isEqualOrNewerVersionThan(int otherMajor, int otherMinor) {
        if (otherMajor < this.getMajorVersion()) {
            return true;
        } else if (otherMajor == this.getMajorVersion()) {
            return otherMinor <= this.getMinorVersion();
        } else {
            return false;
        }
    }

}
