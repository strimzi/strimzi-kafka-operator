/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import java.util.Map;

/**
 * Represents the subject for a certificate
 */
public class Subject {

    private String organizationName;
    private String commonName;
    private Map<String, String> subjectAltNames;

    public String organizationName() {
        return organizationName;
    }

    public void setOrganizationName(String organizationName) {
        this.organizationName = organizationName;
    }

    public String commonName() {
        return commonName;
    }

    public void setCommonName(String commonName) {
        this.commonName = commonName;
    }

    public Map<String, String> subjectAltNames() {
        return subjectAltNames;
    }

    public void setSubjectAltNames(Map<String, String> subjectAltNames) {
        this.subjectAltNames = subjectAltNames;
    }

    @Override
    public String toString() {
        StringBuilder bldr = new StringBuilder();

        if (organizationName != null)   {
            bldr.append(String.format("/O=%s", organizationName));
        }

        if (commonName != null)   {
            bldr.append(String.format("/CN=%s", commonName));
        }

        return bldr.toString();
    }
}
