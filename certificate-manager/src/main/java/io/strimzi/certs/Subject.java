/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Represents the subject for a certificate
 */
public class Subject {

    static class Builder {
        private String organizationName;
        private String commonName;
        private Map<String, Set<String>> subjectAltNames;
        public Builder withCommonName(String commonName) {
            this.commonName = commonName;
            return this;
        }
        public Builder withOrganizationName(String organizationName) {
            this.organizationName = organizationName;
            return this;
        }
        public Builder withSubjectAlternativeName(String type, String san) {
            if (subjectAltNames == null) {
                subjectAltNames = new HashMap<>();
            }
            subjectAltNames.computeIfAbsent(type, k -> new HashSet<>()).add(san);
            return this;
        }
        public Builder addDnsName(String dnsName) {
            return withSubjectAlternativeName("DNS", dnsName);
        }
        public Builder withIpName(String ip) {
            return withSubjectAlternativeName("IP", ip);
        }
        public Subject build() {
            Map<String, String> san = new HashMap<>();
            if (subjectAltNames != null) {
                for (Map.Entry<String, Set<String>> entry : subjectAltNames.entrySet()) {
                    int i = 0;
                    for (String n : entry.getValue()) {
                        san.put(entry.getKey() + "." + (i++), n);
                    }
                }
            }
            return new Subject(commonName, organizationName, san);
        }

    }

    private String organizationName;
    private String commonName;
    private Map<String, String> subjectAltNames;

    public Subject() {
    }

    private Subject(String commonName, String organizationName, Map<String, String> subjectAltNames) {
        this.organizationName = organizationName;
        this.commonName = commonName;
        this.subjectAltNames = subjectAltNames;
    }

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
