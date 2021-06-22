/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import javax.security.auth.x500.X500Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the subject for a certificate.
 * Can be serialized as JSON.
 */
public class Subject {

    public static class Builder {
        private static final Pattern IPV4_ADDRESS = Pattern.compile("[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}");
        private static final Pattern DNS_NAME = Pattern.compile("^(" +
                // a single char dns name
                "[a-zA-Z0-9]|" +
                // can't begin or end with -                followed by more labels of same
                "[a-zA-Z0-9][a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])(\\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9]))*$");
        private String organizationName;
        private String commonName;
        private Set<String> dnsNames = null;
        private Set<String> ipAddresses = null;

        public Builder withCommonName(String commonName) {
            this.commonName = commonName;
            return this;
        }
        public Builder withOrganizationName(String organizationName) {
            this.organizationName = organizationName;
            return this;
        }
        public Builder addDnsName(String dnsName) {
            if (!isValidDnsName(dnsName)) {
                throw new IllegalArgumentException("Invalid DNS name: " + dnsName);
            }
            if (dnsNames == null) {
                dnsNames = new HashSet<>();
            }
            dnsNames.add(dnsName);
            return this;
        }

        public boolean isValidDnsName(String dnsName) {
            return dnsName.length() <= 255
                    && (DNS_NAME.matcher(dnsName).matches()
                    || (dnsName.startsWith("*.") && DNS_NAME.matcher(dnsName.substring(2)).matches()));
        }

        public Builder addIpAddress(String ip) {
            if (!isValidIpv4Address(ip)) {
                throw new IllegalArgumentException("Invalid IPv4 address");
            }
            if (ipAddresses == null) {
                ipAddresses = new HashSet<>();
            }
            ipAddresses.add(ip);
            return this;
        }

        public boolean isValidIpv4Address(String ip) {
            boolean matches = IPV4_ADDRESS.matcher(ip).matches();
            if (matches) {
                String[] split = ip.split("\\.");
                for (String num : split) {
                    int i = Integer.parseInt(num);
                    if (i > 255) {
                        return false;
                    }
                }
            }
            return matches;
        }

        public Subject build() {
            return new Subject(commonName, organizationName, dnsNames, ipAddresses);
        }

    }

    private final String organizationName;
    private final String commonName;
    private final Set<String> dnsNames;
    private final Set<String> ipAddresses;

    @JsonCreator
    private Subject(
            @JsonProperty("commonName") String commonName,
            @JsonProperty("organizationName") String organizationName,
            @JsonProperty("dnsNames") Set<String> dnsNames,
            @JsonProperty("ipAddresses") Set<String> ipAddresses) {
        this.organizationName = organizationName;
        this.commonName = commonName;
        this.dnsNames = dnsNames == null ? Set.of() : Collections.unmodifiableSet(dnsNames);
        this.ipAddresses = ipAddresses == null ? Set.of() : Collections.unmodifiableSet(ipAddresses);
    }

    @JsonProperty
    public String organizationName() {
        return organizationName;
    }

    @JsonProperty
    public String commonName() {
        return commonName;
    }

    public X500Principal principal() {
        if (commonName != null) {
            if (organizationName != null) {
                return new X500Principal("CN=" + commonName() + ", O=" + organizationName());
            } else {
                return new X500Principal("CN=" + commonName());
            }
        } else {
            if (organizationName != null) {
                return new X500Principal("O=" + organizationName());
            } else {
                return null;
            }
        }
    }

    @JsonProperty
    public Set<String> dnsNames() {
        return dnsNames;
    }

    @JsonProperty
    public Set<String> ipAddresses() {
        return ipAddresses;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Subject subject = (Subject) o;
        return Objects.equals(organizationName, subject.organizationName) &&
                Objects.equals(commonName, subject.commonName) &&
                Objects.equals(dnsNames, subject.dnsNames) &&
                Objects.equals(ipAddresses, subject.ipAddresses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(organizationName, commonName, dnsNames, ipAddresses);
    }

    @Override
    public String toString() {
        return "Subject(" +
                "organizationName='" + organizationName + '\'' +
                ", commonName='" + commonName + '\'' +
                ", dnsNames=" + dnsNames +
                ", ipAddresses=" + ipAddresses +
                ')';
    }

    public Map<String, String> subjectAltNames() {
        Map<String, String> san = new HashMap<>();
        int i = 0;
        for (String name : dnsNames()) {
            san.put("DNS." + (i++), name);
        }
        i = 0;
        for (String ip : ipAddresses()) {
            san.put("IP." + (i++), ip);
        }
        return san;
    }

    public boolean hasSubjectAltNames() {
        return !dnsNames().isEmpty() || !ipAddresses().isEmpty();
    }

    /**
     * @return The DN in the format understood by {@code openssl}.
     */
    public String opensslDn() {
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
