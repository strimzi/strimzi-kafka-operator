/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import javax.security.auth.x500.X500Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the subject for a certificate.
 * Can be serialized as JSON.
 */
public class Subject {
    /**
     * Builder class used to build the Subject instance
     */
    public static class Builder {
        private String organizationName;
        private String commonName;
        private Set<String> dnsNames = null;
        private Set<String> ipAddresses = null;

        /**
         * Sets the Common Name
         *
         * @param commonName    Common Name
         *
         * @return  This instance of the Subject builder
         */
        public Builder withCommonName(String commonName) {
            this.commonName = commonName;
            return this;
        }

        /**
         * Sets the Organization Name
         *
         * @param organizationName    Organization Name
         *
         * @return  This instance of the Subject builder
         */
        public Builder withOrganizationName(String organizationName) {
            this.organizationName = organizationName;
            return this;
        }

        /**
         * Adds the name to the certificate subject as SAN. This creates a list with one item and delegates to the
         * addDnsNames method.
         *
         * @param dnsName   DNS name
         *
         * @return  Instance of this builder
         */
        public Builder addDnsName(String dnsName) {
            return addDnsNames(List.of(dnsName));
        }

        /**
         * Adds one or more DNS names which will be used in the certificate subject for the SANs
         *
         * @param newDnsNames   List of DNS names
         *
         * @return  Instance of this builder
         */
        public Builder addDnsNames(List<String> newDnsNames)  {
            if (dnsNames == null) {
                dnsNames = new HashSet<>();
            }

            for (String newDnsName : newDnsNames)   {
                if (!IpAndDnsValidation.isValidDnsNameOrWildcard(newDnsName)) {
                    throw new IllegalArgumentException("Invalid DNS name: " + newDnsName);
                }

                dnsNames.add(newDnsName);
            }

            return this;
        }

        /**
         * Adds the IP address to the list of IP address based SANs. The IP address will be validated to be a valid IPv4
         * or IPv6 address. The IPv6 address will be also normalized into the format used by OpenSSL to make it possible
         * to diff them.
         *
         * @param ip    IP address which should be added
         *
         * @return  The Subject.Builder instance
         */
        public Subject.Builder addIpAddress(String ip) {
            if (ipAddresses == null) {
                ipAddresses = new HashSet<>();
            }

            if (IpAndDnsValidation.isValidIpv4Address(ip)) {
                ipAddresses.add(ip);
            } else if (IpAndDnsValidation.isValidIpv6Address(ip))   {
                ipAddresses.add(IpAndDnsValidation.normalizeIpv6Address(ip));
            } else {
                throw new IllegalArgumentException("Invalid IPv4 or IPv6 address address " + ip);
            }

            return this;
        }

        /**
         * @return  Instance of the Subject class created based on this builder
         */
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

    /**
     * @return  Organization name
     */
    @JsonProperty
    public String organizationName() {
        return organizationName;
    }

    /**
     * @return  Common name
     */
    @JsonProperty
    public String commonName() {
        return commonName;
    }

    /**
     * @return  X500Principal based on this subject
     */
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

    /**
     * @return  Set of DNS names
     */
    @JsonProperty
    public Set<String> dnsNames() {
        return dnsNames;
    }

    /**
     * @return  Set of IP addresses
     */
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

    /**
     * @return  Map with the SANs
     */
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

    /**
     * @return  True if any SANs are present. False otherwise.
     */
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
