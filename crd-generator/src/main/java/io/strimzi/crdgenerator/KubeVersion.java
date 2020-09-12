/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Short.parseShort;

/**
 * Represents a Kubernetes version number and embeds some knowledge of the features available in
 * different Kubernetes versions.
 */
public class KubeVersion implements Comparable<KubeVersion> {

    public static final KubeVersion v1_11 = new KubeVersion((short) 1, (short) 11);
    public static final KubeVersion v1_16 = new KubeVersion((short) 1, (short) 16);

    private final short major;
    private final short minor;

    private KubeVersion(short major, short minor) {
        this.major = major;
        this.minor = minor;
    }

    public static KubeVersion parse(String version) {
        Pattern pattern = Pattern.compile("([0-9]+).([0-9]+)");
        Matcher matcher = pattern.matcher(version);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid kube version");
        }
        return new KubeVersion(parseShort(matcher.group(1)), parseShort(matcher.group(2)));
    }

    public static VersionRange<KubeVersion> parseRange(String range) {
        return VersionRange.parse(range, KubeVersion::parse);
    }

    @Override
    public int compareTo(KubeVersion o) {
        int cmp = Short.compare(major, o.major);
        if (cmp == 0) {
            cmp = Short.compare(minor, o.minor);
        }
        return cmp;
    }

    public boolean supportsSchemaPerVersion() {
        return this.compareTo(v1_16) >= 0;
    }

    /**
     * Does this version of Kubernetes support the given API version of CustomResourceDefinition?
     * @param crdApiVersion The CustomResourceDefinition API version
     * @return Whether this version of Kube has support
     */
    public boolean supportsCrdApiVersion(ApiVersion crdApiVersion) {
        return crdApiVersion.equals(ApiVersion.V1) ? this.compareTo(v1_16) >= 0 : this.compareTo(v1_11) >= 0;
    }
}
