/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.annotations;


import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Short.parseShort;

/**
 * Represents the version of a Kubernetes API, for example {@code v1alpha1} or {@code v2}.
 * These version numbers are comparable, so {@code v1alpha1 < v1beta1 < v1 < v2alpha1} etc.
 */
public class ApiVersion implements Comparable<ApiVersion> {
    /**
     * Describes the API stability - alpha, beta, or stable
     */
    enum Stability {
        ALPHA,
        BETA,
        STABLE
    }

    /**
     * API version pattern
     */
    private static final Pattern PATTERN = Pattern.compile("v([0-9]+)((alpha|beta)([0-9]+))?");

    /**
     * API version v1alpha1
     */
    public static final ApiVersion V1ALPHA1 = parse("v1alpha1");

    /**
     * API version v1beta1
     */
    public static final ApiVersion V1BETA1 = parse("v1beta1");

    /**
     * API version v1beta2
     */
    public static final ApiVersion V1BETA2 = parse("v1beta2");

    /**
     * API version v1
     */
    public static final ApiVersion V1 = parse("v1");

    /**
     * Version range indicating v1beta2 and higher
     */
    public static final VersionRange<ApiVersion> V1BETA2_PLUS = parseRange("v1beta2+");

    /**
     * Version range indicating v1beta1 and higher
     */
    public static final VersionRange<ApiVersion> V1BETA1_PLUS = parseRange("v1beta1+");

    private final short major;
    private final Stability stability;
    private final short minor;

    /**
     * Constructs a Kubernetes API version object
     *
     * @param major         Major version
     * @param stability     Stability (alpha, beta, ...)
     * @param minor         Minor version
     */
    public ApiVersion(short major, Stability stability, short minor) {
        if (major < 0 || minor < 0) {
            throw new RuntimeException();
        }
        this.major = major;
        this.stability = stability;
        this.minor = minor;
    }

    private static Matcher matcher(String apiVersion) {
        return PATTERN.matcher(apiVersion);
    }

    /**
     * Checks whether the String is a valid Kubernetes API version
     *
     * @param apiVersion    String with the API version
     *
     * @return      True if the String is an valid Kubernetes API version. False otherwise
     */
    private static boolean isVersion(String apiVersion) {
        return matcher(apiVersion).matches();
    }

    /**
     * Parse an Kubernetes APi version form String to ApiVersion instance
     *
     * @param apiVersion    String with the Kubernetes API version
     *
     * @return      ApiVersion instance created from the String
     */
    public static ApiVersion parse(String apiVersion) {
        Matcher matcher = matcher(apiVersion);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid version " + apiVersion);
        }
        short major = parseShort(matcher.group(1));
        Stability stability;
        short minor;
        String alphaBeta = matcher.group(3);
        if (matcher.groupCount() > 1 && alphaBeta != null) {
            if ("alpha".equals(alphaBeta)) {
                stability = Stability.ALPHA;
            } else if ("beta".equals(alphaBeta)) {
                stability = Stability.BETA;
            } else {
                throw new IllegalStateException(alphaBeta);
            }
            minor = parseShort(matcher.group(4));
        } else {
            stability = Stability.STABLE;
            minor = 0;
        }
        return new ApiVersion(major, stability, minor);
    }

    /**
     * Parses a range of Kubernetes API versions from String into VersionRange instance
     *
     * @param s     String with the API version range which will be parsed
     *
     * @return      Instance of the VersionRange object matching the range passed as an argument
     */
    public static VersionRange<ApiVersion> parseRange(String s) {
        return VersionRange.parse(s, new VersionRange.VersionParser<ApiVersion>() {
            @Override
            public ApiVersion parse(String version) throws IllegalArgumentException {
                return ApiVersion.parse(version);
            }

            @Override
            public boolean isValid(String version) {
                return ApiVersion.isVersion(version);
            }
        });
    }

    @Override
    public int compareTo(ApiVersion o) {
        int cmp = Integer.compare(major, o.major);
        if (cmp == 0) {
            cmp = stability.compareTo(o.stability);
        }
        if (cmp == 0) {
            cmp = Integer.compare(minor, o.minor);
        }
        return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApiVersion that = (ApiVersion) o;
        return major == that.major &&
                stability == that.stability &&
                minor == that.minor;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, stability, minor);
    }

    @Override
    public String toString() {
        return "v" + major + (stability == Stability.ALPHA ? "alpha" : stability == Stability.BETA ? "beta" : "")
                + (stability == Stability.STABLE ? "" : Integer.toString(minor));
    }

}
