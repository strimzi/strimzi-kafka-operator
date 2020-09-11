/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

public class ApiVersionRange {
    private final ApiVersion from;
    private final ApiVersion to;

    private ApiVersionRange(ApiVersion from, ApiVersion to) {
        // invariant from == null => "all" => null must be null too
        if (from == null && to != null) {
            throw new IllegalArgumentException();
        }
        if (from != null && to != null && from.compareTo(to) > 0) {
            throw new IllegalArgumentException("Bad version range");
        }
        this.from = from;
        this.to = to;
    }

    public static ApiVersionRange parse(String s) {
        if ("all".equals(s)) {
            // all
            return new ApiVersionRange(null, null);
        } else if (ApiVersion.isVersion(s)) {
            // version
            ApiVersion v = ApiVersion.parse(s);
            return new ApiVersionRange(v, v);
        } else if (s.endsWith("+")) {
            // version+
            String s1 = s.substring(0, s.length() - 1);
            if (ApiVersion.isVersion(s1)) {
                ApiVersion v = ApiVersion.parse(s1);
                return new ApiVersionRange(v, null);
            } else {
                throw new IllegalArgumentException();
            }
        } else {
            // version-version
            int index = s.indexOf('-');
            if (index != -1) {
                String from = s.substring(0, index);
                String to = s.substring(index + 1);
                if (ApiVersion.isVersion(from) && ApiVersion.isVersion(to)) {
                    return new ApiVersionRange(ApiVersion.parse(from), ApiVersion.parse(to));
                } else {
                    throw new IllegalArgumentException();
                }
            } else {
                throw new IllegalArgumentException();
            }
        }
    }

    public boolean contains(ApiVersion apiVersion) {
        return from == null || from.compareTo(apiVersion) <= 0 && (to == null || to.compareTo(apiVersion) >= 0);
    }

    public boolean intersects(ApiVersionRange other) {
        if (from == null) {
            return true;
        } else if (to == null) {
            return from.compareTo(other.from) <= 0;
        } else {
            return !(to.compareTo(other.from) < 0 || (other.to != null && from.compareTo(other.to) > 0));
        }
    }
}
