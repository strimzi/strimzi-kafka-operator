/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.annotations;

import java.util.Objects;

/**
 * Represents a range over some comparable version type.
 * Ranges can:
 * <ol>
 *     <li>be Empty</li>
 *     <li>consist of a single version</li>
 *     <li>consist of an inclusive lower bound and no upper bound</li>
 *     <li>consist of inclusive lower and upper bounds</li>
 * </ol>
 * It is possible to test {@linkplain #contains(Comparable) inclusion} of a given version in a version range
 * and whether two version ranges possibly {@linkplain #intersects(VersionRange) intersect}.
 * @param <Version> The type of the version. This class assumes that older versions are less than newer versions and
 *                 {@link Comparable#compareTo(Object)} is consistent with {@link Object#equals(Object)}.
 */
public class VersionRange<Version extends Comparable<Version>> {

    private static final VersionRange EMPTY = new VersionRange<>(null, null);
    private final Version from;
    private final Version to;

    /**
     * @return The empty version range.
     * @param <Version> The type of version range.
     */
    @SuppressWarnings("unchecked")
    public static <Version extends Comparable<Version>> VersionRange<Version> empty() {
        return EMPTY;
    }

    public static <Version extends Comparable<Version>> VersionRange<Version> all() {
        return new VersionRange<>(null, null);
    }

    public boolean isEmpty() {
        return this == EMPTY;
    }

    public boolean isAll() {
        return this.equals(all());
    }

    private VersionRange(Version from, Version to) {
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

    /**
     * Abstracts the parsing of a version.
     * @param <Version>
     */
    interface VersionParser<Version extends Comparable<Version>> {
        Version parse(String version) throws IllegalArgumentException;
        default boolean isValid(String version) {
            try {
                parse(version);
                return true;
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
    }

    /**
     * Parse a version range specified as:
     * <ul>
     *     <li>{@code empty} -- matches no versions</li>
     *     <li>{@code all} -- matches all version</li>
     *     <li>{@code $version} -- matches precisely the given {@code version}.</li>
     *     <li>{@code $from+} -- matches all versions >= the given {@code from} version.</li>
     *     <li>{@code $from-$to} -- matches all versions between the given {@code from} version and {@code to} versions, inclusive.</li>
     * </ul>
     * <p>Obviously this makes assumptions about what constitutes valid syntax for versions, e.g. "empty" cannot be a valid version</p>
     * @param versionRange
     * @param parser A parser for versions.
     * @param <Version>
     * @return
     */
    static <Version extends Comparable<Version>> VersionRange<Version> parse(String versionRange, VersionParser<Version> parser) {
        if ("empty".equals(versionRange)) {
            return empty();
        } else if ("all".equals(versionRange)) {
            // all
            return all();
        } else if (parser.isValid(versionRange)) {
            // version
            Version v = parser.parse(versionRange);
            return new VersionRange<>(v, v);
        } else if (versionRange.endsWith("+")) {
            // version+
            String s1 = versionRange.substring(0, versionRange.length() - 1);
            if (parser.isValid(s1)) {
                Version v = parser.parse(s1);
                return new VersionRange<>(v, null);
            } else {
                throw new IllegalArgumentException("Invalid from version " + s1);
            }
        } else {
            // version-version
            int index = versionRange.indexOf('-');
            if (index != -1) {
                String from = versionRange.substring(0, index);
                String to = versionRange.substring(index + 1);
                if (parser.isValid(from)) {
                    if (parser.isValid(to)) {
                        return new VersionRange<>(parser.parse(from), parser.parse(to));
                    } else {
                        throw new IllegalArgumentException("Invalid to version " + to);
                    }
                } else {
                    throw new IllegalArgumentException("Invalid from version " + from);
                }
            } else {
                throw new IllegalArgumentException("Invalid version range: None of the expected patterns applied");
            }
        }
    }

    public Version lower() {
        return from;
    }

    public Version upper() {
        return to;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionRange that = (VersionRange) o;
        return Objects.equals(from, that.from) &&
                Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }

    public boolean contains(Version apiVersion) {
        return this == EMPTY || apiVersion == null ? false : from == null || from.compareTo(apiVersion) <= 0 && (to == null || to.compareTo(apiVersion) >= 0);
    }

    public boolean intersects(VersionRange<Version> other) {
        if (this == EMPTY) {
            return false;
        } else if (from == null) {
            return true;
        } else if (to == null) {
            return from.compareTo(other.from) <= 0;
        } else {
            return !(to.compareTo(other.from) < 0 || other.to != null && from.compareTo(other.to) > 0);
        }
    }

    @Override
    public String toString() {
        if (this == EMPTY) {
            return "empty";
        } else if (from == null) {
            return "all";
        } else if (to == null) {
            return from + "+";
        } else if (from.equals(to)) {
            return from.toString();
        } else {
            return from + "-" + to;
        }
    }
}
