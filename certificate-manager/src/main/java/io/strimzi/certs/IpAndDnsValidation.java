/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Helper class to help with validation of IP addresses and DNS addresses
 */
public class IpAndDnsValidation {
    // Pattern for matching IPv4 address (requires additional check for segments to be equal or less than 255 - this is done in the isValidIpv4Address method)
    private static final Pattern IPV4_ADDRESS = Pattern.compile("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}$");

    // Pattern for uncompressed IPv6 address -> case-insensitive, tolerates leading 0s
    private static final Pattern IPV6_ADDRESS = Pattern.compile("^(?:[\\da-fA-F]{1,4}:){7}[\\da-fA-F]{1,4}$");

    // Pattern for compressed IPv6 address -> case-insensitive, tolerates leading 0s (requires additional check for number of segments)
    private static final Pattern IPV6_COMPRESSED_ADDRESS = Pattern.compile("^((?:[\\dA-Fa-f]{1,4}(?::[\\dA-Fa-f]{1,4})*)?)::((?:[\\dA-Fa-f]{1,4}(?::[\\dA-Fa-f]{1,4})*)?)$");

    // Pattern for DNS names
    private static final Pattern DNS_NAME;
    static {
        String dnsLabel = "(" +
                // a single char dns name (can't begin or end with)
                "[a-zA-Z\\d]|" +
                // followed by more labels of same
                "[a-zA-Z\\d][a-zA-Z\\d\\-]{0,61}[a-zA-Z\\d])";
        DNS_NAME = Pattern.compile("^" + dnsLabel + "(\\." + dnsLabel + ")*");
    }

    // Pattern used to normalize the segments of the IPv6 address
    private static final Pattern IPV6_LEADING_ZEROS_TRIMMING = Pattern.compile("(^|:)0+([\\da-f]+)(:|$)");

    /**
     * Returns true if the name is a valid DNS name or a wildcard DNS name. That means that it matches the DNS_NAME
     * pattern or starts with *. followed up with a string which matches the DNS_PATTERN.
     *
     * @param name   Name which should be validated
     *
     * @return  True if the name is valid DNS name. False otherwise.
     */
    public static boolean isValidDnsNameOrWildcard(String name) {
        return name.length() <= 255
                && (DNS_NAME.matcher(name).matches()
                || (name.startsWith("*.") && DNS_NAME.matcher(name.substring(2)).matches()));
    }

    /**
     * Returns true if the name is a valid IPv4 or IPv6 address.
     *
     * @param ip    IP address which should be validated
     *
     * @return  True if the name is valid IP address. False otherwise.
     */
    public static boolean isValidIpAddress(String ip) {
        return isValidIpv4Address(ip) || isValidIpv6Address(ip);
    }

    /**
     * Returns true if the name is a valid IPv4 address. Valid IPv4 address is an address which matches the IPV6_ADDRESS
     * or IPV6_COMPRESSED_ADDRESS pattern.
     *
     * @param ip    IP address which should be validated
     *
     * @return  True if the name is valid IPv6 address. False otherwise.
     */
    public static boolean isValidIpv4Address(String ip) {
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

    /**
     * Returns true if the name is a valid IPv6 address. Valid IPv6 address is an address which matches the IPV4_ADDRESS
     * pattern and each segment is equal or less than 255.
     *
     * @param ip    IP address which should be validated
     *
     * @return  True if the name is valid IPv4 address. False otherwise.
     */
    public static boolean isValidIpv6Address(String ip) {
        return IPV6_ADDRESS.matcher(ip).matches()
                // Includes additional segment count check for the compressed address
                || (IPV6_COMPRESSED_ADDRESS.matcher(ip).matches() && ip.split(":").length > 0 && ip.split(":").length <= 7);
    }

    /**
     * Normalizes the IPv6 address. The address passed to thus method should be already validated. Normalized address
     * in Strimzi means that the IPv6 address is:
     *     - uncompressed
     *     - without leading 0
     *     - lowercase
     * This is based on the OpenSSL output. OpenSSL will normalize the IPv6 addresses in SANs this way. And we need to
     * be able to compare the SANs to know when the SANs changed and a new cert should be generated.
     *
     * @param validIp    IPv6 address which should be normalized. The IPv6 address passed here should be already validated.
     *
     * @return  Normalized IPv6 address
     */
    public static String normalizeIpv6Address(String validIp)   {
        String normalized = validIp;

        // Normalize compressed address
        // We insert the required number of 0 segments instead of :: to uncompress the address
        if (normalized.contains("::"))  {
            int segments = normalized.split(":").length - 1;
            normalized = normalized.replace("::", ":" + "0:".repeat(8 - segments));

            if (normalized.startsWith(":")) {
                normalized = "0" + normalized;
            } else if (normalized.endsWith(":"))    {
                normalized = normalized.substring(0, normalized.length() - 3);
            }
        }

        // Continue with uncompressed IP address
        // Make it lowercase
        normalized = normalized.toLowerCase(Locale.ENGLISH);

        // Remove leading zeros (this intentionally runs twice, because I do not know how to write a better REGEX which would do everything in one go)
        normalized = IPV6_LEADING_ZEROS_TRIMMING.matcher(normalized).replaceAll("$1$2$3");
        normalized = IPV6_LEADING_ZEROS_TRIMMING.matcher(normalized).replaceAll("$1$2$3");

        // return the normalized IPv6 address
        return normalized;
    }
}
