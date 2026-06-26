/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class IpAndDnsValidationTest {
    @Test
    public void testIPv6()   {
        assertThat(IpAndDnsValidation.isValidIpv6Address("::1"), is(true));
        assertThat(IpAndDnsValidation.isValidIpv6Address("fc01::"), is(true));
        assertThat(IpAndDnsValidation.isValidIpv6Address("fc01::8d1c"), is(true));
        assertThat(IpAndDnsValidation.isValidIpv6Address("::fc01:8d1c"), is(true));
        assertThat(IpAndDnsValidation.isValidIpv6Address("fc01:8d1c::"), is(true));
        assertThat(IpAndDnsValidation.isValidIpv6Address("1762:0:0:0:0:B03:1:AF18"), is(true));

        assertThat(IpAndDnsValidation.isValidIpv6Address("::"), is(false));
        assertThat(IpAndDnsValidation.isValidIpv6Address("fc01::8j1c"), is(false)); // j is not allowed character
        assertThat(IpAndDnsValidation.isValidIpv6Address("fc01::176d::8d1c"), is(false)); // Too many ::
        assertThat(IpAndDnsValidation.isValidIpv6Address("1762:0:0:0:0:B03::0:1:AF18"), is(false)); // Too many segements
        assertThat(IpAndDnsValidation.isValidIpv6Address("fc01c::8d1c"), is(false)); // Segment with too many characters
        assertThat(IpAndDnsValidation.isValidIpv6Address("fc01:::8d1c"), is(false)); // Triple :
        assertThat(IpAndDnsValidation.isValidIpv6Address("176J:0:0:0:0:B03:1:AF18"), is(false));
        assertThat(IpAndDnsValidation.isValidIpv6Address("1762:0:0:0:0:1:B03:1:AF18"), is(false)); // Too many segments
        assertThat(IpAndDnsValidation.isValidIpv6Address("1762:0:0:0:0:B03:1:2AF18"), is(false)); // Segment with too many characters
        assertThat(IpAndDnsValidation.isValidIpv6Address("1762:0:0:0:0:53B03:1:AF18"), is(false)); // Segment with too many characters
    }

    @Test
    public void testIPv4()   {
        assertThat(IpAndDnsValidation.isValidIpv4Address("127.0.0.1"), is(true));
        assertThat(IpAndDnsValidation.isValidIpv4Address("123.123.123.123"), is(true));

        assertThat(IpAndDnsValidation.isValidIpv4Address("127.0.0.0.1"), is(false));
        assertThat(IpAndDnsValidation.isValidIpv4Address("127.0.1"), is(false));
        assertThat(IpAndDnsValidation.isValidIpv4Address("321.321.321.321"), is(false));
        assertThat(IpAndDnsValidation.isValidIpv4Address("some.domain.name"), is(false));
    }

    @Test
    public void testDnsNames()   {
        assertThat(IpAndDnsValidation.isValidDnsNameOrWildcard("example"), is(true));
        assertThat(IpAndDnsValidation.isValidDnsNameOrWildcard("example.com"), is(true));

        assertThat(IpAndDnsValidation.isValidDnsNameOrWildcard("example:com"), is(false));
        assertThat(IpAndDnsValidation.isValidDnsNameOrWildcard("veryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryveryverylongexample.com"), is(false));

        assertThat(IpAndDnsValidation.isValidDnsNameOrWildcard("example.com."), is(true));
        assertThat(IpAndDnsValidation.isValidDnsNameOrWildcard("example.com.."), is(false));
        assertThat(IpAndDnsValidation.isValidDnsNameOrWildcard("*.example.com."), is(true));
    }

    @Test
    public void testIpv6Normalization() {
        assertThat(IpAndDnsValidation.normalizeIpv6Address("fc01::8d1c"), is("fc01:0:0:0:0:0:0:8d1c"));
        assertThat(IpAndDnsValidation.normalizeIpv6Address("FC01::8D1C"), is("fc01:0:0:0:0:0:0:8d1c"));
        assertThat(IpAndDnsValidation.normalizeIpv6Address("00FC::0D1C"), is("fc:0:0:0:0:0:0:d1c"));
        assertThat(IpAndDnsValidation.normalizeIpv6Address("fc01::af18:8d1c"), is("fc01:0:0:0:0:0:af18:8d1c"));
        assertThat(IpAndDnsValidation.normalizeIpv6Address("::fc01:8d1c"), is("0:0:0:0:0:0:fc01:8d1c"));
        assertThat(IpAndDnsValidation.normalizeIpv6Address("fc01:8d1c::"), is("fc01:8d1c:0:0:0:0:0:0"));
        assertThat(IpAndDnsValidation.normalizeIpv6Address("::8d1c"), is("0:0:0:0:0:0:0:8d1c"));
        assertThat(IpAndDnsValidation.normalizeIpv6Address("fc01::"), is("fc01:0:0:0:0:0:0:0"));
        assertThat(IpAndDnsValidation.normalizeIpv6Address("::1"), is("0:0:0:0:0:0:0:1"));

        assertThat(IpAndDnsValidation.normalizeIpv6Address("1762:0000:000:00:0:0B03:0001:AF18"), is("1762:0:0:0:0:b03:1:af18"));
        assertThat(IpAndDnsValidation.normalizeIpv6Address("1762:0000:0000:0000:0000:0B03:0001:AF18"), is("1762:0:0:0:0:b03:1:af18"));
        assertThat(IpAndDnsValidation.normalizeIpv6Address("0000:0000:0000:1762:0000:0B03:0001:AF18"), is("0:0:0:1762:0:b03:1:af18"));
        assertThat(IpAndDnsValidation.normalizeIpv6Address("0000:0000:0000:1762:0000:0B03::0001:AF18"), is("0:0:0:1762:0:b03:1:af18"));
        assertThat(IpAndDnsValidation.normalizeIpv6Address("1762:0000:0000:0000:0000:0B03:0001:00"), is("1762:0:0:0:0:b03:1:0"));
    }
}
