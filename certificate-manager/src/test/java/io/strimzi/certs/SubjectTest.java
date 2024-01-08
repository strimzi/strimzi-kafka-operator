/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SubjectTest {
    @Test
    public void testSubjectOpensslDn()   {
        Subject.Builder subject = new Subject.Builder()
            .withCommonName("joe");
        assertThat(subject.build().opensslDn(), is("/CN=joe"));

        subject = new Subject.Builder()
            .withOrganizationName("MyOrg");
        assertThat(subject.build().opensslDn(), is("/O=MyOrg"));

        subject = new Subject.Builder()
            .withCommonName("joe")
            .withOrganizationName("MyOrg");
        assertThat(subject.build().opensslDn(), is("/O=MyOrg/CN=joe"));
    }

    @Test
    public void testSubjectAlternativeNames()   {
        Subject subject = new Subject.Builder()
                .withCommonName("joe")
                .withOrganizationName("MyOrg")
                .addDnsName("example.com")
                .addDnsName("example.org")
                .addDnsNames(List.of("example.cz", "example.co.uk"))
                .addIpAddress("123.123.123.123")
                .addIpAddress("127.0.0.1")
                .build();
        assertEquals(Map.of(
                "IP.0", "123.123.123.123",
                "IP.1", "127.0.0.1",
                "DNS.0", "example.org",
                "DNS.1", "example.co.uk",
                "DNS.2", "example.com",
                "DNS.3", "example.cz"),
                subject.subjectAltNames());
    }

    @Test
    public void testSerialization() throws JsonProcessingException {
        Subject subject = new Subject.Builder()
                .withCommonName("joe")
                .withOrganizationName("MyOrg")
                .addDnsName("example.com")
                .addDnsName("example.org")
                .addIpAddress("123.123.123.123")
                .addIpAddress("127.0.0.1")
                .build();
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writeValueAsString(subject);
        assertEquals(subject, mapper.readValue(json, Subject.class));
        assertEquals("{\"commonName\":\"joe\"," +
                "\"organizationName\":\"MyOrg\"," +
                "\"dnsNames\":[\"example.org\",\"example.com\"]," +
                "\"ipAddresses\":[\"123.123.123.123\",\"127.0.0.1\"]}", json);
    }

    @Test
    public void testDnsValidation() {
        new Subject.Builder().addDnsName("example.com");
        new Subject.Builder().addDnsName("*.example.com");
        new Subject.Builder().addDnsName("example.io.");
        assertThrows(IllegalArgumentException.class, () -> new Subject.Builder().addDnsName("foo.*.example.com"));
        assertThrows(IllegalArgumentException.class, () -> new Subject.Builder().addDnsName("54t8g#'/.l"));
        assertThrows(IllegalArgumentException.class, () -> new Subject.Builder().addDnsName("example.io.."));
    }

    @Test
    public void testIPV6()   {
        Subject subject = new Subject.Builder()
                .withCommonName("joe")
                .addIpAddress("fc01::8d1c")
                .addIpAddress("1762:0000:0000:00:0000:0B03:0001:AF18")
                .addIpAddress("1974:0:0:0:0:B03:1:AF74")
                .build();
        assertEquals(Map.of(
                        "IP.0", "fc01:0:0:0:0:0:0:8d1c",
                        "IP.1", "1974:0:0:0:0:b03:1:af74",
                        "IP.2", "1762:0:0:0:0:b03:1:af18"),
                subject.subjectAltNames());
    }
}
