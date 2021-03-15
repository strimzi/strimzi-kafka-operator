/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.certs;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SubjectTests {
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
                .addIpAddress("123.123.123.123")
                .addIpAddress("127.0.0.1")
                .build();
        assertEquals(Map.of(
                "IP.0", "123.123.123.123",
                "IP.1", "127.0.0.1",
                "DNS.0", "example.org",
                "DNS.1", "example.com"),
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
        assertThrows(IllegalArgumentException.class, () -> new Subject.Builder().addDnsName("foo.*.example.come"));
        assertThrows(IllegalArgumentException.class, () -> new Subject.Builder().addDnsName("54t8g#'/.l"));

    }
}
