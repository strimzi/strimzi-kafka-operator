/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class DnsNameGeneratorTest {

    private static final String NAMESPACE = "my-ns";
    private static final String SERVICE_NAME = "my-service";
    private static final String POD_NAME = "my-pod-1";

    @Test
    public void testPodDnsName() {
        assertThat(DnsNameGenerator.of(NAMESPACE, SERVICE_NAME).podDnsName(POD_NAME),
                is("my-pod-1.my-service.my-ns.svc.cluster.local"));
    }

    @Test
    public void testPodDnsNameWithoutClusterDomain()  {
        assertThat(DnsNameGenerator.of(NAMESPACE, SERVICE_NAME).podDnsNameWithoutClusterDomain(POD_NAME),
                is("my-pod-1.my-service.my-ns.svc"));
    }

    @Test
    public void testServiceDnsName()  {
        assertThat(DnsNameGenerator.of(NAMESPACE, SERVICE_NAME).serviceDnsName(),
                is("my-service.my-ns.svc.cluster.local"));
    }

    @Test
    public void testServiceDnsNameWithoutClusterDomain()  {
        assertThat(DnsNameGenerator.of(NAMESPACE, SERVICE_NAME).serviceDnsNameWithoutClusterDomain(),
                is("my-service.my-ns.svc"));
    }

    @Test
    public void testWildcardServiceDnsName()  {
        assertThat(DnsNameGenerator.of(NAMESPACE, SERVICE_NAME).wildcardServiceDnsName(),
                is("*.my-service.my-ns.svc.cluster.local"));
    }

    @Test
    public void testWildcardServiceDnsNameWithoutClusterDomain()  {
        assertThat(DnsNameGenerator.of(NAMESPACE, SERVICE_NAME).wildcardServiceDnsNameWithoutClusterDomain(),
                is("*.my-service.my-ns.svc"));
    }
}