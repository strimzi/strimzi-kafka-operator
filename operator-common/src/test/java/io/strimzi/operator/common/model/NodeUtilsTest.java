/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.NodeAddressBuilder;
import io.strimzi.operator.cluster.model.NodeUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class NodeUtilsTest {
    private static List<NodeAddress> addresses = new ArrayList<>(3);

    static {
        addresses.add(new NodeAddressBuilder().withType("ExternalDNS").withAddress("my.external.address").build());
        addresses.add(new NodeAddressBuilder().withType("InternalDNS").withAddress("my.internal.address").build());
        addresses.add(new NodeAddressBuilder().withType("InternalIP").withAddress("192.168.2.94").build());
    }

    @Test
    public void testFindAddressWithType()   {
        String address = NodeUtils.findAddress(addresses, "InternalDNS");

        assertThat(address, is("my.internal.address"));
    }

    @Test
    public void testFindAddress()   {
        String address = NodeUtils.findAddress(addresses, null);

        assertThat(address, is("my.external.address"));
    }

    @Test
    public void testFindAddressNotFound()   {
        List<NodeAddress> addresses = new ArrayList<>(3);
        addresses.add(new NodeAddressBuilder().withType("SomeAddress").withAddress("my.external.address").build());
        addresses.add(new NodeAddressBuilder().withType("SomeOtherAddress").withAddress("my.internal.address").build());
        addresses.add(new NodeAddressBuilder().withType("YetAnotherAddress").withAddress("192.168.2.94").build());
        String address = NodeUtils.findAddress(addresses, null);

        assertThat(address, is(nullValue()));
    }

    @Test
    public void testFindAddressesNull()   {
        List<NodeAddress> addresses = null;

        String address = NodeUtils.findAddress(addresses, null);

        assertThat(address, is(nullValue()));
    }
}
