/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.NodeAddressBuilder;
import io.strimzi.api.kafka.model.kafka.listener.NodeAddressType;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

public class NodeUtilsTest {
    private static final List<NodeAddress> ADDRESSES = new ArrayList<>(3);

    static {
        ADDRESSES.add(new NodeAddressBuilder().withType("ExternalDNS").withAddress("my.external.address").build());
        ADDRESSES.add(new NodeAddressBuilder().withType("InternalDNS").withAddress("my.internal.address").build());
        ADDRESSES.add(new NodeAddressBuilder().withType("InternalIP").withAddress("192.168.2.94").build());
    }

    @Test
    public void testFindAddressWithAddressType()   {
        String address = NodeUtils.findAddress(ADDRESSES, NodeAddressType.INTERNAL_DNS);

        assertThat(address, is("my.internal.address"));
    }

    @Test
    public void testFindAddressReturnsExternalAddress()   {
        String address = NodeUtils.findAddress(ADDRESSES, null);

        assertThat(address, is("my.external.address"));
    }

    @Test
    public void testFindAddressNullWithInvalidAddressTypes()   {
        List<NodeAddress> addresses = new ArrayList<>(3);
        addresses.add(new NodeAddressBuilder().withType("SomeAddress").withAddress("my.external.address").build());
        addresses.add(new NodeAddressBuilder().withType("SomeOtherAddress").withAddress("my.internal.address").build());
        addresses.add(new NodeAddressBuilder().withType("YetAnotherAddress").withAddress("192.168.2.94").build());

        String address = NodeUtils.findAddress(addresses, null);

        assertThat(address, is(nullValue()));
    }

    @Test
    public void testFindAddressNullWhenAddressesNull()   {
        List<NodeAddress> addresses = null;

        String address = NodeUtils.findAddress(addresses, null);

        assertThat(address, is(nullValue()));
    }

    @Test
    public void testFindAddressWithMultipleAddressesOfSameType()   {
        List<NodeAddress> addresses = new ArrayList<>(3);
        addresses.add(new NodeAddressBuilder().withType("ExternalDNS").withAddress("my.external.address").build());
        addresses.add(new NodeAddressBuilder().withType("ExternalDNS").withAddress("my.external.address2").build());
        addresses.add(new NodeAddressBuilder().withType("InternalDNS").withAddress("my.internal.address").build());
        addresses.add(new NodeAddressBuilder().withType("InternalDNS").withAddress("my.internal.address2").build());
        addresses.add(new NodeAddressBuilder().withType("InternalIP").withAddress("192.168.2.94").build());

        String address = NodeUtils.findAddress(addresses, null);

        assertThat(address, is("my.external.address"));
    }

    @Test
    public void testFindAddressWithType()   {
        String address = NodeUtils.findAddress(ADDRESSES, NodeAddressType.INTERNAL_DNS);

        assertThat(address, is("my.internal.address"));
    }

    @Test
    public void testFindAddress()   {
        String address = NodeUtils.findAddress(ADDRESSES, null);

        assertThat(address, is("my.external.address"));
    }

    @Test
    public void testFindAddressNotFound()   {
        List<NodeAddress> addresses = new ArrayList<>(3);
        addresses.add(new NodeAddressBuilder().withType("SomeAddress").withAddress("my.external.address").build());
        addresses.add(new NodeAddressBuilder().withType("SomeOtherAddress").withAddress("my.internal.address").build());
        addresses.add(new NodeAddressBuilder().withType("YetAnotherAddress").withAddress("192.168.2.94").build());
        String address = NodeUtils.findAddress(addresses, null);

        assertThat(address, is(CoreMatchers.nullValue()));
    }

    @Test
    public void testFindAddressesNull()   {
        List<NodeAddress> addresses = null;

        String address = NodeUtils.findAddress(addresses, null);

        assertThat(address, is(CoreMatchers.nullValue()));
    }
}
