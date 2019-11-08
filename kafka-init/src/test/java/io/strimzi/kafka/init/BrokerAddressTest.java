/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;


public class BrokerAddressTest {
    @Test
    public void testSetHostAndPort() {
        String addresses = "0://test0:0";

        BrokerAddress brokerAddress = BrokerAddress.parse(addresses);

        assertThat(brokerAddress.getAdvertisedHost(), is("test0"));
        assertThat(brokerAddress.getAdvertisedPort(), is(new Integer(0)));
    }

    @Test
    public void testHostNotPresent() {
        String addresses = "0://:0";

        BrokerAddress brokerAddress = BrokerAddress.parse(addresses);

        assertThat(brokerAddress.getAdvertisedHost(), is(nullValue()));
        assertThat(brokerAddress.getAdvertisedPort(), is(new Integer(0)));
    }

    @Test
    public void testPortNotPresent() {
        String addresses = "0://test0:";

        BrokerAddress brokerAddress = BrokerAddress.parse(addresses);

        assertThat(brokerAddress.getAdvertisedHost(), is("test0"));
        assertThat(brokerAddress.getAdvertisedPort(), is(nullValue()));
    }

    @Test
    public void testHostAndPortNotPresent() {
        String addresses = "0://:";

        BrokerAddress brokerAddress = BrokerAddress.parse(addresses);

        assertThat(brokerAddress.getAdvertisedHost(), is(nullValue()));
        assertThat(brokerAddress.getAdvertisedPort(), is(nullValue()));
    }
}
