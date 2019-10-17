/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BrokerAddressTest {
    @Test
    public void testSetHostAndPort() {
        String addresses = "0://test0:0";

        BrokerAddress brokerAddress = BrokerAddress.parse(addresses);

        assertEquals(brokerAddress.getAdvertisedHost(), "test0");
        assertEquals(brokerAddress.getAdvertisedPort(), new Integer(0));
    }

    @Test
    public void testHostNotPresent() {
        String addresses = "0://:0";

        BrokerAddress brokerAddress = BrokerAddress.parse(addresses);

        assertEquals(brokerAddress.getAdvertisedHost(), null);
        assertEquals(brokerAddress.getAdvertisedPort(), new Integer(0));
    }

    @Test
    public void testPortNotPresent() {
        String addresses = "0://test0:";

        BrokerAddress brokerAddress = BrokerAddress.parse(addresses);

        assertEquals(brokerAddress.getAdvertisedHost(), "test0");
        assertEquals(brokerAddress.getAdvertisedPort(), null);
    }

    @Test
    public void testHostAndPortNotPresent() {
        String addresses = "0://:";

        BrokerAddress brokerAddress = BrokerAddress.parse(addresses);

        assertEquals(brokerAddress.getAdvertisedHost(), null);
        assertEquals(brokerAddress.getAdvertisedPort(), null);
    }
}
