/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

public class BrokerAddress {
    private final Integer index;
    private final String advertisedHost;
    private final Integer advertisedPort;

    public BrokerAddress(Integer index, String advertisedHost, Integer advertisedPort) {
        this.index = index;
        this.advertisedHost = advertisedHost;
        this.advertisedPort = advertisedPort;
    }

    public Integer getIndex() {
        return index;
    }

    public String getAdvertisedHost() {
        return advertisedHost;
    }

    public Integer getAdvertisedPort() {
        return advertisedPort;
    }

    public static BrokerAddress parse(String address) {
        String[] indexAndAddress = address.split("://");
        Integer index = Integer.parseInt(indexAndAddress[0]);
        String[] hostAndPort = indexAndAddress[1].split(":");
        if (hostAndPort.length == 0) {
            return new BrokerAddress(index, null, null);
        }
        String host = hostAndPort[0].trim().length() > 0 ? hostAndPort[0] : null;
        Integer port = hostAndPort.length > 1
            && hostAndPort[1].trim().length() > 0 ?
            Integer.parseInt(hostAndPort[1]) : null;
        return new BrokerAddress(index, host, port);
    }
}
