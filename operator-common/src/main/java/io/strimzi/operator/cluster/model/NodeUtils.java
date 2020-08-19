/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.NodeAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NodeUtils {
    private static final Logger log = LogManager.getLogger(NodeUtils.class);

    /**
     * Tries to find the right address of the node. The different addresses has different prioprities:
     *      1. ExternalDNS
     *      2. ExternalIP
     *      3. Hostname
     *      4. InternalDNS
     *      5. InternalIP
     *
     * @param addresses List of addresses which are assigned to our node
     * @param preferredAddressType Name of the address which is preferred by the user. Null if not specified.
     *
     * @return  Address of the node
     */
    public static String findAddress(List<NodeAddress> addresses, String preferredAddressType)   {
        if (addresses == null)  {
            return null;
        }

        Map<String, String> addressMap = addresses.stream()
                .collect(Collectors.toMap(NodeAddress::getType, NodeAddress::getAddress, (address1, address2) -> {
                    log.warn("Found multiple addresses with the same type. Only the first address '{}' will be used.", address1);
                    return address1;
                }));

        // If user set preferred address type, we should check it first
        if (preferredAddressType != null && addressMap.containsKey(preferredAddressType)) {
            return addressMap.get(preferredAddressType);
        }

        if (addressMap.containsKey("ExternalDNS"))  {
            return addressMap.get("ExternalDNS");
        } else if (addressMap.containsKey("ExternalIP"))  {
            return addressMap.get("ExternalIP");
        } else if (addressMap.containsKey("InternalDNS"))  {
            return addressMap.get("InternalDNS");
        } else if (addressMap.containsKey("InternalIP"))  {
            return addressMap.get("InternalIP");
        } else if (addressMap.containsKey("Hostname")) {
            return addressMap.get("Hostname");
        }

        return null;
    }
}
