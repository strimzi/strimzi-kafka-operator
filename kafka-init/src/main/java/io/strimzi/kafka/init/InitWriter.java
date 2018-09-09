/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

public class InitWriter {

    private static final Logger log = LogManager.getLogger(InitWriter.class);

    private KubernetesClient client;
    private InitWriterConfig config;

    protected final static String FILE_RACK_ID = "rack.id";
    protected final static String FILE_EXTERNAL_ADDRESS = "external.address";

    public InitWriter(KubernetesClient client, InitWriterConfig config) {
        this.client = client;
        this.config = config;
    }

    /**
     * Write the rack-id
     *
     * @return if the operation was executed successfully
     */
    public boolean writeRack() {

        Map<String, String> nodeLabels = client.nodes().withName(config.getNodeName()).get().getMetadata().getLabels();
        log.info("NodeLabels = {}", nodeLabels);
        String rackId = nodeLabels.get(config.getRackTopologyKey());
        log.info("Rack: {} = {}", config.getRackTopologyKey(), rackId);

        if (rackId == null) {
            log.error("Node {} doesn't have the label {} for getting the rackid",
                    config.getNodeName(), config.getRackTopologyKey());
            return false;
        }

        return write(FILE_RACK_ID, rackId);
    }

    /**
     * Write the external address of this node
     *
     * @return if the operation was executed successfully
     */
    public boolean writeExternalAddress() {

        List<NodeAddress> addresses = client.nodes().withName(config.getNodeName()).get().getStatus().getAddresses();
        log.info("NodeLabels = {}", addresses);
        String externalAddress = findAddress(addresses);

        if (externalAddress == null) {
            log.error("External address nto found");
            return false;
        } else  {
            log.info("External address found {}", externalAddress);
        }

        return write(FILE_EXTERNAL_ADDRESS, externalAddress);
    }

    /**
     * Write provided information into a file
     *
     * @param file          Target file
     * @param information   Information to be written
     * @return              true if write succeeded, false otherwise
     */
    private boolean write(String file, String information) {
        PrintWriter writer = null;
        boolean isWritten;
        try {
            writer = new PrintWriter(config.getInitFolder() + "/" + file, "UTF-8");
            writer.write(information);
            log.info("Information {} written successfully to file {}", information, file);
            isWritten = true;
        } catch (IOException e) {
            log.error("Error writing the information {} to file {}", information, file, e);
            isWritten = false;
        } finally {
            if (writer != null)
                writer.close();
        }

        return isWritten;
    }

    /**
     * Tries to find the right address of the node. The different addresses has different prioprities:
     *      1. ExternalDNS
     *      2. ExternalIP
     *      3. Hostname
     *      4. InternalDNS
     *      5. InternalIP
     *
     * @param addresses List of addresses which are assigned to our node
     * @return  Address of the node
     */
    protected String findAddress(List<NodeAddress> addresses)   {
        if (addresses == null)  {
            return null;
        }

        NodeAddress found = null;

        found = findAddressType(addresses, "ExternalDNS");
        if (found != null)  {
            return found.getAddress();
        }

        found = findAddressType(addresses, "ExternalIP");
        if (found != null)  {
            return found.getAddress();
        }

        found = findAddressType(addresses, "Hostname");
        if (found != null)  {
            return found.getAddress();
        }

        found = findAddressType(addresses, "InternalDNS");
        if (found != null)  {
            return found.getAddress();
        }

        found = findAddressType(addresses, "InternalIP");
        if (found != null)  {
            return found.getAddress();
        }

        return null;
    }

    /**
     * Find if address of given type exists
     *
     * @param addresses     List of node addresses
     * @param type          Type of address we are looking fine
     * @return
     */
    protected NodeAddress findAddressType(List<NodeAddress> addresses, String type)   {
        for (NodeAddress address : addresses) {
            if (type.equals(address.getType())) {
                return address;
            }
        }

        return null;
    }
}
