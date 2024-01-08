/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.init;

import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.listener.NodeAddressType;
import io.strimzi.operator.common.model.NodeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Collects and writes the configuration collected in the init container
 */
public class InitWriter {
    private static final Logger LOGGER = LogManager.getLogger(InitWriter.class);

    private final KubernetesClient client;
    private final InitWriterConfig config;

    protected final static String FILE_RACK_ID = "rack.id";
    protected final static String FILE_EXTERNAL_ADDRESS = "external.address";

    /**
     * Constructs the InitWriter
     *
     * @param client    Kubernetes client
     * @param config    InitWriter configuration
     */
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
        LOGGER.info("NodeLabels = {}", nodeLabels);
        String rackId = nodeLabels.get(config.getRackTopologyKey());
        LOGGER.info("Rack: {} = {}", config.getRackTopologyKey(), rackId);

        if (rackId == null) {
            LOGGER.error("Node {} doesn't have the label {} for getting the rackid",
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
        StringBuilder externalAddresses = new StringBuilder();

        String address = NodeUtils.findAddress(addresses, null);

        if (address == null) {
            LOGGER.error("External address not found");
            return false;
        } else  {
            LOGGER.info("Default External address found {}", address);
            externalAddresses.append(externalAddressExport(null, address));
        }

        for (NodeAddressType type : NodeAddressType.values())   {
            address = NodeUtils.findAddress(addresses, type);
            LOGGER.info("External {} address found {}", type.toValue(), address);
            externalAddresses.append(externalAddressExport(type, address));
        }

        return write(FILE_EXTERNAL_ADDRESS, externalAddresses.toString());
    }

    /**
     * Formats address type and address into shell export command for environment variable
     *
     * @param type      Type of the address. Use null for default address
     * @param address   Address for given type
     * @return          String with the shell command
     */
    private String externalAddressExport(NodeAddressType type, String address) {
        String envVar;

        if (type != null) {
            envVar = String.format("STRIMZI_NODEPORT_%s_ADDRESS", type.toValue().toUpperCase(Locale.ENGLISH));
        } else {
            envVar = "STRIMZI_NODEPORT_DEFAULT_ADDRESS";
        }

        return String.format("export %s=%s", envVar, address) + System.lineSeparator();
    }

    /**
     * Write provided information into a file
     *
     * @param file          Target file
     * @param information   Information to be written
     * @return              true if write succeeded, false otherwise
     */
    private boolean write(String file, String information) {
        boolean isWritten;

        try (PrintWriter writer = new PrintWriter(config.getInitFolder() + "/" + file, StandardCharsets.UTF_8)) {
            writer.write(information);

            if (writer.checkError())    {
                LOGGER.error("Failed to write the information {} to file {}", information, file);
                isWritten = false;
            } else {
                LOGGER.info("Information {} written successfully to file {}", information, file);
                isWritten = true;
            }
        } catch (IOException e) {
            LOGGER.error("Error writing the information {} to file {}", information, file, e);
            isWritten = false;
        }

        return isWritten;
    }
}
