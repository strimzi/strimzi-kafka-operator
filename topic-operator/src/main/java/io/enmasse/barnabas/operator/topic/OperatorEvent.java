/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.enmasse.barnabas.operator.topic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class OperatorEvent implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(OperatorEvent.class);

    public static final String MAP_KEY_PARTITIONS = "partitions";
    public static final String MAP_KEY_REPLICAS = "replicas";
    public static final String MAP_KEY_NAME = "name";
    public static final String MAK_KEY_CONFIG = "config";
    public static final String MAP_KEY_REPLICA = "replica.";

    protected final Operator operator;

    protected OperatorEvent(Operator operator) {
        this.operator = operator;
    }

    public abstract void process() throws OperatorException;

    public void run() {
        logger.info("Processing event {}", this);
        OperatorEvent operatorEvent = this;
        Event outcomeEvent = null;
        try {
            operatorEvent.process();
            //outcomeEvent = createSuccessEvent(meta);
            logger.info("Event {} processed successfully", this);
        } catch (OperatorException e) {
            logger.error("Event {} had an error", this, e);
            outcomeEvent = createErrorEvent(e);
        } catch (Exception e) {
            logger.error("Event {} had an error", this, e);
        }
        operator.createEvent(outcomeEvent);

    }

    private Event createSuccessEvent(HasMetadata configMap) {
        String myHost = "";
        return new EventBuilder()
                .withApiVersion("v1")
                .withNewInvolvedObject()
                .withKind(configMap.getKind())
                .withName(configMap.getMetadata().getName())
                .withApiVersion(configMap.getApiVersion())
                .withNamespace(configMap.getMetadata().getNamespace())
                .withUid(configMap.getMetadata().getUid())
                .endInvolvedObject()
                .withType("Success")
                .withMessage(this.getClass().getSimpleName() + " successful")
                //.withReason("")
                .withNewSource()
                .withHost(myHost)
                .withComponent(Operator.class.getName())
                .endSource()
                .build();
    }

    private Event createErrorEvent(OperatorException e) {
        String myHost = "";
        EventBuilder evtb = new EventBuilder().withApiVersion("v1");
        if (e.getMetadata() != null) {
            HasMetadata meta = e.getMetadata();
            evtb.withNewInvolvedObject()
                .withKind(e.getMetadata().getKind())
                .withName(meta.getMetadata().getName())
                .withApiVersion(meta.getApiVersion())
                .withNamespace(meta.getMetadata().getNamespace())
                .withUid(meta.getMetadata().getUid())
                .endInvolvedObject();
        }
        evtb.withType("Warning")
            .withMessage(this.getClass().getSimpleName() + " failed: " + e.getMessage())
            //.withReason("")
            .withNewSource()
            .withHost(myHost)
            .withComponent(Operator.class.getName())
            .endSource();
        return evtb.build();
    }

    /** Topic created in ZK */
    public static class TopicCreated extends OperatorEvent {
        private final TopicName topicName;
        private final TopicDescription topicDescription;
        private final Config topicConfig;

        public TopicCreated(Operator operator, TopicName topicName, TopicDescription topicDescription, Config topicConfig) {
            super(operator);
            this.topicName = topicName;
            this.topicDescription = topicDescription;
            this.topicConfig = topicConfig;
        }

        @Override
        public void process() throws OperatorException {
            // 1. get topic data from AdminClient
            //   How do we avoid a race here, where the topic exists in ZK, but not yet visible from AC?
            //   Or can we get all the info from ZK?
            if (topicDescription.isInternal()) {
                logger.info("Topic {} is internal, so not creating a ConfigMap", topicName);
            } else {
                ConfigMap cm = mkConfigMap(topicDescription, topicConfig);
                // 2. create ConfigMap in k8s
                operator.createConfigMap(cm);
            }
            // TODO What about races when topic deleted and recreated?
        }

        private ConfigMap mkConfigMap(TopicDescription topicDescription, Config topicConfig) {
            ConfigMapBuilder builder = new ConfigMapBuilder().withApiVersion("v1");

            builder.withNewMetadata().withName(this.topicName.asMapName().toString()).withLabels(mkLabels()).endMetadata();
            List<TopicPartitionInfo> partitions = topicDescription.partitions();
            Map<String, String> mapData = new HashMap<>(partitions.size() + 4);
            mapData.put(MAP_KEY_NAME, this.topicName.toString());
            mapData.put(MAK_KEY_CONFIG, configToYaml(topicConfig));
            for (TopicPartitionInfo partition : partitions) {
                int partitionId = partition.partition();
                partition.replicas();
                if (!mapData.containsKey(MAP_KEY_REPLICAS)) {
                    mapData.put(MAP_KEY_REPLICAS, Integer.toString(partition.replicas().size()));
                }
                mapData.put(MAP_KEY_REPLICA +partitionId, partition.replicas().toString());
            }
            builder.withData(mapData);
            return builder.build();
        }

        private Map<String, String> mkLabels() {
            Map<String, String> labels = new HashMap<>(3);
            labels.put("app", "barnabas");
            labels.put("type", "runtime");
            labels.put("kind", "topic");
            return labels;
        }

        private String configToYaml(Config config) {
            YAMLFactory yf = new YAMLFactory();
            ObjectMapper mapper = new ObjectMapper(yf);
            ObjectNode root = mapper.createObjectNode();
            for (ConfigEntry entry : config.entries()) {
                root.put(entry.name(), entry.value());
            }
            StringWriter sw = new StringWriter();
            try {
                SequenceWriter seqw = mapper.writerWithDefaultPrettyPrinter().writeValues(sw);
                seqw.write(root);
                return sw.toString();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String toString() {
            return "TopicCreated("+topicName+")";
        }
    }

    /** Topic deleted in ZK */
    public static class TopicDeleted extends OperatorEvent {
        public final TopicName topicName;
        public TopicDeleted(Operator operator, TopicName topicName) {
            super(operator);
            this.topicName = topicName;
        }

        @Override
        public void process() {
            // Record that it's us who is deleting the config map
            // delete the ConfigMap in k8s
            // ignore the watch for the configmap deletion
            operator.deleteConfigMap(topicName);
        }

        @Override
        public String toString() {
            return "TopicDeleted("+topicName+")";
        }
    }

    /** Topic config modified in ZK */
    public static class TopicConfigModified extends OperatorEvent {
        public final String topicName;
        public TopicConfigModified(Operator operator, String topicName) {
            super(operator);
            this.topicName = topicName;
        }

        @Override
        public void process() {
            // TODO get topic data from AdminClient
            // How do we avoid a race here, where the topic exists in ZK, but not yet visible from AC?
            // Record that it's us who is creating to the config map
            // create ConfigMap in k8s
            // ignore the watch for the configmap creation
        }

        @Override
        public String toString() {
            return "TopicConfigModified("+topicName+")";
        }
    }

    /** Topic partitions modified in ZK */
    public static class TopicPartitionsModified extends OperatorEvent {
        public final String topicName;
        public TopicPartitionsModified(Operator operator, String topicName) {
            super(operator);
            this.topicName = topicName;
        }

        @Override
        public void process() {

        }

        @Override
        public String toString() {
            return "TopicPartitionsModified("+topicName+")";
        }
    }

    /** ConfigMap created in k8s */
    public static class ConfigMapCreated extends OperatorEvent {
        private final static Logger logger = LoggerFactory.getLogger(ConfigMapCreated.class);

        public final ConfigMap configMap;

        public ConfigMapCreated(Operator operator, ConfigMap configMap) {
            super(operator);
            this.configMap = configMap;
        }

        @Override
        public void process() throws OperatorException {
            ObjectMeta metadata = configMap.getMetadata();
            String mapName = metadata.getName();
            logger.info("ConfigMap '{}' added", mapName);
            Map<String, String> data = configMap.getData();
            final String topicName = data.getOrDefault(MAP_KEY_NAME, mapName);
            String partitionsStr = data.get(MAP_KEY_PARTITIONS);
            if (partitionsStr == null || partitionsStr.isEmpty()) {
                throw new OperatorException(configMap, "Cannot create topic '{}': no '{}' given in ConfigMap '{}'");
            }
            int partitions = Integer.parseInt(partitionsStr);
            String replicasStr = data.get(MAP_KEY_REPLICAS);
            if (replicasStr == null || replicasStr.isEmpty()) {
                throw new OperatorException(configMap, "Cannot create topic '{}': no '{}' given in ConfigMap '{}'");
            }
            try {
                short numReplicas = Short.parseShort(replicasStr);
                logger.info("Creating topic '{}' with {} partitions and {} replicas", topicName, partitions, numReplicas);
                operator.createTopic(new NewTopic(topicName, partitions, numReplicas), (ar) -> {
                    if (ar.isSuccess()) {
                        logger.info("Created topic '{}' for ConfigMap '{}': {}", topicName, mapName);
                    } else {
                        if (ar.exception() instanceof TopicExistsException) {
                            // TODO reconcile
                        } else {
                            throw new OperatorException(configMap, ar.exception());
                        }
                    }
                });
            } catch (NumberFormatException e) {
                throw new OperatorException(configMap, "Assigning replicas not yet supported");
            }
        }

        @Override
        public String toString() {
            ObjectMeta metadata = configMap.getMetadata();
            String mapName = metadata.getName();
            return "ConfigMapCreated("+mapName+")";
        }
    }

    /** ConfigMap modified in k8s */
    public static class ConfigMapModified extends OperatorEvent {
        private final static Logger logger = LoggerFactory.getLogger(ConfigMapModified.class);

        public final ConfigMap configMap;

        public ConfigMapModified(Operator operator, ConfigMap configMap) {
            super(operator);
            this.configMap = configMap;
        }

        @Override
        public void process() throws OperatorException {
            ObjectMeta metadata = configMap.getMetadata();
            String mapName = metadata.getName();
            logger.info("ConfigMap '{}' modified", mapName);
            Map<String, String> data = configMap.getData();
            TopicName topicName = new TopicName(configMap);
            operator.describeTopic(topicName, (result) -> {
                if (result.isSuccess()) {
                    TopicDescription topicInfo = result.result();
                    int configuredNumPartitions = topicInfo.partitions().size();
                    String partitionsStr = data.get(MAP_KEY_PARTITIONS);
                    if (partitionsStr == null || partitionsStr.isEmpty()) {
                        throw new OperatorException(configMap, "Cannot create topic '{}': no '{}' given in ConfigMap '{}'");
                    }
                    try {
                        int topicNumPartitions = Integer.parseInt(partitionsStr);
                        if (configuredNumPartitions != topicNumPartitions) {
                            // TODO need to add partitions
                            throw new OperatorException(configMap, "Assigning replicas not yet supported");
                        }
                    } catch (NumberFormatException e) {
                        // TODO need to check assigned partitions matches configured assignment
                        // and if not we need to reassign
                        throw new OperatorException(configMap, "Assigning replicas not yet supported");
                    }
                } else {
                    throw new OperatorException(configMap, result.exception());
                }
            });

            // TODO reconcile topic config
        }

        @Override
        public String toString() {
            ObjectMeta metadata = configMap.getMetadata();
            String mapName = metadata.getName();
            return "ConfigMapModified("+mapName+")";
        }
    }

    /** ConfigMap deleted in k8s */
    public static class ConfigMapDeleted extends OperatorEvent {
        private final static Logger logger = LoggerFactory.getLogger(ConfigMapDeleted.class);

        public final ConfigMap configMap;

        public ConfigMapDeleted(Operator operator, ConfigMap configMap) {
            super(operator);
            this.configMap = configMap;
        }

        @Override
        public void process() throws OperatorException {
            ObjectMeta metadata = configMap.getMetadata();
            String mapName = metadata.getName();
            logger.info("ConfigMap '{}' deleted", mapName);
            TopicName topicName = new TopicName(configMap);
            logger.info("Deleting topic '{}'", topicName);
            operator.deleteTopic(topicName, (result) -> {
                if (result.isSuccess()) {
                    logger.info("Deleted topic '{}' for ConfigMap '{}': {}", topicName, mapName);
                } else {
                    throw new OperatorException(configMap, result.exception());
                }
            });

            // TODO we need to do better than simply logging on error
            // -- can we produce some kind of error event in k8s?

            // -- really we want an error to propagate out of the Kubernetes API for deleting the config map
            //    but that's only really an option with a CRD and customer operator
        }

        @Override
        public String toString() {
            ObjectMeta metadata = configMap.getMetadata();
            String mapName = metadata.getName();
            return "ConfigMapDeleted("+mapName+")";
        }
    }

}
