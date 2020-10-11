/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.kafkaclients.clientproperties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProducerProperties extends AbstractKafkaClientProperties<ProducerProperties> {

    private static final Logger LOGGER = LogManager.getLogger(ProducerProperties.class);
    private static final String DEFAULT_MAX_BLOG_MS_CONFIG = "6000"; // 60 * 100
    private static final String DEFAULT_ACKS_CONFIG = "1";

    public static class ProducerPropertiesBuilder extends AbstractKafkaClientProperties.KafkaClientPropertiesBuilder<ProducerPropertiesBuilder> {

        public ProducerPropertiesBuilder withBootstrapServerConfig(String bootstrapServer) {

            this.properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            return this;
        }

        public ProducerPropertiesBuilder withKeySerializerConfig(Class<? extends Serializer> keySerializer) {

            this.properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getName());
            return this;
        }

        public ProducerPropertiesBuilder withValueSerializerConfig(Class<? extends Serializer> valueSerializer) {

            this.properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
            return this;
        }

        public ProducerPropertiesBuilder withMaxBlockMsConfig(String maxBlockMsConfig) {

            this.properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMsConfig);
            return this;
        }

        public ProducerPropertiesBuilder withClientIdConfig(String clientId) {

            this.properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            return this;
        }

        public ProducerPropertiesBuilder withAcksConfig(String acksConfig) {

            this.properties.setProperty(ProducerConfig.ACKS_CONFIG, acksConfig);
            return this;
        }

        @Override
        public ProducerProperties build() {
            return new ProducerProperties(this);
        }

        @Override
        protected ProducerPropertiesBuilder self() {
            return this;
        }
    }

    private ProducerProperties(ProducerPropertiesBuilder builder) {
        super(builder);

        if (builder.properties.getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG) == null || builder.properties.getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG).isEmpty()) {
            LOGGER.debug("Setting default value of {} to {}", ProducerConfig.MAX_BLOCK_MS_CONFIG, DEFAULT_MAX_BLOG_MS_CONFIG);
            properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, DEFAULT_MAX_BLOG_MS_CONFIG);
        }

        if (builder.properties.getProperty(ProducerConfig.ACKS_CONFIG) == null || builder.properties.getProperty(ProducerConfig.ACKS_CONFIG).isEmpty()) {
            LOGGER.debug("Setting default value of {} to {}", ProducerConfig.ACKS_CONFIG, DEFAULT_ACKS_CONFIG);
            properties.setProperty(ProducerConfig.ACKS_CONFIG, DEFAULT_ACKS_CONFIG);
        }

        properties = builder.properties;
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public ProducerProperties.ProducerPropertiesBuilder toBuilder(ProducerProperties clientProperties) {
        ProducerProperties.ProducerPropertiesBuilder builder = new ProducerProperties.ProducerPropertiesBuilder();

        builder.withNamespaceName(clientProperties.getNamespaceName());
        builder.withClusterName(clientProperties.getClusterName());
        builder.withSecurityProtocol(SecurityProtocol.forName(clientProperties.getProperties().getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG)));
        builder.withBootstrapServerConfig(clientProperties.getProperties().getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        try {
            builder.withKeySerializerConfig((Class<? extends Serializer>) Class.forName(clientProperties.getProperties().getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)));
            builder.withValueSerializerConfig((Class<? extends Serializer>) Class.forName(clientProperties.getProperties().getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        builder.withClientIdConfig(clientProperties.getProperties().getProperty(ProducerConfig.CLIENT_ID_CONFIG));
        builder.withMaxBlockMsConfig(clientProperties.getProperties().getProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG));
        builder.withAcksConfig(clientProperties.getProperties().getProperty(ProducerConfig.ACKS_CONFIG));
        builder.withSharedProperties();

        return builder;
    }
}
