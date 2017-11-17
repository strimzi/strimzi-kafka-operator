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

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class K8sImpl implements K8s {

    private final static Logger logger = LoggerFactory.getLogger(Operator.class);

    private final CmPredicate cmPredicate;

    private KubernetesClient client;

    public K8sImpl(KubernetesClient client, CmPredicate cmPredicate) {
        this.client = client;
        this.cmPredicate = cmPredicate;
    }

    @Override
    public void createConfigMap(ConfigMap cm) {
        client.configMaps().create(cm);
    }

    @Override
    public void updateConfigMap(ConfigMap cm) {
        client.configMaps().createOrReplace(cm);
    }

    @Override
    public void deleteConfigMap(TopicName topicName) {
        // Delete the CM by the topic name, because neither ZK nor Kafka know the CM name
        client.configMaps().withField("name", topicName.toString()).delete();
    }

    @Override
    public List<ConfigMap> listMaps() {
        return client.configMaps().withLabels(cmPredicate.labels()).list().getItems();
    }

    @Override
    public ConfigMap getFromName(MapName mapName) {
        return client.configMaps().withName(mapName.toString()).get();
    }

    /**
     * Create the given k8s event
     */
    @Override
    public void createEvent(Event event) {
        try {
            //logger.debug("Creating event {}", event);
            //client.events().create(outcomeEvent);
        } catch (KubernetesClientException e) {
            logger.error("Error creating event {}", event, e);
        }
    }
}
