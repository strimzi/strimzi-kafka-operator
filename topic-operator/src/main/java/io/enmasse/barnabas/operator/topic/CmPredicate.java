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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class CmPredicate implements Predicate<ConfigMap> {

    private final Map<String, String> labels;

    public CmPredicate(Map<String, String> labels) {
        this.labels = labels;
    }

    public CmPredicate(String... labels) {
        if (labels.length % 2 != 0) {
            throw new IllegalArgumentException();
        }
        this.labels = new HashMap<>(labels.length/2);
        for (int i = 0; i < labels.length; i+=2) {
            this.labels.put(labels[i], labels[i+1]);
        }
    }

    public Map<String, String> labels() {
        return labels;
    }

    @Override
    public boolean test(ConfigMap configMap) {
        Map<String, String> mapLabels = configMap.getMetadata().getLabels();
        for (Map.Entry<String,String> entry : labels.entrySet()) {
            if (!entry.getValue().equals(mapLabels.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
