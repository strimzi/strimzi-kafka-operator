/*
 * Copyright 2018 Red Hat Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Event;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

public interface K8s {

    void createConfigMap(ConfigMap cm, Handler<AsyncResult<Void>> handler);

    void updateConfigMap(ConfigMap cm, Handler<AsyncResult<Void>> handler);

    void deleteConfigMap(MapName mapName, Handler<AsyncResult<Void>> handler);

    void listMaps(Handler<AsyncResult<List<ConfigMap>>> handler);

    /**
     * Get the ConfigMap with the given name, invoking the given handler with the result.
     * If a ConfigMap with the given name does not exist, the handler will be called with
     * a null {@link AsyncResult#result() result()}.
     */
    void getFromName(MapName mapName, Handler<AsyncResult<ConfigMap>> handler);

    void createEvent(Event event, Handler<AsyncResult<Void>> handler);
}
