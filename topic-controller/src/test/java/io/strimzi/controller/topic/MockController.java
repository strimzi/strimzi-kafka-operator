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

package io.strimzi.controller.topic;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;

class MockController extends Controller {

    public MockController() {
        super(null, null, null, null, null);
    }

    static class Event {
        private final Event.Type type;

        static enum Type {
            CREATE,
            DELETE,
            MODIFY
        }
        private final TopicName topicName;
        private final ConfigMap configMap;

        public Event(Event.Type type, TopicName topicName) {
            this.type = type;
            this.topicName = topicName;
            this.configMap = null;
        }

        public Event(Event.Type type, ConfigMap configMap) {
            this.type = type;
            this.topicName = null;
            this.configMap = configMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Event event = (Event) o;

            if (type != event.type) return false;
            if (topicName != null ? !topicName.equals(event.topicName) : event.topicName != null)
                return false;
            return configMap != null ? configMap.equals(event.configMap) : event.configMap == null;
        }

        @Override
        public int hashCode() {
            int result = type.hashCode();
            result = 31 * result + (topicName != null ? topicName.hashCode() : 0);
            result = 31 * result + (configMap != null ? configMap.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "type=" + type +
                    ", topicName=" + topicName +
                    ", configMap=" + configMap +
                    '}';
        }
    }

    public AsyncResult<Void> topicCreatedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName()+".topicCreatedResult");
    public AsyncResult<Void> topicDeletedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName()+".topicDeletedResult");
    public AsyncResult<Void> topicModifiedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName()+".topicModifiedResult");
    public AsyncResult<Void> cmAddedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName()+".cmAddedResult");
    public AsyncResult<Void> cmDeletedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName()+".cmDeletedResult");
    public AsyncResult<Void> cmModifiedResult = Future.failedFuture("Unexpected mock interaction. Configure " + getClass().getSimpleName()+".cmModifiedResult");
    private List<Event> events = new ArrayList<>();

    public List<Event> getEvents() {
        return events;
    }

    @Override
    public void onTopicCreated(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        events.add(new Event(Event.Type.CREATE, topicName));
        handler.handle(topicCreatedResult);
    }

    @Override
    public void onTopicDeleted(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        events.add(new Event(Event.Type.DELETE, topicName));
        handler.handle(topicDeletedResult);
    }

    @Override
    public void onTopicConfigChanged(TopicName topicName, Handler<AsyncResult<Void>> handler) {
        events.add(new Event(Event.Type.MODIFY, topicName));
        handler.handle(topicModifiedResult);
    }

    @Override
    public void onConfigMapAdded(ConfigMap cm, Handler<AsyncResult<Void>> handler) {
        events.add(new Event(Event.Type.CREATE, cm));
        handler.handle(cmAddedResult);
    }

    @Override
    public void onConfigMapModified(ConfigMap cm, Handler<AsyncResult<Void>> handler) {
        events.add(new Event(Event.Type.MODIFY, cm));
        handler.handle(cmModifiedResult);
    }

    @Override
    public void onConfigMapDeleted(ConfigMap cm, Handler<AsyncResult<Void>> handler) {
        events.add(new Event(Event.Type.DELETE, cm));
        handler.handle(cmDeletedResult);
    }
}
