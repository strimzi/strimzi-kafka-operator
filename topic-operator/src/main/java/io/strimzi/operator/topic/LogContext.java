/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.api.kafka.model.KafkaTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class LogContext {

    private final static Logger LOGGER = LogManager.getLogger(K8sTopicWatcher.class);

    private static AtomicInteger ctx = new AtomicInteger();
    private final String base;
    private final String trigger;
    private String resourceVersion;

    private LogContext(String trigger) {
        base = ctx.getAndIncrement() + "|" + trigger;
        this.trigger = trigger;
    }


    static LogContext zkWatch(String znode, String childAction) {
        return new LogContext(znode + " " + childAction);
    }

    static LogContext kubeWatch(Watcher.Action action, KafkaTopic kafkaTopic) {
        LogContext logContext = new LogContext("kube " + action(action) + kafkaTopic.getMetadata().getName());
        logContext.resourceVersion = kafkaTopic.getMetadata().getResourceVersion();
        return logContext;
    }

    private static String action(Watcher.Action action) {
        switch (action) {
            case ADDED:
                return "+";
            case MODIFIED:
                return "=";
            case DELETED:
                return "-";
        }
        return "!";
    }

    static LogContext periodic(String periodicType) {
        return new LogContext(periodicType);
    }

    public String trigger() {
        return trigger;
    }

    @Override
    public String toString() {
        if (resourceVersion == null) {
            return base;
        } else {
            return base + "|" + resourceVersion;
        }
    }

    public LogContext withKubeTopic(KafkaTopic kafkaTopic) {
        String newResourceVersion = kafkaTopic == null ? null : kafkaTopic.getMetadata().getResourceVersion();
        if (!Objects.equals(resourceVersion, newResourceVersion)) {
            LOGGER.debug("{}: Concurrent modification in kube: new version {}", this, newResourceVersion);
        }
        this.resourceVersion = newResourceVersion;
        return this;
    }
}
