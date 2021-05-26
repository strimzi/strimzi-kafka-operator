/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.client.Watcher;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class LogContext {

    private final static ReconciliationLogger LOGGER = ReconciliationLogger.create(K8sTopicWatcher.class);

    private static AtomicInteger ctx = new AtomicInteger();
    private final String base;
    private final String trigger;
    private final String namespace;
    private final String topicName;
    private String resourceVersion;

    private LogContext(String trigger, String namespace, String topicName) {
        base = ctx.getAndIncrement() + "|" + trigger;
        this.namespace = namespace;
        this.topicName = topicName;
        this.trigger = trigger;
    }


    static LogContext zkWatch(String znode, String childAction, String namespace, String topicName) {
        return new LogContext(znode + " " + childAction, namespace, topicName);
    }

    static LogContext kubeWatch(Watcher.Action action, KafkaTopic kafkaTopic) {
        LogContext logContext = new LogContext("kube " + action(action) + kafkaTopic.getMetadata().getName(), kafkaTopic.getMetadata().getNamespace(), kafkaTopic.getMetadata().getName());
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

    static LogContext periodic(String periodicType, String namespace, String topicName) {
        return new LogContext(periodicType, namespace, topicName);
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

    public Marker getMarker() {
        String marker = "KafkaTopic(" + namespace + "/" + topicName + ")";
        LOGGER.traceOp("marker is {}", marker);
        return MarkerManager.getMarker(marker);
    }

    public LogContext withKubeTopic(KafkaTopic kafkaTopic) {
        String newResourceVersion = kafkaTopic == null ? null : kafkaTopic.getMetadata().getResourceVersion();
        if (!Objects.equals(resourceVersion, newResourceVersion)) {
            LOGGER.debugOp("{}: Concurrent modification in kube: new version {}", this, newResourceVersion);
        }
        this.resourceVersion = newResourceVersion;
        return this;
    }

    public Reconciliation toReconciliation() {
        return new Reconciliation(trigger, "KafkaTopic", namespace, topicName);
    }
}
