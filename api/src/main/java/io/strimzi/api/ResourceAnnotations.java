/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api;

/**
 * Class for holding some annotation keys.
 */
public class ResourceAnnotations {
    /**
     * The Strimzi domain used in all annotations
     */
    public static final String STRIMZI_DOMAIN = "strimzi.io/";

    /**
     * Annotation used to force rebuild of the container image even if the
     * dockerfile did not change
     */
    public static final String STRIMZI_IO_CONNECT_FORCE_REBUILD = STRIMZI_DOMAIN + "force-rebuild";

    /**
     * Annotation used to pause resource reconciliation
     */
    public static final String ANNO_STRIMZI_IO_PAUSE_RECONCILIATION = STRIMZI_DOMAIN + "pause-reconciliation";

    /**
     * Annotation to trigger manually rolling updats
     */
    public static final String ANNO_STRIMZI_IO_MANUAL_ROLLING_UPDATE = STRIMZI_DOMAIN + "manual-rolling-update";

    /**
     * This annotation with related possible values (approve, stop, refresh) is set
     * by the user for interacting
     * with the rebalance operator in order to start, stop, or refresh rebalancing
     * proposals and operations.
     */
    public static final String ANNO_STRIMZI_IO_REBALANCE = STRIMZI_DOMAIN + "rebalance";

    /**
     * Use this boolean annotation to auto-approve a rebalance optimization proposal
     * without the need for the
     * manual approval by applying the strimzi.io/rebalance=approve annotation
     */
    public static final String ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL = STRIMZI_DOMAIN + "rebalance-auto-approval";

    /**
     * Annotation for restarting Mirror Maker 2 connector
     */
    public static final String ANNO_STRIMZI_IO_RESTART_CONNECTOR = STRIMZI_DOMAIN + "restart-connector";

    /**
     * Annotation for restarting KafkaConnector task
     */
    public static final String ANNO_STRIMZI_IO_RESTART_TASK = STRIMZI_DOMAIN + "restart-task";

    /**
     * Annotation for restarting Mirror Maker 2 connector task
     */
    public static final String ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK = STRIMZI_DOMAIN + "restart-connector-task";

    /**
     * Key for specifying which Mirror Maker 2 connector should be restarted
     */
    public static final String ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_CONNECTOR = "connector";

    /**
     * Key for specifying which Mirror Maker 2 connector task should be restarted
     */
    public static final String ANNO_STRIMZI_IO_RESTART_CONNECTOR_TASK_PATTERN_TASK = "task";

    /**
     * Annotation for configuring the ranges of node IDs which should be used for
     * given node pool
     */
    public static final String ANNO_STRIMZI_IO_NEXT_NODE_IDS = STRIMZI_DOMAIN + "next-node-ids";

    /**
     * Annotation for configuring the ranges of node IDs which should be used for
     * given node pool
     */
    public static final String ANNO_STRIMZI_IO_REMOVE_NODE_IDS = STRIMZI_DOMAIN + "remove-node-ids";

    /**
     * Annotation for enabling or disabling the Node Pools. This annotation is used
     * on the Kafka CR
     */
    public static final String ANNO_STRIMZI_IO_NODE_POOLS = STRIMZI_DOMAIN + "node-pools";

    /**
     * Annotation key for deleting both a Pod and a related PVC
     */
    public static final String ANNO_STRIMZI_IO_DELETE_POD_AND_PVC = STRIMZI_DOMAIN + "delete-pod-and-pvc";

    /**
     * Annotation for requesting a renewal of the CA and rolling it out
     */
    public static final String ANNO_STRIMZI_IO_FORCE_RENEW = STRIMZI_DOMAIN + "force-renew";

    /**
     * Annotation for requesting a brand-new CA to be generated and rolled out
     */
    public static final String ANNO_STRIMZI_IO_FORCE_REPLACE = STRIMZI_DOMAIN + "force-replace";

    /**
     * Annotation used to skip the check on broker scale-down
     */
    public static final String ANNO_STRIMZI_IO_SKIP_BROKER_SCALEDOWN_CHECK = STRIMZI_DOMAIN + "skip-broker-scaledown-check";

    /**
     * Annotation for defining a cluster as KRaft (enabled) or ZooKeeper (disabled) based.
     * This annotation is used on the Kafka CR
     * If missing or with an invalid value, the cluster is assumed to be ZooKeeper-based
     */
    public static final String ANNO_STRIMZI_IO_KRAFT = STRIMZI_DOMAIN + "kraft";
}
