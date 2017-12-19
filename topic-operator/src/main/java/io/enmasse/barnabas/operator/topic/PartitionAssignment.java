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

import java.util.List;
import java.util.Map;

/**
 * Abstracts how partitions are assigned to brokers.
 * This abstraction is necessary as in some deployments of Barnabas there will be a
 * "cluster controller" which will be able to figure out a "good enough" assignment.
 * In other deployments we might leave it to the Kafka broker to determine the assignment.
 */
public interface PartitionAssignment {
    // TODO write a ClusterBalancedPartitionAssignment

    // TODO We could support configuring this in the config map
    // via a ConfiguredPartitionAssignment

    /**
     * Return an assignment of partitions to replicating brokers for a topics which is having more partitions created,
     * or null to let the Kafka cluster controller decide.
     * The first broker in the list
     * is the preferred leader for the partition.
     * There should {@link Topic#getNumPartitions() topic.getNumPartitions()}} entries in the returned map,
     * and each list in the returned map should have length
     * {@link Topic#getNumReplicas()}  topic.getNumReplicas()}}, and contain no duplicates.
     */
    List<List<Integer>> newPartitions(Topic topic, int numNewPartitions);

    /**
     * Return an assignment of partitions to replicating brokers for a new topic,
     * or null to let the Kafka cluster controller decide.
     * The first broker in the list
     * is the preferred leader for the partition.
     * There should {@link Topic#getNumPartitions() topic.getNumPartitions()}} entries in the returned map,
     * and each list in the returned map should have length
     * {@link Topic#getNumReplicas()}  topic.getNumReplicas()}}, and contain no duplicates.
     */
    Map<Integer, List<Integer>> newTopic(Topic topic);

    Map<Integer,List<Integer>> changeReplicationFactor(Topic result);
}
