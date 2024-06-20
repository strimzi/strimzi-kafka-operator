/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.model;

import java.util.List;
import java.util.stream.Stream;

/**
 * Holder for some batch computation result partitioned by error.
 * Each list element is a {@link Pair} where the key contains the input, and the value contains the computation result.
 * The value can be {@link Either} a successful computation result (right), or an error instance (left).
 *      
 * @param okList Success result list.
 * @param errorsList Error result list.
 * @param <K> Type of key.
 * @param <V> Type of value.
 */
public record PartitionedByError<K, V>(List<Pair<K, Either<TopicOperatorException, V>>> okList,
                                       List<Pair<K, Either<TopicOperatorException, V>>> errorsList) {
    /**
     * @return Success result list.
     */
    public Stream<Pair<K, V>> ok() {
        return okList.stream().map(x -> new Pair<>(x.getKey(), x.getValue().right()));
    }

    /**
     * @return Error result list.
     */
    public Stream<Pair<K, TopicOperatorException>> errors() {
        return errorsList.stream().map(x -> new Pair<>(x.getKey(), x.getValue().left()));
    }
}
