/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic.model;

/**
 * A key-value pair.
 * We can't use Map.entry because it forbids null values, which we want to allow.
 * @param <K> Type of key.
 * @param <V> Type of value.
 */
public record Pair<K, V>(K getKey, V getValue) {
}
