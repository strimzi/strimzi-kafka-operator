/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

/**
 * Provides the built-in implementations of the PodSecurityProvider plugins:
 *     - BaselinePodSecurityProvider which is the default implementation and provides security contexts which match
 *       the baseline Kubernetes security profile
 *     - RestrictedPodSecurityProvider which is an optional implementation and provides security contexts which match
 *       the restricted Kubernetes security profile
 *
 * @since 0.31.0
 */
package io.strimzi.plugin.security.profiles.impl;