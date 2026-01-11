/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import io.strimzi.api.kafka.model.common.ConnectorState;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Spec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2TargetClusterSpec;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2TargetClusterSpecBuilder;
import io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException;

import java.util.HashMap;
import java.util.Map;

/**
 * Class for holding the various conversions specific for the KafkaMirrorMaker2 API conversion
 */
@SuppressWarnings("deprecation")
public class MirrorMaker2Conversions {
    private MirrorMaker2Conversions() { }

    /**
     * Checks for the presence of OAuth authentication in the KafkaMirrorMaker2 resource and its clusters. If OAuth
     * authentication is found, an error is raised and the user has to convert it manually.
     *
     * @return  The conversion
     */
    public static Conversion<KafkaMirrorMaker2> enforceOauth() {
        return Conversion.replace("/spec", new Conversion.DefaultConversionFunction<KafkaMirrorMaker2Spec>() {
            @Override
            Class<KafkaMirrorMaker2Spec> convertedType() {
                return KafkaMirrorMaker2Spec.class;
            }

            @Override
            public KafkaMirrorMaker2Spec apply(KafkaMirrorMaker2Spec spec) {
                if (spec == null) {
                    return null;
                }

                if (spec.getClusters() != null && !spec.getClusters().isEmpty()) {
                    for (KafkaMirrorMaker2ClusterSpec clusterSpec : spec.getClusters()) {
                        if (clusterSpec.getAuthentication() != null && clusterSpec.getAuthentication() instanceof KafkaClientAuthenticationOAuth)   {
                            throw new ApiConversionFailedException("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool.");
                        }
                    }
                }

                if (spec.getTarget() != null && spec.getTarget().getAuthentication() != null && spec.getTarget().getAuthentication() instanceof KafkaClientAuthenticationOAuth) {
                    throw new ApiConversionFailedException("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool.");
                }

                if (spec.getMirrors() != null && !spec.getMirrors().isEmpty()) {
                    for (KafkaMirrorMaker2MirrorSpec mirror : spec.getMirrors()) {
                        if (mirror.getSource() != null && mirror.getSource().getAuthentication() != null && mirror.getSource().getAuthentication() instanceof KafkaClientAuthenticationOAuth)   {
                            throw new ApiConversionFailedException("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool.");
                        }
                    }
                }

                return spec;
            }
        });
    }

    /**
     * Converts the blacklisted blacklist fields to the excluded fields in the KafkaMirrorMaker2 resource and its mirror
     * definitions.
     *
     * @return  The conversion
     */
    public static Conversion<KafkaMirrorMaker2> excludedFields() {
        return Conversion.replace("/spec", new Conversion.DefaultConversionFunction<KafkaMirrorMaker2Spec>() {
            @Override
            Class<KafkaMirrorMaker2Spec> convertedType() {
                return KafkaMirrorMaker2Spec.class;
            }

            @Override
            public KafkaMirrorMaker2Spec apply(KafkaMirrorMaker2Spec spec) {
                if (spec == null) {
                    return null;
                }

                if (spec.getMirrors() != null && !spec.getMirrors().isEmpty()) {
                    for (KafkaMirrorMaker2MirrorSpec mirror : spec.getMirrors()) {
                        if (mirror.getTopicsBlacklistPattern() != null) {
                            if (mirror.getTopicsExcludePattern() == null) {
                                mirror.setTopicsExcludePattern(mirror.getTopicsBlacklistPattern());
                            }

                            mirror.setTopicsBlacklistPattern(null);
                        }

                        if (mirror.getGroupsBlacklistPattern() != null) {
                            if (mirror.getGroupsExcludePattern() == null) {
                                mirror.setGroupsExcludePattern(mirror.getGroupsBlacklistPattern());
                            }

                            mirror.setGroupsBlacklistPattern(null);
                        }
                    }
                }

                return spec;
            }
        });
    }

    /**
     * Converts the pause field to the state field in the KafkaMirrorMaker2 resource and its mirror definitions.
     *
     * @return  The conversion
     */
    public static Conversion<KafkaMirrorMaker2> pauseToState() {
        return Conversion.replace("/spec", new Conversion.DefaultConversionFunction<KafkaMirrorMaker2Spec>() {
            @Override
            Class<KafkaMirrorMaker2Spec> convertedType() {
                return KafkaMirrorMaker2Spec.class;
            }

            @Override
            public KafkaMirrorMaker2Spec apply(KafkaMirrorMaker2Spec spec) {
                if (spec == null) {
                    return null;
                }
                if (spec.getMirrors() != null && !spec.getMirrors().isEmpty()) {
                    for (KafkaMirrorMaker2MirrorSpec mirror : spec.getMirrors()) {
                        if (mirror.getSourceConnector() != null && mirror.getSourceConnector().getPause() != null) {
                            if (mirror.getSourceConnector().getPause() && mirror.getSourceConnector().getState() == null)  {
                                mirror.getSourceConnector().setState(ConnectorState.PAUSED);
                            }

                            mirror.getSourceConnector().setPause(null);
                        }

                        if (mirror.getCheckpointConnector() != null && mirror.getCheckpointConnector().getPause() != null) {
                            if (mirror.getCheckpointConnector().getPause() && mirror.getCheckpointConnector().getState() == null)  {
                                mirror.getCheckpointConnector().setState(ConnectorState.PAUSED);
                            }

                            mirror.getCheckpointConnector().setPause(null);
                        }
                    }
                }

                return spec;
            }
        });
    }

    /**
     * Removes the heartbeat connector from the KafkaMirrorMaker2 resource and its mirror definitions.
     *
     * @return  The conversion
     */
    public static Conversion<KafkaMirrorMaker2> removeHeartbeatConnector() {
        return Conversion.replace("/spec", new Conversion.DefaultConversionFunction<KafkaMirrorMaker2Spec>() {
            @Override
            Class<KafkaMirrorMaker2Spec> convertedType() {
                return KafkaMirrorMaker2Spec.class;
            }

            @Override
            public KafkaMirrorMaker2Spec apply(KafkaMirrorMaker2Spec spec) {
                if (spec == null) {
                    return null;
                }

                if (spec.getMirrors() != null && !spec.getMirrors().isEmpty()) {
                    for (KafkaMirrorMaker2MirrorSpec mirror : spec.getMirrors()) {
                        if (mirror.getHeartbeatConnector() != null) {
                            mirror.setHeartbeatConnector(null);
                        }
                    }
                }

                return spec;
            }
        });
    }

    /**
     * Restructures the KafkaMirrorMaker2 resource and its source and target cluster definitions.
     *
     * @return  The conversion
     */
    public static Conversion<KafkaMirrorMaker2> mm2SpecRestructuring() {
        return Conversion.replace("/spec", new Conversion.DefaultConversionFunction<KafkaMirrorMaker2Spec>() {
            @Override
            Class<KafkaMirrorMaker2Spec> convertedType() {
                return KafkaMirrorMaker2Spec.class;
            }

            @Override
            public KafkaMirrorMaker2Spec apply(KafkaMirrorMaker2Spec spec) {
                if (spec == null) {
                    return null;
                }

                String targetAlias; // Needed later to validate the mirrors

                if (spec.getTarget() == null) {
                    if (spec.getClusters() == null || spec.getClusters().isEmpty() || spec.getConnectCluster() == null) {
                        throw new ApiConversionFailedException("KafkaMirrorMaker2 resource seems to be missing the target cluster definition in both the new and old format. Please fix the resource manually and re-run the conversion tool.");
                    }

                    targetAlias = spec.getConnectCluster();
                    KafkaMirrorMaker2ClusterSpec oldTargetCluster = spec.getClusters().stream().filter(c -> spec.getConnectCluster().equals(c.getAlias())).findFirst().orElseThrow(() -> new ApiConversionFailedException(".spec.connectCluster not found in .spec.clusters. Please fix the resource manually and re-run the conversion tool."));
                    Map<String, Object> config = new HashMap<>();
                    if (oldTargetCluster.getConfig() != null) {
                        config = oldTargetCluster.getConfig();
                    }

                    KafkaMirrorMaker2TargetClusterSpec target = new KafkaMirrorMaker2TargetClusterSpecBuilder()
                            .withAlias(oldTargetCluster.getAlias())
                            .withBootstrapServers(oldTargetCluster.getBootstrapServers())
                            .withGroupId(config.getOrDefault("group.id", "mirrormaker2-cluster").toString())
                            .withConfigStorageTopic(config.getOrDefault("config.storage.topic", "mirrormaker2-cluster-configs").toString())
                            .withOffsetStorageTopic(config.getOrDefault("offset.storage.topic", "mirrormaker2-cluster-offsets").toString())
                            .withStatusStorageTopic(config.getOrDefault("status.storage.topic", "mirrormaker2-cluster-status").toString())
                            .withTls(oldTargetCluster.getTls())
                            .withAuthentication(oldTargetCluster.getAuthentication())
                            .build();

                    config.remove("group.id");
                    config.remove("config.storage.topic");
                    config.remove("status.storage.topic");
                    config.remove("offset.storage.topic");

                    // We set config only here so that we remove the old configuration options above if needed
                    target.setConfig(config);
                    spec.setTarget(target);
                } else {
                    targetAlias = spec.getTarget().getAlias();
                }

                if (spec.getMirrors() != null && !spec.getMirrors().isEmpty()) {
                    for (KafkaMirrorMaker2MirrorSpec mirror : spec.getMirrors()) {
                        if (mirror.getTargetCluster() != null && !mirror.getTargetCluster().equals(targetAlias)) {
                            throw new ApiConversionFailedException("KafkaMirrorMaker2 resource has different Connect and Target clusters. Please fix the resource manually and re-run the conversion tool.");
                        }

                        if (mirror.getSource() == null) {
                            if (spec.getClusters() == null || spec.getClusters().isEmpty() || mirror.getSourceCluster() == null) {
                                throw new ApiConversionFailedException("KafkaMirrorMaker2 resource seems to be missing the source cluster definition in both the new and old format. Please fix the resource manually and re-run the conversion tool.");
                            }

                            KafkaMirrorMaker2ClusterSpec oldSourceCluster = spec.getClusters().stream().filter(c -> mirror.getSourceCluster().equals(c.getAlias())).findFirst().orElseThrow(() -> new ApiConversionFailedException(".spec.mirrors[].sourceCluster not found in .spec.clusters. Please fix the resource manually and re-run the conversion tool."));
                            mirror.setSource(oldSourceCluster);
                        }

                        mirror.setSourceCluster(null);
                        mirror.setTargetCluster(null);
                    }
                }

                spec.setConnectCluster(null);
                spec.setClusters(null);

                return spec;
            }
        });
    }
}
