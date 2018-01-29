package io.strimzi.controller.cluster;

import io.fabric8.kubernetes.api.builder.BaseFluent;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.DoneableConfigMap;
import io.fabric8.kubernetes.api.model.DoneablePersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.DoneableService;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.extensions.Deployment;
import io.fabric8.kubernetes.api.model.extensions.DoneableDeployment;
import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.Patchable;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.fabric8.openshift.api.model.BuildConfig;
import io.fabric8.openshift.api.model.ImageStream;
import io.fabric8.openshift.client.OpenShiftClient;
import io.fabric8.openshift.client.dsl.BuildConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Utility class for handling OpenShift resources
 */
public class OpenShiftUtils {
    private static final Logger log = LoggerFactory.getLogger(OpenShiftUtils.class.getName());

    private final OpenShiftClient client;

    /**
     * Constructor
     *
     * @param client    Fabric8 OpenShift client
     */
    public OpenShiftUtils(OpenShiftClient client) {
        this.client = client;
    }

    /**
     * Get Fabric8 OpenShift client
     *
     * @return  Fabric8 OpenShift client for direct use
     */
    public OpenShiftClient getOpenShiftClient() {
        return client;
    }

    /**
     * Get OpenShift resource definition
     *
     * @param namespace     OpenShift project
     * @param name          Resource name
     * @param type          Resource type
     * @return              OpenShift resource definition
     */
    public HasMetadata get(String namespace, String name, Class<? extends HasMetadata> type) {
        log.info("Getting {} {} from namespace {}", type.getSimpleName(), name, namespace);

        if (type == BuildConfig.class) {
            return client.buildConfigs().inNamespace(namespace).withName(name).get();
        } else if (type == ImageStream.class) {
            return client.imageStreams().inNamespace(namespace).withName(name).get();
        } else {
            throw new RuntimeException("Unsupported type " + type.getSimpleName());
        }
    }

    /**
     * Get OpenShift resource
     *
     * @param namespace     OpenShift project
     * @param name          Resource name
     * @param type          Resource type
     * @return              OpenShift Resource
     */
    public Resource<? extends HasMetadata, ? extends BaseFluent<? extends BaseFluent<?>>> getResource(String namespace, String name, Class<? extends HasMetadata> type) {
        log.info("Getting resource {} {} from namespace {}", type.getSimpleName(), name, namespace);

        if (type == BuildConfig.class) {
            return client.buildConfigs().inNamespace(namespace).withName(name);
        } else if (type == ImageStream.class) {
            return client.imageStreams().inNamespace(namespace).withName(name);
        } else {
            throw new RuntimeException("Unsupported type " + type.getSimpleName());
        }
    }
}
