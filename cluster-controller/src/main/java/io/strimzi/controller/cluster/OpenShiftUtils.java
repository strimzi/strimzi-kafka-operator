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

public class OpenShiftUtils {
    private static final Logger log = LoggerFactory.getLogger(OpenShiftUtils.class.getName());

    private final OpenShiftClient client;

    public OpenShiftUtils(OpenShiftClient client) {
        this.client = client;
    }

    public OpenShiftClient getOpenShiftClient() {
        return client;
    }

    public void create(HasMetadata res) {
        log.info("Creating {} {}", res.getClass().getSimpleName(), res.getMetadata().getName());

        if (res instanceof BuildConfig) {
            client.buildConfigs().createOrReplace((BuildConfig)res);
        } else if (res instanceof ImageStream) {
            client.imageStreams().createOrReplace((ImageStream)res);
        } else {
            throw new RuntimeException("Unsupported type " + res.getClass().getSimpleName());
        }
    }

    public boolean exists(String namespace, String name, Class<? extends HasMetadata> type) {
        log.info("Checking if {} {} exists in namespace {}", type.getSimpleName(), name, namespace);
        return get(namespace, name, type) == null ? false : true;
    }

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

    public void delete(String namespace, String name, Class<? extends HasMetadata> type) {
        log.info("Deleting {} {} from namespace {}", type.getSimpleName(), name, namespace);

        if (type == BuildConfig.class) {
            client.buildConfigs().inNamespace(namespace).withName(name).delete();
        } else if (type == ImageStream.class) {
            client.imageStreams().inNamespace(namespace).withName(name).delete();
        } else {
            throw new RuntimeException("Unsupported type " + type.getSimpleName());
        }
    }
}
