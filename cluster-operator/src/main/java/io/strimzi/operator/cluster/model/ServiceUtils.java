/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.strimzi.api.kafka.model.template.HasMetadataTemplate;
import io.strimzi.api.kafka.model.template.InternalServiceTemplate;
import io.strimzi.api.kafka.model.template.IpFamily;
import io.strimzi.api.kafka.model.template.IpFamilyPolicy;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Labels;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Shared methods for working with Services
 */
public class ServiceUtils {
    /**
     * Creates a ClusterIP Service
     *
     * @param name           Name of the Service
     * @param namespace      Namespace of the Service
     * @param labels         Labels of the Service
     * @param ownerReference OwnerReference of the Service
     * @param template       Service template with user's custom configuration
     * @param ports          List of service ports
     *
     * @return  New ClusterIP Service
     */
    public static Service createClusterIpService(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            InternalServiceTemplate template,
            List<ServicePort> ports
    )   {
        return createService(
                name,
                namespace,
                labels,
                ownerReference,
                template,
                ports,
                labels.strimziSelectorLabels(),
                "ClusterIP",
                null,
                null,
                ipFamilyPolicy(template),
                ipFamilies(template)
        );
    }

    /**
     * Creates a discoverable ClusterIP Service. discoverable service has some additional labels and annotations to
     * simplify discovery.
     *
     * @param name                  Name of the Service
     * @param namespace             Namespace of the Service
     * @param labels                Labels of the Service
     * @param ownerReference        OwnerReference of the Service
     * @param template              Service template with user's custom configuration
     * @param ports                 List of service ports
     * @param discoveryLabels       Additional discovery labels
     * @param discoveryAnnotations  Additional discovery annotations
     *
     * @return  New discoverable ClusterIP Service
     */
    public static Service createDiscoverableClusterIpService(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            InternalServiceTemplate template,
            List<ServicePort> ports,
            Map<String, String> discoveryLabels,
            Map<String, String> discoveryAnnotations
    )   {
        return createService(
                name,
                namespace,
                labels.withStrimziDiscovery(),
                ownerReference,
                template,
                ports,
                labels.strimziSelectorLabels(),
                "ClusterIP",
                discoveryLabels,
                discoveryAnnotations,
                ipFamilyPolicy(template),
                ipFamilies(template)
        );
    }

    /**
     * Creates a service
     *
     * @param name                  Name of the Service
     * @param namespace             Namespace of the Service
     * @param labels                Labels of the Service
     * @param ownerReference        OwnerReference of the Service
     * @param template              Template with user's custom metadata for this service
     * @param ports                 List of service ports
     * @param selector              Selector for selecting the Pods to route the traffic to
     * @param type                  Type of the service
     * @param additionalLabels      Additional labels
     * @param additionalAnnotations Additional annotations
     * @param ipFamilyPolicy        IP Family Policy configuration
     * @param ipFamilies            List of IP familiers
     *
     * @return  New Service
     */
    public static Service createService(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            HasMetadataTemplate template,
            List<ServicePort> ports,
            Labels selector,
            String type,
            Map<String, String> additionalLabels,
            Map<String, String> additionalAnnotations,
            IpFamilyPolicy ipFamilyPolicy,
            List<IpFamily> ipFamilies
    )   {
        return new ServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withAdditionalLabels(Util.mergeLabelsOrAnnotations(additionalLabels, TemplateUtils.labels(template))).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(Util.mergeLabelsOrAnnotations(additionalAnnotations, TemplateUtils.annotations(template)))
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withType(type)
                    .withSelector(selector.toMap())
                    .withPorts(ports)
                    .withIpFamilyPolicy(ipFamilyPolicyToString(ipFamilyPolicy))
                    .withIpFamilies(ipFamiliesToListOfStrings(ipFamilies))
                .endSpec()
                .build();
    }

    /**
     * Creates a headless service
     *
     * @param name              Name of the Service
     * @param namespace         Namespace of the Service
     * @param labels            Labels of the Service
     * @param ownerReference    OwnerReference of the Service
     * @param template          Service template with user's custom configuration
     * @param ports             List of service ports
     *
     * @return  New headless service
     */
    public static Service createHeadlessService(
            String name,
            String namespace,
            Labels labels,
            OwnerReference ownerReference,
            InternalServiceTemplate template,
            List<ServicePort> ports
    )   {
        return new ServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withLabels(labels.withAdditionalLabels(TemplateUtils.labels(template)).toMap())
                    .withNamespace(namespace)
                    .withAnnotations(TemplateUtils.annotations(template))
                    .withOwnerReferences(ownerReference)
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIP")
                    .withClusterIP("None")
                    .withSelector(labels.strimziSelectorLabels().toMap())
                    .withPorts(ports)
                    .withPublishNotReadyAddresses(true)
                    .withIpFamilyPolicy(ipFamilyPolicyToString(ipFamilyPolicy(template)))
                    .withIpFamilies(ipFamiliesToListOfStrings(ipFamilies(template)))
                .endSpec()
                .build();
    }

    private static String ipFamilyPolicyToString(IpFamilyPolicy ipFamilyPolicy)  {
        return ipFamilyPolicy != null ? ipFamilyPolicy.toValue() : null;
    }

    private static List<String> ipFamiliesToListOfStrings(List<IpFamily> ipFamilies)  {
        return ipFamilies != null ? ipFamilies.stream().map(IpFamily::toValue).collect(Collectors.toList()) : null;
    }

    private static IpFamilyPolicy ipFamilyPolicy(InternalServiceTemplate template)  {
        return template != null ? template.getIpFamilyPolicy() : null;
    }

    private static List<IpFamily> ipFamilies(InternalServiceTemplate template)  {
        return template != null ? template.getIpFamilies() : null;
    }

    /**
     * Creates Service port
     *
     * @param name          Name of the port
     * @param port          The port on the service which can be accessed by clients
     * @param targetPort    The port on the container / Pod where the connections will be routed
     * @param protocol      Protocol used by this port
     *
     * @return  Created port
     */
    public static ServicePort createServicePort(String name, int port, int targetPort, String protocol)   {
        return createServicePort(name, port, targetPort, null, protocol);
    }

    /**
     * Creates Service port with a specific node port
     *
     * @param name          Name of the port
     * @param port          The port on the service which can be accessed by clients
     * @param targetPort    The port on the container / Pod where the connections will be routed
     * @param nodePort      The desired node port number
     * @param protocol      Protocol used by this port
     *
     * @return  Created port
     */
    public static ServicePort createServicePort(String name, int port, int targetPort, Integer nodePort, String protocol)   {
        return new ServicePortBuilder()
                .withName(name)
                .withProtocol(protocol)
                .withPort(port)
                .withNewTargetPort(targetPort)
                .withNodePort(nodePort)
                .build();
    }
}
