/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.securityprofiles;

import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.platform.PlatformFeatures;
import io.strimzi.plugin.security.profiles.PodSecurityProvider;
import io.strimzi.plugin.security.profiles.impl.BaselinePodSecurityProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Factory used to find and initialize the selected Pod Security Provider based on the configuration
 */
public class PodSecurityProviderFactory {
    private static final Logger LOGGER = LogManager.getLogger(PodSecurityProviderFactory.class);

    // The default value is set for Unit Tests which might be run without the factory being initialized
    private static PodSecurityProvider provider;
    static {
        initialize();
    }

    /**
     * Finds the configured provider or throws an exception if it is not found
     *
     * @param providerClass The class name of the provider which should be used
     *
     * @return  An instance of the configured PodSecurityProvider implementation
     */
    /* test */ static PodSecurityProvider findProviderOrThrow(String providerClass)   {
        ServiceLoader<PodSecurityProvider> loader = ServiceLoader.load(PodSecurityProvider.class);

        for (PodSecurityProvider provider : loader)  {
            if (providerClass.equals(provider.getClass().getCanonicalName()))   {
                LOGGER.info("Found PodSecurityProvider {}", providerClass);
                return provider;
            }
        }

        // The provider was not found
        LOGGER.warn("PodSecurityProvider {} was not found. Available providers are {}", providerClass, loader.stream().map(p -> p.getClass().getCanonicalName()).collect(Collectors.toSet()));
        throw new InvalidConfigurationException("PodSecurityProvider " + providerClass + " was not found.");
    }

    /**
     * Initializes the PodSecurityProvider factory. It finds and instantiates the implementation of the configured
     * provider and stores it internally so that it can be easily returned later when used.
     *
     * @param providerClass     Class of the PodSecurityProvider which should be used
     * @param platformFeatures  Platform features which should be used to configure the provider
     */
    public static void initialize(String providerClass, PlatformFeatures platformFeatures) {
        PodSecurityProviderFactory.provider = findProviderOrThrow(providerClass);
        LOGGER.info("Initializing PodSecurityProvider {}", providerClass);
        PodSecurityProviderFactory.provider.configure(platformFeatures);
    }

    /**
     * Initializes the PodSecurityProvider factory with the default values. The default values use the Baseline provider
     * with minimal supported version of Kubernetes.
     */
    public static void initialize() {
        provider = new BaselinePodSecurityProvider();
        provider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));
    }

    /**
     * @return Returns the initialized PodSecurityProvider instance
     */
    public static PodSecurityProvider getProvider()  {
        return provider;
    }
}
