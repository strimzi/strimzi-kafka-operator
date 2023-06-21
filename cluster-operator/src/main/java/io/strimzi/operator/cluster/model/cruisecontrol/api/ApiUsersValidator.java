/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.cruisecontrol.api;

import io.strimzi.api.kafka.model.balancing.ApiUser;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.CruiseControl.API_ADMIN_NAME;
import static io.strimzi.operator.cluster.model.CruiseControl.API_HEALTHCHECK_NAME;

/**
 * Util methods for validating Cruise Control API Users
 */
public class ApiUsersValidator {
    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ApiUsersValidator.class.getName());
    private final static List<String> FORBIDDEN_NAMES = List.of(API_ADMIN_NAME, API_HEALTHCHECK_NAME);

    /**
     * Validated the API users configuration. If the configuration is not valid, InvalidResourceException will be thrown.
     *
     * @param reconciliation The reconciliation
     * @param apiUsers       API users which should be validated
     */
    public static void validate(Reconciliation reconciliation, List<ApiUser> apiUsers) throws InvalidResourceException {
        Set<String> errors = validateAndGetErrorMessages(apiUsers);

        if (!errors.isEmpty())  {
            LOGGER.errorCr(reconciliation, "Cruise Control API users configuration is not valid: {}", errors);
            throw new InvalidResourceException("Cruise Control API user configuration is not valid: " + errors);
        }
    }

    /*test*/ static Set<String> validateAndGetErrorMessages(List<ApiUser> apiUsers) {
        Set<String> errors = new HashSet<>(0);
        List<String> names = getNames(apiUsers);

        if (names.size() != apiUsers.size())   {
            errors.add("every API user needs to have a unique name " +  names.size() + " " + apiUsers.size());
        }
        for (String name : names) {
            if (FORBIDDEN_NAMES.contains(name)) {
                errors.add("name " + name + " is forbidden and cannot be used");
            }
        }
        return errors;
    }

    /**
     * Extracts all distinct API usernames from the API users
     *
     * @param apiUsers List of all API users
     * @return         List of used names
     */
    public static List<String> getNames(List<ApiUser> apiUsers)    {
        return apiUsers.stream().map(ApiUser::getName).distinct().collect(Collectors.toList());
    }
}
