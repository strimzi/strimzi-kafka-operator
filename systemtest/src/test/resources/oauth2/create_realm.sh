#!/usr/bin/env bash

USERNAME=$1
PASSWORD=$2
URL=$3

TOKEN=$(curl --insecure -X POST -d "client_id=admin-cli&client_secret=aGVsbG8td29ybGQtcHJvZHVjZXItc2VjcmV0&grant_type=password&username=$USERNAME&password=$PASSWORD" "https://$URL/auth/realms/master/protocol/openid-connect/token" | awk -F '\"' '{print $4}')

RESULT=$(curl -v --insecure "https://$URL/auth/admin/realms" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Postman-Token: 3a6cd746-03b5-46fe-a54a-014fc7c51983' \
  -H 'cache-control: no-cache' \
  -d '{
    "realm": "internal",
    "accessTokenLifespan": 300,
    "ssoSessionMaxLifespan": 32140800,
    "ssoSessionIdleTimeout": 32140800,
    "enabled": true,
    "sslRequired": "external",
    "users": [
        {
            "username": "alice",
            "enabled": true,
            "email": "alice@example.com",
            "credentials": [
                {
                    "type": "password",
                    "value": "alice-password"
                }
            ],
            "realmRoles": [
                "user"
            ],
            "clientRoles": {
                "kafka": [
                    "kafka-topic:superapp_*:owner"
                ]
            }
        },
        {
            "username": "admin",
            "enabled": true,
            "email": "admin@example.com",
            "credentials": [
                {
                    "type": "password",
                    "value": "admin-password"
                }
            ],
            "realmRoles": [
                "admin"
            ],
            "clientRoles": {
                "realm-management": [
                    "realm-admin"
                ],
                "kafka": [
                    "kafka-admin"
                ]
            }
        },
        {
            "username": "service-account-kafka-broker",
            "enabled": true,
            "email": "service-account-kafka-broker@placeholder.org",
            "serviceAccountClientId": "kafka-broker",
            "clientRoles": {
                "kafka": [
                    "kafka-admin"
                ]
            }
        },
        {
            "username": "service-account-kafka-producer",
            "enabled": true,
            "email": "service-account-kafka-producer@placeholder.org",
            "serviceAccountClientId": "kafka-producer"
        },
        {
            "username": "service-account-kafka-consumer",
            "enabled": true,
            "email": "service-account-kafka-consumer@placeholder.org",
            "serviceAccountClientId": "kafka-consumer"
        },
        {
            "username": "service-account-kafka-connect",
            "enabled": true,
            "email": "service-account-kafka-connect@placeholder.org",
            "serviceAccountClientId": "kafka-connect"
        },
        {
            "username": "service-account-kafka-bridge",
            "enabled": true,
            "email": "service-account-kafka-bridge@placeholder.org",
            "serviceAccountClientId": "kafka-bridge"
        },
        {
            "username": "service-account-kafka-mirror-maker",
            "enabled": true,
            "email": "service-account-kafka-mirror-maker@placeholder.org",
            "serviceAccountClientId": "kafka-mirror-maker"
        },
        {
            "username": "service-account-kafka-mirror-maker-2",
            "enabled": true,
            "email": "service-account-kafka-mirror-maker-2@placeholder.org",
            "serviceAccountClientId": "kafka-mirror-maker-2"
        },
        {
            "username": "service-account-hello-world-producer",
            "enabled": true,
            "email": "service-account-hello-world-producer@placeholder.org",
            "serviceAccountClientId": "hello-world-producer"
        },
        {
            "username": "service-account-hello-world-consumer",
            "enabled": true,
            "email": "service-account-hello-world-consumer@placeholder.org",
            "serviceAccountClientId": "hello-world-consumer"
        },
        {
            "username": "service-account-hello-world-streams",
            "enabled": true,
            "email": "service-account-hello-world-streams@placeholder.org",
            "serviceAccountClientId": "hello-world-streams"
        }
    ],
    "roles": {
        "realm": [
            {
                "name": "user",
                "description": "User privileges"
            },
            {
                "name": "admin",
                "description": "Administrator privileges"
            }
        ],
        "client": {
            "kafka": [
                {
                    "name": "kafka-admin",
                    "description": "Kafka administrator - can perform any action on any Kafka resource",
                    "clientRole": true
                },
                {
                    "name": "kafka-topic:superapp_*:owner",
                    "description": "Owner of topics that begin with '\''superapp_'\'' prefix. Can perform any operation on these topics.",
                    "clientRole": true
                },
                {
                    "name": "kafka-topic:superapp_*:consumer",
                    "description": "Consumer of topics that begin with '\''superapp_'\'' prefix. Can perform READ, and DESCRIBE on these topics.",
                    "clientRole": true
                }
            ]
        }
    },
    "scopeMappings": [
        {
            "client": "kafka-producer",
            "roles": [
                "offline_access"
            ]
        },
        {
            "client": "kafka-consumer",
            "roles": [
                "offline_access"
            ]
        },
        {
            "clientScope": "offline_access",
            "roles": [
                "offline_access"
            ]
        }
    ],
    "clientScopeMappings": {
        "kafka": [
            {
                "client": "kafka-broker",
                "roles": [
                    "kafka-admin"
                ]
            }
        ]
    },
    "clients": [
        {
            "clientId": "kafka",
            "enabled": true,
            "publicClient": true,
            "bearerOnly": false,
            "standardFlowEnabled": false,
            "implicitFlowEnabled": false,
            "directAccessGrantsEnabled": false,
            "serviceAccountsEnabled": false,
            "consentRequired": false,
            "fullScopeAllowed": false
        },
        {
            "clientId": "kafka-broker",
            "enabled": true,
            "clientAuthenticatorType": "client-secret",
            "secret": "kafka-broker-secret",
            "publicClient": false,
            "bearerOnly": false,
            "standardFlowEnabled": false,
            "implicitFlowEnabled": false,
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "consentRequired": false,
            "fullScopeAllowed": false,
            "attributes": {
                "access.token.lifespan": "32140800"
            }
        },
        {
            "clientId": "kafka-producer",
            "enabled": true,
            "clientAuthenticatorType": "client-secret",
            "secret": "kafka-producer-secret",
            "publicClient": false,
            "bearerOnly": false,
            "standardFlowEnabled": false,
            "implicitFlowEnabled": false,
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "consentRequired": false,
            "fullScopeAllowed": false,
            "attributes": {
                "access.token.lifespan": "36000"
            }
        },
        {
            "clientId": "kafka-consumer",
            "enabled": true,
            "clientAuthenticatorType": "client-secret",
            "secret": "kafka-consumer-secret",
            "publicClient": false,
            "bearerOnly": false,
            "standardFlowEnabled": false,
            "implicitFlowEnabled": false,
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "consentRequired": false,
            "fullScopeAllowed": false,
            "attributes": {
                "access.token.lifespan": "32140800"
            }
        },
        {
            "clientId": "hello-world-producer",
            "enabled": true,
            "clientAuthenticatorType": "client-secret",
            "secret": "hello-world-producer-secret",
            "publicClient": false,
            "bearerOnly": false,
            "standardFlowEnabled": false,
            "implicitFlowEnabled": false,
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "consentRequired": false,
            "fullScopeAllowed": false,
            "attributes": {
                "access.token.lifespan": "36000"
            }
        },
        {
            "clientId": "hello-world-consumer",
            "enabled": true,
            "clientAuthenticatorType": "client-secret",
            "secret": "hello-world-consumer-secret",
            "publicClient": false,
            "bearerOnly": false,
            "standardFlowEnabled": false,
            "implicitFlowEnabled": false,
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "consentRequired": false,
            "fullScopeAllowed": false,
            "attributes": {
                "access.token.lifespan": "32140800"
            }
        },
        {
            "clientId": "hello-world-streams",
            "enabled": true,
            "clientAuthenticatorType": "client-secret",
            "secret": "hello-world-streams-secret",
            "publicClient": false,
            "bearerOnly": false,
            "standardFlowEnabled": false,
            "implicitFlowEnabled": false,
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "consentRequired": false,
            "fullScopeAllowed": false,
            "attributes": {
                "access.token.lifespan": "32140800"
            }
        },
        {
            "clientId": "kafka-connect",
            "enabled": true,
            "clientAuthenticatorType": "client-secret",
            "secret": "kafka-connect-secret",
            "publicClient": false,
            "bearerOnly": false,
            "standardFlowEnabled": false,
            "implicitFlowEnabled": false,
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "consentRequired": false,
            "fullScopeAllowed": false,
            "attributes": {
                "access.token.lifespan": "32140800"
            }
        },
        {
            "clientId": "kafka-bridge",
            "enabled": true,
            "clientAuthenticatorType": "client-secret",
            "secret": "kafka-bridge-secret",
            "publicClient": false,
            "bearerOnly": false,
            "standardFlowEnabled": false,
            "implicitFlowEnabled": false,
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "consentRequired": false,
            "fullScopeAllowed": false,
            "attributes": {
                "access.token.lifespan": "32140800"
            }
        },
        {
            "clientId": "kafka-mirror-maker",
            "enabled": true,
            "clientAuthenticatorType": "client-secret",
            "secret": "kafka-mirror-maker-secret",
            "publicClient": false,
            "bearerOnly": false,
            "standardFlowEnabled": false,
            "implicitFlowEnabled": false,
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "consentRequired": false,
            "fullScopeAllowed": false,
            "attributes": {
                "access.token.lifespan": "32140800"
            }
        },
        {
            "clientId": "kafka-mirror-maker-2",
            "enabled": true,
            "clientAuthenticatorType": "client-secret",
            "secret": "kafka-mirror-maker-2-secret",
            "publicClient": false,
            "bearerOnly": false,
            "standardFlowEnabled": false,
            "implicitFlowEnabled": false,
            "directAccessGrantsEnabled": true,
            "serviceAccountsEnabled": true,
            "consentRequired": false,
            "fullScopeAllowed": false,
            "attributes": {
                "access.token.lifespan": "32140800"
            }
        }
    ]
}')

if [[ ${RESULT} != "" && ${RESULT} != *"Conflict detected"* ]]; then
  echo "[ERROR] $(date -u +"%Y-%m-%d %H:%M:%S") Realm wasn't imported!"
  exit 1
fi

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Realm was successfully imported!"
