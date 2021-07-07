#!/usr/bin/env bash
set +x

USERNAME=$1
PASSWORD=$2
URL=$3

TOKEN_CURL_OUT=$(curl --insecure -X POST -d "client_id=admin-cli&client_secret=aGVsbG8td29ybGQtcHJvZHVjZXItc2VjcmV0&grant_type=password&username=$USERNAME&password=$PASSWORD" "https://$URL/auth/realms/master/protocol/openid-connect/token")
echo "[INFO] TOKEN_CURL_OUT: ${TOKEN_CURL_OUT}\n"
TOKEN=$(echo ${TOKEN_CURL_OUT} | awk -F '\"' '{print $4}')


TOKEN=$(curl --insecure -X POST -d "client_id=admin-cli&client_secret=aGVsbG8td29ybGQtcHJvZHVjZXItc2VjcmV0&grant_type=password&username=$USERNAME&password=$PASSWORD" "https://$URL/auth/realms/master/protocol/openid-connect/token" | awk -F '\"' '{print $4}')

RESULT=$(curl -v --insecure "https://$URL/auth/admin/realms" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Postman-Token: 3a6cd746-03b5-46fe-a54a-014fc7c51983' \
  -H 'cache-control: no-cache' \
  -d '{
      "realm": "scope-test",
      "accessTokenLifespan": 2592000,
      "ssoSessionMaxLifespan": 32140800,
      "ssoSessionIdleTimeout": 32140800,
      "enabled": true,
      "sslRequired": "external",
      "users": [
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
            "kafka" : ["kafka-admin"]
          }
        },
        {
          "username": "service-account-kafka-client",
          "enabled": true,
          "serviceAccountClientId": "kafka-client",
          "realmRoles": [
            "default-roles-audience"
          ]
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
          "kafka": [],
          "kafka-client": []
        }
      },
      "scopeMappings": [
        {
          "client": "kafka-broker",
          "roles": [
            "offline_access"
          ]
        },
        {
          "client": "kafka-client",
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
      "clientScopes": [
        {
         "name": "test",
         "description": "Custom test scope",
         "protocol": "openid-connect",
         "attributes": {
           "include.in.token.scope": "true",
           "display.on.consent.screen": "false"
         }
        },
        {
          "name": "offline_access",
          "description": "OpenID Connect built-in scope: offline_access",
          "protocol": "openid-connect",
          "attributes": {
            "consent.screen.text": "${offlineAccessScopeConsentText}",
            "display.on.consent.screen": "true"
          }
        },
        {
          "name": "profile",
          "description": "OpenID Connect built-in scope: profile",
          "protocol": "openid-connect",
          "attributes": {
            "include.in.token.scope": "true",
            "display.on.consent.screen": "true",
            "consent.screen.text": "${profileScopeConsentText}"
          },
          "protocolMappers": [
            {
              "name": "username",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-usermodel-property-mapper",
              "consentRequired": false,
              "config": {
                "userinfo.token.claim": "true",
                "user.attribute": "username",
                "id.token.claim": "true",
                "access.token.claim": "true",
                "claim.name": "preferred_username",
                "jsonType.label": "String"
              }
            }
          ]
        }
      ],
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
          "consentRequired" : false,
          "fullScopeAllowed" : false
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
          "consentRequired" : false,
          "fullScopeAllowed" : false,
          "attributes": {
            "access.token.lifespan": "32140800"
          }
        },
        {
          "clientId": "kafka-client",
          "enabled": true,
          "clientAuthenticatorType": "client-secret",
          "secret": "kafka-client-secret",
          "bearerOnly": false,
          "consentRequired": false,
          "standardFlowEnabled": false,
          "implicitFlowEnabled": false,
          "directAccessGrantsEnabled": true,
          "serviceAccountsEnabled": true,
          "publicClient": false,
          "attributes": {
            "access.token.lifespan": "32140800"
          },
          "fullScopeAllowed": true,
          "nodeReRegistrationTimeout": -1,
          "defaultClientScopes": [
            "profile"
          ],
          "optionalClientScopes": [
            "test",
            "offline_access"
          ]
        }
      ]
  }')

if [[ ${RESULT} != "" && ${RESULT} != *"Conflict detected"* ]]; then
  echo "[ERROR] $(date -u +"%Y-%m-%d %H:%M:%S") Authentication realm wasn't imported!"
  exit 1
fi

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Authentication realm was successfully imported!"

exit 0
