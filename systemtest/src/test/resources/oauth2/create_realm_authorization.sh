#!/usr/bin/env bash

# This script is using oauth2 tests

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
  "realm": "kafka-authz",
  "accessTokenLifespan": 120,
  "ssoSessionIdleTimeout": 864000,
  "ssoSessionMaxLifespan": 864000,
  "enabled": true,
  "sslRequired": "external",
  "roles": {
    "realm": [
      {
        "name": "Dev Team A",
        "description": "Developer on Dev Team A"
      },
      {
        "name": "Dev Team B",
        "description": "Developer on Dev Team B"
      },
      {
        "name": "Ops Team",
        "description": "Operations team member"
      }
    ],
    "client": {
      "team-a-client": [],
      "team-b-client": [],
      "kafka-cli": [],
      "kafka": [
        {
          "name": "uma_protection",
          "clientRole": true
        }
      ]
    }
  },
  "groups" : [
    {
      "name" : "ClusterManager Group",
      "path" : "/ClusterManager Group"
    }, {
      "name" : "ClusterManager-cluster2 Group",
      "path" : "/ClusterManager-cluster2 Group"
    }, {
      "name" : "Ops Team Group",
      "path" : "/Ops Team Group"
    }
  ],
  "users": [
    {
      "username" : "alice",
      "enabled" : true,
      "totp" : false,
      "emailVerified" : true,
      "firstName" : "Alice",
      "email" : "alice@strimzi.io",
      "credentials" : [ {
        "type" : "password",
        "secretData" : "{\"value\":\"KqABIiReBuRWbP4pBct3W067pNvYzeN7ILBV+8vT8nuF5cgYs2fdl2QikJT/7bGTW/PBXg6CYLwJQFYrBK9MWg==\",\"salt\":\"EPgscX9CQz7UnuZDNZxtMw==\"}",
        "credentialData" : "{\"hashIterations\":27500,\"algorithm\":\"pbkdf2-sha256\"}"
      } ],
      "disableableCredentialTypes" : [ ],
      "requiredActions" : [ ],
      "realmRoles" : [ "offline_access", "uma_authorization" ],
      "clientRoles" : {
        "account" : [ "view-profile", "manage-account" ]
      },
      "groups" : [ "/ClusterManager Group" ]
    }, {
      "username" : "bob",
      "enabled" : true,
      "totp" : false,
      "emailVerified" : true,
      "firstName" : "Bob",
      "email" : "bob@strimzi.io",
      "credentials" : [ {
        "type" : "password",
        "secretData" : "{\"value\":\"QhK0uLsKuBDrMm9Z9XHvq4EungecFRnktPgutfjKtgVv2OTPd8D390RXFvJ8KGvqIF8pdoNxHYQyvDNNwMORpg==\",\"salt\":\"yxkgwEyTnCGLn42Yr9GxBQ==\"}",
        "credentialData" : "{\"hashIterations\":27500,\"algorithm\":\"pbkdf2-sha256\"}"
      } ],
      "disableableCredentialTypes" : [ ],
      "requiredActions" : [ ],
      "realmRoles" : [ "offline_access", "uma_authorization" ],
      "clientRoles" : {
        "account" : [ "view-profile", "manage-account" ]
      },
      "groups" : [ "/ClusterManager-cluster2 Group" ]
    },
    {
      "username" : "service-account-team-a-client",
      "enabled" : true,
      "serviceAccountClientId" : "team-a-client",
      "realmRoles" : [ "offline_access", "Dev Team A" ],
      "clientRoles" : {
        "account" : [ "manage-account", "view-profile" ]
      },
      "groups" : [ ]
    },
    {
      "username" : "service-account-team-b-client",
      "enabled" : true,
      "serviceAccountClientId" : "team-b-client",
      "realmRoles" : [ "offline_access", "Dev Team B" ],
      "clientRoles" : {
        "account" : [ "manage-account", "view-profile" ]
      },
      "groups" : [ ]
    }
  ],
  "clients": [
    {
      "clientId": "team-a-client",
      "enabled": true,
      "clientAuthenticatorType": "client-secret",
      "secret": "team-a-client-secret",
      "bearerOnly": false,
      "consentRequired": false,
      "standardFlowEnabled": false,
      "implicitFlowEnabled": false,
      "directAccessGrantsEnabled": true,
      "serviceAccountsEnabled": true,
      "publicClient": false,
      "fullScopeAllowed": true
    },
    {
      "clientId": "team-b-client",
      "enabled": true,
      "clientAuthenticatorType": "client-secret",
      "secret": "team-b-client-secret",
      "bearerOnly": false,
      "consentRequired": false,
      "standardFlowEnabled": false,
      "implicitFlowEnabled": false,
      "directAccessGrantsEnabled": true,
      "serviceAccountsEnabled": true,
      "publicClient": false,
      "fullScopeAllowed": true
    },
    {
      "clientId": "kafka",
      "enabled": true,
      "clientAuthenticatorType": "client-secret",
      "secret": "kafka-secret",
      "bearerOnly": false,
      "consentRequired": false,
      "standardFlowEnabled": false,
      "implicitFlowEnabled": false,
      "directAccessGrantsEnabled": true,
      "serviceAccountsEnabled": true,
      "authorizationServicesEnabled": true,
      "publicClient": false,
      "fullScopeAllowed": true,
      "authorizationSettings": {
        "allowRemoteResourceManagement": true,
        "policyEnforcementMode": "ENFORCING",
        "resources": [
          {
            "name": "Topic:a-*",
            "type": "Topic",
            "ownerManagedAccess": false,
            "displayName": "Topics that start with a-",
            "attributes": {},
            "uris": [],
            "scopes": [
              {
                "name": "Create"
              },
              {
                "name": "Delete"
              },
              {
                "name": "Describe"
              },
              {
                "name": "Write"
              },
              {
                "name": "Read"
              },
              {
                "name": "Alter"
              },
              {
                "name": "DescribeConfigs"
              },
              {
                "name": "AlterConfigs"
              }
            ]
          },
          {
            "name": "Group:x-*",
            "type": "Group",
            "ownerManagedAccess": false,
            "displayName": "Consumer groups that start with x-",
            "attributes": {},
            "uris": [],
            "scopes": [
              {
                "name": "Describe"
              },
              {
                "name": "Delete"
              },
              {
                "name": "Read"
              }
            ]
          },
          {
            "name": "Topic:x-*",
            "type": "Topic",
            "ownerManagedAccess": false,
            "displayName": "Topics that start with x-",
            "attributes": {},
            "uris": [],
            "scopes": [
              {
                "name": "Create"
              },
              {
                "name": "Describe"
              },
              {
                "name": "Delete"
              },
              {
                "name": "Write"
              },
              {
                "name": "Read"
              },
              {
                "name": "Alter"
              },
              {
                "name": "DescribeConfigs"
              },
              {
                "name": "AlterConfigs"
              }
            ]
          },
          {
            "name": "Group:a-*",
            "type": "Group",
            "ownerManagedAccess": false,
            "displayName": "Groups that start with a-",
            "attributes": {},
            "uris": [],
            "scopes": [
              {
                "name": "Describe"
              },
              {
                "name": "Read"
              }
            ]
          },
          {
            "name": "Group:*",
            "type": "Group",
            "ownerManagedAccess": false,
            "displayName": "Any group",
            "attributes": {},
            "uris": [],
            "scopes": [
              {
                "name": "Describe"
              },
              {
                "name": "Read"
              },
              {
                "name": "DescribeConfigs"
              },
              {
                "name": "AlterConfigs"
              }
            ]
          },
          {
            "name": "Topic:*",
            "type": "Topic",
            "ownerManagedAccess": false,
            "displayName": "Any topic",
            "attributes": {},
            "uris": [],
            "scopes": [
              {
                "name": "Create"
              },
              {
                "name": "Delete"
              },
              {
                "name": "Describe"
              },
              {
                "name": "Write"
              },
              {
                "name": "Read"
              },
              {
                "name": "Alter"
              },
              {
                "name": "DescribeConfigs"
              },
              {
                "name": "AlterConfigs"
              }
            ]
          },
          {
            "name": "Topic:b-*",
            "type": "Topic",
            "ownerManagedAccess": false,
            "attributes": {},
            "uris": [],
            "scopes": [
              {
                "name": "Create"
              },
              {
                "name": "Delete"
              },
              {
                "name": "Describe"
              },
              {
                "name": "Write"
              },
              {
                "name": "Read"
              },
              {
                "name": "Alter"
              },
              {
                "name": "DescribeConfigs"
              },
              {
                "name": "AlterConfigs"
              }
            ]
          },
          {
            "name": "kafka-cluster:cluster2,Cluster:*",
            "type": "Cluster",
            "ownerManagedAccess": false,
            "displayName": "Cluster scope on cluster2",
            "attributes": {},
            "uris": [],
            "scopes": [
              {
                "name": "DescribeConfigs"
              },
              {
                "name": "AlterConfigs"
              },
              {
                "name": "ClusterAction"
              }
            ]
          },
          {
            "name": "kafka-cluster:cluster2,Group:*",
            "type": "Group",
            "ownerManagedAccess": false,
            "displayName": "Any group on cluster2",
            "attributes": {},
            "uris": [],
            "scopes": [
              {
                "name": "Delete"
              },
              {
                "name": "Describe"
              },
              {
                "name": "Read"
              },
              {
                "name": "DescribeConfigs"
              },
              {
                "name": "AlterConfigs"
              }
            ]
          },
          {
            "name": "kafka-cluster:cluster2,Topic:*",
            "type": "Topic",
            "ownerManagedAccess": false,
            "displayName": "Any topic on cluster2",
            "attributes": {},
            "uris": [],
            "scopes": [
              {
                "name": "Create"
              },
              {
                "name": "Delete"
              },
              {
                "name": "Describe"
              },
              {
                "name": "Write"
              },
              {
                "name": "IdempotentWrite"
              },
              {
                "name": "Read"
              },
              {
                "name": "Alter"
              },
              {
                "name": "DescribeConfigs"
              },
              {
                "name": "AlterConfigs"
              }
            ]
          },
          {
            "name" : "Cluster:*",
            "type" : "Cluster",
            "ownerManagedAccess" : false,
            "attributes" : { },
            "uris" : [ ]
          }
        ],
        "policies": [
          {
            "name": "Dev Team A",
            "type": "role",
            "logic": "POSITIVE",
            "decisionStrategy": "UNANIMOUS",
            "config": {
              "roles": "[{\"id\":\"Dev Team A\",\"required\":true}]"
            }
          },
          {
            "name": "Dev Team B",
            "type": "role",
            "logic": "POSITIVE",
            "decisionStrategy": "UNANIMOUS",
            "config": {
              "roles": "[{\"id\":\"Dev Team B\",\"required\":true}]"
            }
          },
          {
            "name": "Ops Team",
            "type": "role",
            "logic": "POSITIVE",
            "decisionStrategy": "UNANIMOUS",
            "config": {
              "roles": "[{\"id\":\"Ops Team\",\"required\":true}]"
            }
          },
          {
            "name" : "ClusterManager Group",
            "type" : "group",
            "logic" : "POSITIVE",
            "decisionStrategy" : "UNANIMOUS",
            "config" : {
              "groups" : "[{\"path\":\"/ClusterManager Group\",\"extendChildren\":false}]"
            }
          }, {
            "name" : "ClusterManager of cluster2 Group",
            "type" : "group",
            "logic" : "POSITIVE",
            "decisionStrategy" : "UNANIMOUS",
            "config" : {
              "groups" : "[{\"path\":\"/ClusterManager-cluster2 Group\",\"extendChildren\":false}]"
            }
          },
          {
            "name": "Dev Team A owns topics that start with a- on any cluster",
            "type": "resource",
            "logic": "POSITIVE",
            "decisionStrategy": "UNANIMOUS",
            "config": {
              "resources": "[\"Topic:a-*\"]",
              "applyPolicies": "[\"Dev Team A\"]"
            }
          },
          {
            "name": "Dev Team A can write to topics that start with x- on any cluster",
            "type": "scope",
            "logic": "POSITIVE",
            "decisionStrategy": "UNANIMOUS",
            "config": {
              "resources": "[\"Topic:x-*\"]",
              "scopes": "[\"Describe\",\"Write\"]",
              "applyPolicies": "[\"Dev Team A\"]"
            }
          },
          {
            "name": "Dev Team B owns topics that start with b- on cluster cluster2",
            "type": "resource",
            "logic": "POSITIVE",
            "decisionStrategy": "UNANIMOUS",
            "config": {
              "resources": "[\"Topic:b-*\"]",
              "applyPolicies": "[\"Dev Team B\"]"
            }
          },
          {
            "name": "Dev Team B can read from topics that start with x- on any cluster",
            "type": "scope",
            "logic": "POSITIVE",
            "decisionStrategy": "UNANIMOUS",
            "config": {
              "resources": "[\"Topic:x-*\"]",
              "scopes": "[\"Describe\",\"Read\"]",
              "applyPolicies": "[\"Dev Team B\"]"
            }
          },
          {
            "name": "Dev Team B can update consumer group offsets that start with x- on any cluster",
            "type": "scope",
            "logic": "POSITIVE",
            "decisionStrategy": "UNANIMOUS",
            "config": {
              "resources": "[\"Group:x-*\"]",
              "scopes": "[\"Describe\",\"Read\"]",
              "applyPolicies": "[\"Dev Team B\"]"
            }
          },
          {
            "name": "Dev Team A can use consumer groups that start with a- on any cluster",
            "type": "resource",
            "logic": "POSITIVE",
            "decisionStrategy": "UNANIMOUS",
            "config": {
              "resources": "[\"Group:a-*\"]",
              "applyPolicies": "[\"Dev Team A\"]"
            }
          },
          {
            "name": "ClusterManager of cluster2 Group has full access to topics on cluster2",
            "type": "resource",
            "logic": "POSITIVE",
            "decisionStrategy": "UNANIMOUS",
            "config": {
              "resources": "[\"kafka-cluster:cluster2,Topic:*\"]",
              "applyPolicies": "[\"ClusterManager of cluster2 Group\"]"
            }
          },
          {
            "name": "ClusterManager of cluster2 Group has full access to consumer groups on cluster2",
            "type": "resource",
            "logic": "POSITIVE",
            "decisionStrategy": "UNANIMOUS",
            "config": {
              "resources": "[\"kafka-cluster:cluster2,Group:*\"]",
              "applyPolicies": "[\"ClusterManager of cluster2 Group\"]"
            }
          },
          {
            "name": "ClusterManager of cluster2 Group has full access to cluster config on cluster2",
            "type": "resource",
            "logic": "POSITIVE",
            "decisionStrategy": "UNANIMOUS",
            "config": {
              "resources": "[\"kafka-cluster:cluster2,Cluster:*\"]",
              "applyPolicies": "[\"ClusterManager of cluster2 Group\"]"
            }
          }, {
            "name" : "ClusterManager Group has full access to manage and affect groups",
            "type" : "resource",
            "logic" : "POSITIVE",
            "decisionStrategy" : "UNANIMOUS",
            "config" : {
              "resources" : "[\"Group:*\"]",
              "applyPolicies" : "[\"ClusterManager Group\"]"
            }
          }, {
            "name" : "ClusterManager Group has full access to manage and affect topics",
            "type" : "resource",
            "logic" : "POSITIVE",
            "decisionStrategy" : "UNANIMOUS",
            "config" : {
              "resources" : "[\"Topic:*\"]",
              "applyPolicies" : "[\"ClusterManager Group\"]"
            }
          }, {
            "name" : "ClusterManager Group has full access to cluster config",
            "type" : "resource",
            "logic" : "POSITIVE",
            "decisionStrategy" : "UNANIMOUS",
            "config" : {
              "resources" : "[\"Cluster:*\"]",
              "applyPolicies" : "[\"ClusterManager Group\"]"
            }
          }
        ],
        "scopes": [
          {
            "name": "Create"
          },
          {
            "name": "Read"
          },
          {
            "name": "Write"
          },
          {
            "name": "Delete"
          },
          {
            "name": "Alter"
          },
          {
            "name": "Describe"
          },
          {
            "name": "ClusterAction"
          },
          {
            "name": "DescribeConfigs"
          },
          {
            "name": "AlterConfigs"
          },
          {
            "name": "IdempotentWrite"
          }
        ],
        "decisionStrategy": "AFFIRMATIVE"
      }
    },
    {
      "clientId": "kafka-cli",
      "enabled": true,
      "clientAuthenticatorType": "client-secret",
      "secret": "kafka-cli-secret",
      "bearerOnly": false,
      "consentRequired": false,
      "standardFlowEnabled": false,
      "implicitFlowEnabled": false,
      "directAccessGrantsEnabled": true,
      "serviceAccountsEnabled": false,
      "publicClient": true,
      "fullScopeAllowed": true
    }
  ]
}')

if [[ ${RESULT} != "" && ${RESULT} != *"Conflict detected"* ]]; then
  echo "[ERROR] $(date -u +"%Y-%m-%d %H:%M:%S") Realm wasn't imported!"
  exit 1
fi

echo "[INFO] $(date -u +"%Y-%m-%d %H:%M:%S") Realm was successfully imported!"
