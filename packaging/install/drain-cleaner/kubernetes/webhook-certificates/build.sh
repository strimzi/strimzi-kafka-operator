#!/usr/bin/env bash

function check_command_present() {
    command -v "${1}" >/dev/null 2>&1 || { echo -e >&2 "${RED}${1} is required but it's not installed.${NO_COLOUR}"; exit 1; }
}

check_command_present cfssl
check_command_present openssl

# Generate CA
cfssl genkey -initca ca.json | cfssljson -bare ca

# Sign webhook certificate
cfssl genkey webhook.json | cfssljson -bare webhook
cfssl sign -config config.json -profile server -ca ca.pem -ca-key ca-key.pem webhook.csr webhook.json | cfssljson -bare webhook

# Create CRT bundles
cat webhook.pem > webhook-bundle.crt
cat ca.pem >> webhook-bundle.crt

# Convert keys to PKCS8
openssl pkcs8 -topk8 -nocrypt -in ca-key.pem -out ca.key
openssl pkcs8 -topk8 -nocrypt -in webhook-key.pem -out webhook.key