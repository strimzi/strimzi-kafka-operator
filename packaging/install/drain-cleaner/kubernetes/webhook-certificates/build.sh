#!/usr/bin/env bash

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