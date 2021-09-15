# Installing Strimzi Drain Cleaner on Kubernetes without CertManager

Strimzi Drain Cleaner uses a `ValidationWebhook` to receive information about Strimzi pods being evicted.
Kubernetes requires that `ValidationWebhooks` are secured using TLS.
So the web-hook service needs to have HTTPS support.
And the CA of the certificate used for this service needs to be specified in the `ValidatingWebhookConfiguration` resource.

This directory contains sample files with pre-generated certificates.
As long as you don't change the namespace name or any service / pod names, you can just install them.

Additionally, in the `webhook-certificates` subdirectory, you have files which you can use to generate your own certificates using the [`cfssl` tool](https://github.com/cloudflare/cfssl).
In case you decide to generate your own certificates, you can use the script to generate them and then you have to update the `040-Secret.yaml` and `070-ValidatingWebhookConfiguration.yaml`.
Remember, that both resources contain the certificates encoded in base64.