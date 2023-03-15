# Installing Strimzi Drain Cleaner on Kubernetes without CertManager

Strimzi Drain Cleaner uses a `ValidationWebhook` to receive information about Strimzi pods being evicted.
Kubernetes requires that `ValidationWebhooks` are secured using TLS.
So the web-hook service needs to have HTTPS support.
And the CA of the certificate used for this service needs to be specified in the `ValidatingWebhookConfiguration` resource.

This directory contains sample installation files without certificates.
The following procedures describe how to generate certificates and add them to installation files.

## Generating certificates using OpenSSL

Use the OpenSSL TLS management tool to generate the TLS certificate for the Strimzi Drain Cleaner webhook.
The steps below have been tested with OpenSSL 1.1.1 and should work on Linux, MacOS, or in the Windows Subsystem for Linux.

1) Create and navigate to a subdirectory called `tls-certificate`:
   
   ```
   mkdir tls-certificate
   cd tls-certificate
   ```
2) Generate a CA public certificate and private key in the `tls-certificate` directory:
   ```
   openssl req -nodes -new -x509 -keyout ca.key -out ca.crt -subj "/CN=Strimzi Drain Cleaner CA"
   ```
   A `ca.crt` and `ca.key` file is created. 
3) Generate the private TLS key for the Strimzi Drain Cleaner:
   
   ```
   openssl genrsa -out tls.key 2048
   ```
   
   A `tls.key` file is created.
4) Generate a Certificate Signing Request and sign it by adding the CA public certificate (`ca.crt`) you generated:
   
   ```
   openssl req -new -key tls.key -subj "/CN=strimzi-drain-cleaner.strimzi-drain-cleaner.svc" \
        | openssl x509 -req -CA ca.crt -CAkey ca.key -CAcreateserial -extfile <(printf "subjectAltName=DNS:strimzi-drain-cleaner.strimzi-drain-cleaner.svc") -out tls.crt
   ```
   
   A `tls.crt` file is created.
   If you plan to change the name of the Strimzi Drain Cleaner service or install it into a different namespace, you have to change the Subject Alternative Name (SAN) of the certificate.
   The SAN must follow the pattern `<SERVICE_NAME>.<NAMESPACE_NAME>.svc`.
5) The `tls-certificate` directory should now contain several certificate files which we will use in the installation files.
   You can exit the `tls-certificate` directory now.
   
   ```
   cd ..
   ```

## Updating the installation files with the generated certificates

After you have generated the certificates you need, update the installation files.
This procedure assumes that you used the previous procedure to generate the certificate files.
If you generated your certificates in a different way or on a different path, you should update the path in the commands.

1) Edit the `caBundle` field in the [`070-ValidatingWebhookConfiguration.yaml`](070-ValidatingWebhookConfiguration.yaml) installation file to specify a Base64 encoded public key of your CA.
   You can use the `base64` utility to get the Base64 encoded public key:
   
   ```
   base64 tls-certificate/ca.crt
   ```
   
   After adding the Base64 encoded public key, your YAML should look similar to this:
   
   ```yaml
   # ...
   clientConfig:
     service:
       namespace: "strimzi-drain-cleaner"
       name: "strimzi-drain-cleaner"
       path: /drainer
       port: 443
     caBundle: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURKekNDQWcrZ0F3SUJBZ0lVSUNSMmtCRDlwdHFGTzhrRndtSEc5VVlhQTFJd0RRWUpLb1pJaHZjTkFRRUwKQlFBd0l6RWhNQjhHQTFVRUF3d1lVM1J5YVcxNmFTQkVjbUZwYmlCRGJHVmhibVZ5SUVOQk1CNFhEVEl5TURneQpPVEV6TkRJME1sb1hEVEl5TURreU9ERXpOREkwTWxvd0l6RWhNQjhHQTFVRUF3d1lVM1J5YVcxNmFTQkVjbUZwCmJpQkRiR1ZoYm1WeUlFTkJNSUlCSWpBTkJna3Foa2lHOXcwQkFRRUZBQU9DQVE4QU1JSUJDZ0tDQVFFQXFjUU8KbFRkV0V5SWliNGhwcmFUR3F6RUVham5pK0x2eERSTmEwY2lFcnY1MjNDdEVjOHNxMzliNGVoTFhpaEMrOTdwRQpCajJXTmQzVmszdlIzWGEvNk9LeU0yK21NdXlhaFprb0UyVThMRkpoS1JLdFU2bGg1QlFERU1TMkdrdWhZUWhhClNrM2J2NzVsVk1Bd3hldkEvY1J0RGRGK2dlUFN2c1NoZHhTSEtyZWxPSXdIeldkdnFkU2FUUW40cHFDdmdxMU0KRmJ0bmk0eTlMckVQOFYwSDNrWjJYM0pWUm01RVRkVlVDbU9yYm9lTHp6d0lXRWttNTZ6MFd0R0grcDRJNFhPeApHQzhvemViUDlTZkpNN2JRSzZPdCtVaStSY3puZ1gxQTlJWlFHbnlJR0Z3RDJvVmVFT3NIRzJHQlBxeGttZkIwCkI0bU92bXlnWjBtenVaOVl4UUlEQVFBQm8xTXdVVEFkQmdOVkhRNEVGZ1FVL2FtZEY1ZUppNDRLQ1NybnJtaDUKVkd6Z054NHdId1lEVlIwakJCZ3dGb0FVL2FtZEY1ZUppNDRLQ1NybnJtaDVWR3pnTng0d0R3WURWUjBUQVFILwpCQVV3QXdFQi96QU5CZ2txaGtpRzl3MEJBUXNGQUFPQ0FRRUFtWG5hVTFGRVkrd25KbGcwQTNDM0NEMmlCTUNlCk9UemorTWtrdWZrdFM3OWhneG84VWh0bU80MjhZSitXbnhTaEcwcHN5SlZCdHFQYmdQeGJYR3pKbnpsbFUzYm0KQjUzdFNFZ2RYem9Md3pCQk1zNStzek9jSVZUK2VRT3NVREZheEFIZlh5ODFUdE96bXFoTVRQTGtiWkptY29DUwpYWFlobnFqZlhDVi9RdGYvUHB0RzlLRXBWZytsdDl4QWV1MzkvTi91TFZKQlBSc2psQ3dBQ0dIWUFSL3g2MnlvClJRR1dZZjJGOSs4dVIzN0VhK2QwaXpVTzVyUlBrb21uaml6V3Myc1Q0YkFTckZJMkhTQnF2S1VNTzVJeVZrVmYKSm1MWU9nZU9iTzhlZjFPRll5U1JheFgzUU5OUTR5UzRjR1R5WHVPbnEvMStTUGp4YTF2amJHRnFFUT09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
   # ...
   ```
2) Create the `strimzi-drain-cleaner` namespace:
   
   ```
   kubectl create ns strimzi-drain-cleaner
   ```
3) Use `kubectl` to create a secret named `strimzi-drain-cleaner` with the `tls.crt` and `tls.key` files you generated:
   
   ```
   kubectl create secret tls strimzi-drain-cleaner \
       -n strimzi-drain-cleaner  \
       --cert=tls-certificate/tls.crt \
       --key=tls-certificate/tls.key
   ```
   
   This secret is used by the Strimzi Drain Cleaner deployment.
   The resulting Secret should look similar to this:
   
   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     creationTimestamp: "2022-08-29T13:57:14Z"
     name: strimzi-drain-cleaner
     namespace: strimzi-drain-cleaner
     resourceVersion: "224173"
     uid: 10583b09-b8e1-4d34-bf35-10d4f247026d
   type: kubernetes.io/tls
   data:
     tls.crt: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUM1RENDQWN3Q0ZCUlJsaERKU0dKWkZtbHYwWFd6RWtSVkxUWjNNQTBHQ1NxR1NJYjNEUUVCQ3dVQU1DTXgKSVRBZkJnTlZCQU1NR0ZOMGNtbHRlbWtnUkhKaGFXNGdRMnhsWVc1bGNpQkRRVEFlRncweU1qQTRNamt4TXpVegpNVGxhRncweU1qQTVNamd4TXpVek1UbGFNRG94T0RBMkJnTlZCQU1NTDNOMGNtbHRlbWt0WkhKaGFXNHRZMnhsCllXNWxjaTV6ZEhKcGJYcHBMV1J5WVdsdUxXTnNaV0Z1WlhJdWMzWmpNSUlCSWpBTkJna3Foa2lHOXcwQkFRRUYKQUFPQ0FROEFNSUlCQ2dLQ0FRRUEzTWRJSW1PWnVKU09xZ3hEdDdyVEZ2bnVSeS9RY25heGhldVJRcWMwQ05oNQpGTmRkWXYzZ3ZvNUQrNjlqSmwyRFMzOVgyNXhKSXF6eW5GR0NhdUd3SUk5dVE1ME53OVJBeHpldDNsQytDNDVYClMyc3p5UzA4OW91b1d1R1ZaYVZ4QzJlbkRuZCtYOXdzMWp5eEdqN093SVJvdG5CK096WFJsdFpIb001Z3dRcEUKVFVpR0VUdTRzQUtVeWZMbUNKVjVSWEtoZm1mNWNQT2E0ck13cTdPY2VRMzBmdGZSQWdVdGtOWm4wczNOZHQzUApzZjNUOXhXbHVkUlJVQzZ2VnNHZ2RZWmFpSDFOaHNSWkxVQ2JKT1Zic0kyMEh3NnFpOHUvT0JCZjU3bG83SUFLClZRSGhQRXB2NkI5cFRaR3kxR0NUcUxGdHhzUkRPYmt3eFJwWFVPWStxd0lEQVFBQk1BMEdDU3FHU0liM0RRRUIKQ3dVQUE0SUJBUUI1aTFWVkc0alFzb1pRSnRXeFA3c3FvWlRSNHdBejdWWlp2ME4xZkpFL1hlWng1bGNpRzdhagpKYkgxdjhuT3ZJc2FpaVNsWUJKNTJHRThmem5EZDJXTFZNczZ0d3VNc0hXa215dFZxT0VEY01GWmErSjN6eklKCnRpSHhGVHIrV2wrTmlOUCtDTzNOMmp3VWNIV0hwTm9TOUNBU01oNStnYjlHV2RxUFRKRCtSZmRTRk80aTFQNDkKcUlCOW8wRkJ3Nkc3L0ZXeG1jRHY3TmgzS2RiVzdwZDhPeCtCdjJlanZ2WXRzUzNBUmx2c0ZpWEh2N0lTdmV6aQphRnJMaElFcURFcHBMcmZTRU85VmR6N1VvV3NKV1BjdmwwQmJxRVNrakh3S2JJaFBrNzdRRU95QWFkRE5YTUFoCmJUUHg2czZLbStMUTQzWVhxTHMwODBjNWR4a01IbWJxCi0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
     tls.key: LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFcFFJQkFBS0NBUUVBM01kSUltT1p1SlNPcWd4RHQ3clRGdm51UnkvUWNuYXhoZXVSUXFjMENOaDVGTmRkCll2M2d2bzVEKzY5akpsMkRTMzlYMjV4SklxenluRkdDYXVHd0lJOXVRNTBOdzlSQXh6ZXQzbEMrQzQ1WFMyc3oKeVMwODlvdW9XdUdWWmFWeEMyZW5EbmQrWDl3czFqeXhHajdPd0lSb3RuQitPelhSbHRaSG9NNWd3UXBFVFVpRwpFVHU0c0FLVXlmTG1DSlY1UlhLaGZtZjVjUE9hNHJNd3E3T2NlUTMwZnRmUkFnVXRrTlpuMHMzTmR0M1BzZjNUCjl4V2x1ZFJSVUM2dlZzR2dkWVphaUgxTmhzUlpMVUNiSk9WYnNJMjBIdzZxaTh1L09CQmY1N2xvN0lBS1ZRSGgKUEVwdjZCOXBUWkd5MUdDVHFMRnR4c1JET2Jrd3hScFhVT1krcXdJREFRQUJBb0lCQVFDM05xRkQrSWV1eDRtRQowRnk1OGM0UE9TVmw2ZVgvdDBRbXNKQ0JVYVE2MnZuUU05RUp2MGxzbVQ4TmRFVEJwOFMzT0Z3K29QbUlUeUdlClczM3hHSTFDMkFSWEU5UkNlTGV4R3lHc1pqRHdBaFdyUHJGZzk2dXBwY2YyYzFHNVlvdm5QUU5EWENLQmhvT20KM2dMU2x1Q1luc2tPN1ZlejV6dWhBdjI2RXNuMEp2ZnZqNEVlN0tKdmd0Wkc1cGZYbTh3TGcrNmFNUlFPWFNTTQpCaC9TN2dvVjEvcjJLemRXUUF4bmEvSXE4Q01pQVZCZDZkNFo2SUx1a056cE1iZkpQOTl0eVIybEdxYlJWS25hCnZkRkZmWE9jVElKNWFEWmxEblJVTzVWdDRuZ2FoV1pPVm1YTFdtdURzOUNTS1k5V2FUcmJMbThMdmVSTUcwTm8KcmJVRVp1bGhBb0dCQVBtVG8yU3R1cFNheHVjeGlPWThSSGZ0REhEbHgrNkQwbExzamRzeVkrbEkzUTZCcVM2WQpDUitNSUFNRDNlb3lSQnI0VjQ2K01lVEt5WklJUnUxOTNyamZhWEFOOWIxcUpuVWhxK1FOMkxsQzhhUlNuNlVOCkxhdS8vZCtYUWxoQ3BKZnNCUjl2U2Q3eCswQ1lZdk9rUmM1TkZ1eE1sTGh3N21OVFJMYnU4WTZaQW9HQkFPSjEKNXpiK0F1R0JUN2pQRkFHcnEzTHdySHdwellmbStoWG1tWS9waFFrMHkxTXU5cXRla1cwMUx6b1FVNW85ejRqWApBdUN3YlpvRkZPRWJGTlpQSVZGSDlDeUwzbndJQ2krZ0NPZXdlQTM2SGFUMUZMWkw1alhmNlVKZkdHQ1RiUEIxCk9QTmk4T00yY1h4aGt4aFNVbzJ1TGNDRGpHVGlBV1hOM01Fa3ZGWGpBb0dCQUx1cHp4L0NIbHpsY1JDZ3g1N1AKekhBYzE5RUFodkNMUUpoSDlYYlFFaDUxUW50STllenFOMW40WkkyLzE1cmxSeDY4aENINlU4d0V5OEs1NUpOcApwUktBTTJrbHBoVlNmeTV1TGpFaVZFY2gxazlJTFhEUFV2c2xONEZyM0dBL1pFSTBIN24rVlo2RmQvYWZYekJ5CjRtWjR3dmJWc3JKcGdHZEJVcGIzWHIxcEFvR0Fmelo2UnpNVHpUZkYzK3c3c2VDTXRFbXNxeWNVZFFaMGIvOFYKUG5tUXFTWkdIWmxoS2MwNjVsMGRCWUlNWkxYYU1tV09FVWdxbVgzSFI5amRzTEhNNW9zRUNFNGVFaEMySUNESApQOWVxNlRjYWxnS3l1dUVTRml1TkxpS2JZQTBSQWxiblFoblBkZU9zaHBHTmZ2a250TDBtcUdGOWFXSm9KSmNyCkxqaURyNzhDZ1lFQXY4V2ZqWGZqa3pTYlpTZVFWQTh2S0NrNG5hYkVpRjJqeWh6amtCODhlNDFLUHBnOUFrdGsKd0o3WEppVnlJaW5CMytHNjl0NWQzbGJ3NWVMNkp5bVhBVEtOdyttaE9jQVlLZ0Y2MFlxejg2KzdtUno1R0M3egpLM3NUaC9lVlNJVnJVdXJzSkltVk9EbHk2WkpqSXRBYitSOGNaOXFydEFtN1Q0eUVHTlFPdHJJPQotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo=
   ```
4) With the installation files prepared, deploy Strimzi Drain Cleaner:
   
   ```
   kubectl apply -f ./
   ```

## Certificate renewals

Users are responsible for renewing the certificates before they expire.
The certificates can be renewed by just repeating the whole process and applying the updated YAML files.