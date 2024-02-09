[![Strimzi](./documentation/logo/strimzi.png)](https://strimzi.io/)

# Run Apache Kafka on Kubernetes and OpenShift

[![Build Status](https://dev.azure.com/cncf/strimzi/_apis/build/status/build?branchName=main)](https://dev.azure.com/cncf/strimzi/_build/latest?definitionId=16&branchName=main)
[![GitHub release](https://img.shields.io/github/release/strimzi/strimzi-kafka-operator.svg)](https://github.com/strimzi/strimzi-kafka-operator/releases/latest)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Twitter Follow](https://img.shields.io/twitter/follow/strimziio?style=social)](https://twitter.com/strimziio)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/strimzi-kafka-operator)](https://artifacthub.io/packages/search?repo=strimzi-kafka-operator)

Strimzi provides a way to run an [Apache Kafka®][kafka] cluster on 
[Kubernetes][k8s] or [OpenShift][os] in various deployment configurations.
See our [website][strimzi] for more details about the project.

## Quick Starts

To get up and running quickly, check our [Quick Start for Minikube, OKD (OpenShift Origin) and Kubernetes Kind](https://strimzi.io/quickstarts/). 

## Documentation

Documentation for the current _main_ branch as well as all releases can be found on our [website][strimzi].

## Roadmap

The roadmap of the Strimzi Operator project is maintained as [GitHub Project](https://github.com/orgs/strimzi/projects/1).

## Getting help

If you encounter any issues while using Strimzi, you can get help using:

- [#strimzi channel on CNCF Slack](https://slack.cncf.io/)
- [Strimzi Users mailing list](https://lists.cncf.io/g/cncf-strimzi-users/topics)
- [GitHub Discussions](https://github.com/strimzi/strimzi-kafka-operator/discussions)

## Strimzi Community Meetings

You can join our regular community meetings:
* Thursday 8:00 AM UTC (every 4 weeks starting from 4th June 2020) - [convert to your timezone](https://www.thetimezoneconverter.com/?t=8%3A00&tz=UTC)
* Thursday 4:00 PM UTC (every 4 weeks starting from 18th June 2020) - [convert to your timezone](https://www.thetimezoneconverter.com/?t=16%3A00&tz=UTC)

Resources:
* [Meeting minutes, agenda and Zoom link](https://docs.google.com/document/d/1V1lMeMwn6d2x1LKxyydhjo2c_IFANveelLD880A6bYc/edit#heading=h.vgkvn1hr5uor)
* [Recordings](https://youtube.com/playlist?list=PLpI4X8PMthYfONZopcRd4X_stq1C14Rtn)
* [Calendar](https://calendar.google.com/calendar/embed?src=c_m9pusj5ce1b4hr8c92hsq50i00%40group.calendar.google.com) ([Subscribe to the calendar](https://calendar.google.com/calendar/u/0?cid=Y19tOXB1c2o1Y2UxYjRocjhjOTJoc3E1MGkwMEBncm91cC5jYWxlbmRhci5nb29nbGUuY29t))

## Contributing

You can contribute by:
- Raising any issues you find using Strimzi
- Fixing issues by opening Pull Requests
- Improving documentation
- Talking about Strimzi

All bugs, tasks or enhancements are tracked as [GitHub issues](https://github.com/strimzi/strimzi-kafka-operator/issues). Issues which 
might be a good start for new contributors are marked with ["good-start"](https://github.com/strimzi/strimzi-kafka-operator/labels/good-start)
label.

The [Dev guide](https://github.com/strimzi/strimzi-kafka-operator/blob/main/development-docs/DEV_GUIDE.md) describes how to build Strimzi.
Before submitting a patch, please make sure to understand, how to test your changes before opening a PR [Test guide](https://github.com/strimzi/strimzi-kafka-operator/blob/main/development-docs/TESTING.md).

The [Documentation Contributor Guide](https://strimzi.io/contributing/guide/) describes how to contribute to Strimzi documentation.

If you want to get in touch with us first before contributing, you can use:

- [#strimzi channel on CNCF Slack](https://slack.cncf.io/)
- [Strimzi Dev mailing list](https://lists.cncf.io/g/cncf-strimzi-dev/topics)

## License
Strimzi is licensed under the [Apache License](./LICENSE), Version 2.0

## Container signatures

From the 0.38.0 release, Strimzi containers are signed using the [`cosign` tool](https://github.com/sigstore/cosign).
Strimzi currently does not use the keyless signing and the transparency log.
To verify the container, you can copy the following public key into a file:

```
-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAET3OleLR7h0JqatY2KkECXhA9ZAkC
TRnbE23Wb5AzJPnpevvQ1QUEQQ5h/I4GobB7/jkGfqYkt6Ct5WOU2cc6HQ==
-----END PUBLIC KEY-----
```

And use it to verify the signature:

```
cosign verify --key strimzi.pub quay.io/strimzi/operator:latest --insecure-ignore-tlog=true
```

## Software Bill of Materials (SBOM)

From the 0.38.0 release, Strimzi publishes the software bill of materials (SBOM) of our containers.
The SBOMs are published as an archive with `SPDX-JSON` and `Syft-Table` formats signed using cosign.
For releases, they are also pushed into the container registry.
To verify the SBOM signatures, please use the Strimzi public key:

```
-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAET3OleLR7h0JqatY2KkECXhA9ZAkC
TRnbE23Wb5AzJPnpevvQ1QUEQQ5h/I4GobB7/jkGfqYkt6Ct5WOU2cc6HQ==
-----END PUBLIC KEY-----
```

You can use it to verify the signature of the SBOM files with the following command:

```
cosign verify-blob --key cosign.pub --bundle <SBOM-file>.bundle --insecure-ignore-tlog=true <SBOM-file>
```

---

Strimzi is a <a href="http://cncf.io">Cloud Native Computing Foundation</a> incubating project.

![CNCF ><](./documentation/logo/cncf-color.png)

[strimzi]: https://strimzi.io "Strimzi"
[kafka]: https://kafka.apache.org "Apache Kafka"
[k8s]: https://kubernetes.io/ "Kubernetes"
[os]: https://www.openshift.com/ "OpenShift"
