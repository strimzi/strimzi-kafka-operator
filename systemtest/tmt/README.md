# Testing farm

This document gives a detailed breakdown of the testing processes using testing farm service.

## Pre-requisites

* Python >=3.9
* TMT command line tool (optional) - for lint and check tmt formatted test plans and tests
  * `pip install tmt[all]`
* Testing farm command line tool - for trigger your test plan in testing-farm
  * `pip install tft-cli`

## Links

* [Test Management Tool (tmt)](https://tmt.readthedocs.io/en/latest/index.html)
* [Testing farm](https://docs.testing-farm.io/general/0.1/index.html)

## Current plans and tests
Plans are stored in [plans](./plans) folder, there is a file called `main.fmf` which contains test plan definition. 
This definition is composed of hw requirements, prepare steps for created VM executor and specific plans. Specific
plan defines selectors for [tests](./tests) which should be executed.

### List of plans
* smoke
* upgrade
* regression-operators
* regression-brokers-and-security
* regression-operands
* sanity
* performance
* performance-capacity
* performance-topic-operator-capacity

## Usage

### Pre-requisites
1. Get API token for testing farm [(how-to obtain token)](https://docs.testing-farm.io/general/0.1/onboarding.html)
2. Store token into env var ```export TESTING_FARM_API_TOKEN="your_token"```

### Run tests

Run all plans
```commandline
testing-farm request --compose Fedora-38 --git-url https://github.com/strimzi/strimzi-kafka-operator.git
```

Select specific plan and git branch
```commandline
testing-farm request --compose Fedora-38 \
 --git-url https://github.com/strimzi/strimzi-kafka-operator.git \
 --git-ref some-branch \
 --plan smoke
```

Run multi-arch build
```commandline
testing-farm request --compose Fedora-Rawhide \
 --git-url https://github.com/strimzi/strimzi-kafka-operator.git \
 --git-ref some-branch \
 --plan smoke \
 --arch aarch64,x86_64
```

## Packit-as-a-service for PR check

[Packit-as-a-service](https://github.com/marketplace/packit-as-a-service) is a github application
for running testing-farm jobs from PR requested by command. Definition of the jobs is stored in
[.packit.yaml](../../.packit.yaml). Packit can be triggered from the PR by comment, but only members of strimzi
organization are able to run tests.

### Usage

Run all jobs for PR
```
/packit test
```

Run selected jobs by label
```
/packit test --labels upgrade,kraft-operators
```

#### Available labels
Packit jobs has plenty of available labels that you can use in trigger command.
The jobs are grouped based on specific use-case.

| Group           | Labels                              | Description                                                                                   |
|-----------------|-------------------------------------|-----------------------------------------------------------------------------------------------|
| acceptance      | acceptance                          | Acceptance tests                                                                              |
| acceptance_ipv6 | acceptance_ipv6                     | Acceptance tests with IPv6 configuration for Kube cluster                                     |
|                 | ipv6                                |                                                                                               |
| acceptance_dual | acceptance_dual                     | Acceptance tests with dual (IPv4 + IPv6) configuration for Kube cluster                       |
|                 | dual                                |                                                                                               |
| upgrade         | upgrade                             | Upgrade tests                                                                                 |
| sanity          | sanity                              | Sanity tests (plan with 1h execution time)                                                    |
| smoke           | smoke                               | Smoke tests (just one test)                                                                   |
| regression      | regression                          | Regression tests (all regression related labels, operators + operands + brokers-and-security) |
|                 | operands                            | Tests related to operands                                                                     |
|                 | regression-operands                 |                                                                                               |
|                 | brokers-and-security                | Tests related to Kafka Brokers and Security (specific test classes)                           |
|                 | regression-brokers-and-security     |                                                                                               |
|                 | bas                                 |                                                                                               |
|                 | operators                           | Tests more related to operators                                                               |
|                 | regression-operators                |                                                                                               |
| performance     | performance                         | Performance tests (all performance related labels)                                            |
|                 | performance-common                  | Common performance tests                                                                      |
|                 | performance-capacity                | Capacity tests for operators                                                                  |
|                 | performance-topic-operator-capacity | Capacity tests for Topic Operator only                                                        |

