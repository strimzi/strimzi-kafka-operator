# GitHub repository restructuring

Currently, Strimzi has several Github repositories:
* The _main_ Strimzi Kafka Operators repo
* The Kafka Bridge repo
* The Kafka OAuth repo
* The Client Examples repo
* The Strimzi Admin repo for the planned Strimzi Admin API
* The Strimzi lab repo with some demos
* The Strimzi website repo

Despite us having multiple repositories, some things which are related to Strimzi itself seem to be only in the Operator repository.
For example the design proposals or the governance documents are central and should apply to whole Strimzi, but are in the operator repository only.
That might not be logical since some users might use only some parts and might miss these things.

## Proposed changes

### Governance

We should create a new repo called `governance`.
Into this repo, we should move the `GOVERNANCE.md` document with the Strimzi project governance as well as the `MAINTAINERS` file with the list of maintainers.
In all other repositories, we should have a files `GOVERNANCE.md` and `MAINTAINERS.md` which will just contain a link to the file in the `governance` repository.
Apart from these two files, the `governance` repo should also contain a license file and code of conduct file.

All changes to the governance and maintainers should be done through PRs in that repository.
The voting should be done either on the PR or on the mailing lists.

### Design Proposals

The design proposals are currently stored in the `design` folder of the `strimzi-kafka-operators` repository.
Right now, the existing proposals focus on the operators.
But it is expected that in the future we would have also proposals for the other components or proposals which include more than one repository.

We should create a new repository called `proposals` which should be used for the proposals with changes for all our projects.
The repository should contain an overview list of proposals and for easier orientation, the proposals should be numbered starting from 1.

The existing proposals should be moved to the new repository and get the numbers assigned.
Any proposals opened at the time of the move should be moved to the new repo as well.

### Strimzi roadmap

Additionally, the Strimzi Roadmap is kept as a project in the Strimzi operators repo as well.
It is central and includes the overall roadmap for all Strimzi projects as well.
The roadmap should be moved to the Strimzi organization as organization wide project.

## Next steps

If this proposal is approved, the steps described in the proposed changes should be done without undue delay:
* Create the new repositories and move the files
* Move the Roadmap GitHub project
* Add the links to the governance policy and maintainers list to all other repositories.