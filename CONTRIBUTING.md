# Contributing

You can contribute by:

* Raising any issues you find using Strimzi
* Fixing issues by opening Pull Requests
* Improving documentation
* Talking about Strimzi

All bugs, tasks or enhancements are tracked as [GitHub issues][issues]. Issues which might be a good start for new contributors are marked with the [“good-start”][newbie-issues] label.

The [developer quick start guide][dev-quick-start] describes how to quickly get setup to build Strimzi from source.

The [development guide][development-guide] describes in more detail the various ways you can build Strimzi and how to test your changes before submitting a patch or opening a pull request.

The [release checklist][release-list] describes the steps that need to be completed for a new version release.

The [documentation contributor guide][doc-contrib-guide] describes how to contribute to Strimzi documentation.

If you want to get in touch with us first before contributing, you can use:

- [#strimzi channel on CNCF Slack](https://slack.cncf.io/)
- [Strimzi Dev mailing list](https://lists.cncf.io/g/cncf-strimzi-dev/topics)

[issues]: https://github.com/strimzi/strimzi-kafka-operator/issues
[newbie-issues]: https://github.com/strimzi/strimzi-kafka-operator/labels/good-start
[development-guide]: development-docs/DEV_GUIDE.md
[release-list]: development-docs/RELEASE.md
[doc-contrib-guide]: https://strimzi.io/contributing/guide/

# How to become a maintainer

The governance of the project is defined in the [GOVERNANCE.md](https://github.com/strimzi/strimzi-kafka-operator/blob/master/GOVERNANCE.md) file in the Strimzi Github repository, but in summary certain members of the community are the "maintainers" who decide the direction of the project. New maintainers can be elected by a ⅔ majority vote of the existing maintainers.

So as to be transparent and to ensure that all potential maintainers are judged fairly and consistently the following criteria should be taken into account in electing new maintainers:

## Criteria

* Sustained contributions over at least 3 months ideally including non-trivial PRs.
* An area of developing expertise. This includes the HTTP Bridge and Oauth components. The candidate does not have to be the _go-to_ expert, but they should have a more than superficial understanding of at least one area. For example:
    - A particular operator
    - Some Kubernetes concern (CRDs, API, JSON Schema, and so on)
    - Some associated bit of Kafka technology in which the project has a strategic interest (Cruise Control, for example)
    - System tests
    - Documentation
* Reviewing other contributors' PRs (not just for trivial errors, such as typos, but also guiding contributors on "big picture" aspects such as the future direction of the project).
* Contributing to different aspects of the project and community. For example it’s not enough to just implement features. We should also consider:
    - Bug reporting, triage and fixing
    - Addressing technical debt (in code, testing and documentation).
    - Documentation (both content and structure).
    - Helping users, for example on GitHub, the mailing list, Slack, Stack Overflow, and so on.   
    - Promotion of Strimzi, for example by blogging, speaking at conferences, and so on.
* An understanding of the overall architecture/structure of the codebase.
* Knowing when to ask for help or seek consensus (being a maintainer is not a licence to merge "at will" whatever you chose).
* An indication of being committed to the long term success of the project.

