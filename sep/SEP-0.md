# Strimzi Enhancement Proposal - Definitions and procedures

ID: SEP-0
Author(s): Thomas Cooper <t.cooper@redhat.com>
Status: Proposal

## Motivation

We want to ensure that the Strimzi project is open to contributions from as wide a community of developers as possible. To achieve this we need to make the design process transparent and approachable. This is so that new contributors can see why certain features were adopted in the first place and the thinking behind their implementation. To quote the Kafka Improvement Proposals (KIP) procedures:

>All technical decisions have pros and cons so it is important we capture the thought process that lead to a decision or design to avoid flip-flopping needlessly.

To that end major new features, changes and/or alterations to public APIs in the core Strimzi code base should follow a standard procedure. This procedure, which we propose to call the Strimzi Enhancement Proposal (SEP), should lay out the reason for the proposal and a high-level plan outlining the intended changes. Strimzi community members can then comment on the SEP and, once a majority of maintainer level members agree, then the proposal can be accepted and work on the changes can commence.

These work involved in creating a SEP is intended to be proportional to the magnitude of the proposed change — small changes should require only a couple of brief paragraphs, whereas large changes will need detailed design discussions.

## Changes requiring a SEP

Any of the following should be considered a major change:

    - Any major new feature, subsystem, or piece of functionality
    - Any change that impacts the public interfaces of the project

_To Discuss:_ Should we be more specific here about the types of changes or should we leave this intentionally vague?

## What should be included in a SEP?

A SEP should contain the following sections:

    - Motivation: Describe the problem to be solved.
    - Proposed Changes: Describe the changes you that will be made as part of this proposal. This may be fairly extensive and have large subsections of its own. Or it may be a few sentences, depending on the scope of the change.
    - New or Changed Public Interfaces: Highlight to the community if your proposal will effect other areas of the codebase.
    - Migration Plan and Compatibility: Make clear if this proposal will involve user intervention in the operator upgrade procedure or other changes to the configuration of Strimzi that should be clearly documented and highlighted.
    - Rejected Alternatives: Describe the other alternatives that were considered and why they were rejected? The goal of this section is to help people understand why this is the best solution now, and also to prevent reinventing the wheel in the future when old alternatives are reconsidered.

## SEP Process

1) In your Strimzi fork create a new branch and make copy of the markdown `SEP-Template.md` file in the `sep` folder and use the next valid SEP index number e.g. `cp sep/SEP-Template.md sep/SEP-14.md`. Keep in mind that other SEPs could be merged whilst your SEP is under review and therefore you may need to change the index number. _Note_: If you need to include images/diagrams or other supporting material in your SEP then create a folder under the `sep` folder with the relevant SEP index and place the template and all relevant materials inside.
2) Fill in the relevant sections of the SEP template file.
3) Submit a PR to the Strimzi repository. The title should be of the form `SEP: <one line summary>`. Do not use the SEP index number in the PR title initially as this could change whilst it is under review.
4) Address comments from the Strimzi community.
5) If a majority of the Strimzi maintainers approve the SEP then they will add the _SEP Approved_ label to the PR. 
6) Add the relevant index number to the PR title and update the index in the SEP if required. 
6) Start work on the feature using the same SEP branch, this ensures that the SEP design document is included in the commit merged into the master branch.

## Approval of SEPs

SEPs require the approval of a majority of Strimzi maintainers, identified in the [`MAINTAINERS`](../MAINTAINERS) file. Usually this will be given by a comment on the SEP pull request which uses language clearly approving the SEP. Once a majority approval is given one of the maintainers will add the _SEP Approved_ label to the PR.

## Alternatives

The markdown file based system proposed above was chosen as it allows the design document to form part of the commit containing the implementation. This has several advantages when it comes to new developers understanding the Strimzi codebase and its evolution. However there are alternatives to the proposed process above:

### SEP indexing

The SEP number approach above is quite complicated and could lead to issues. Alternatively: 

    - We could use a single file in the repo `sep` folder that tracks all approved SEPs currently under development. This file could be updated by a maintainer when a SEP is approved. This would create a source of truth for the indexing but would mean more admin burden for maintainers.
    - We could use the project's wiki or another external systems to create a source of truth for SEP index numbers and allow contributors to edit this and claim an index number.

### SEP process

There are many alternatives to the proposed process. One such option would be to use the Strimzi Github repo's wiki (or another wiki implementation) to hold the design docs. Discussion could then take place on an associated mailing list (this is similar to the Apache Kafka KIP process). The issue here is that we loose the link between design doc and commit and requires heavy use of email as the main discussion medium (something the Strimzi community does not currently use very much).
