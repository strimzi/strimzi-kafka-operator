# Label

#### Label: `label`
The `label` label is used for tests that verify the correct application and validation of custom labels in Kubernetes resources. This includes ensuring that custom labels are properly set during resource creation and correctly retrieved and validated against expected values.

#### Key Aspects Covered
1. **Custom Label Creation**:
    - Applying custom labels to Kubernetes resources during their creation.
    - Ensuring the resources are correctly labeled as specified.

2. **Label Retrieval**:
    - Retrieving Kubernetes resources with custom labels.
    - Validating that the retrieved labels match the expected values.

3. **Label Validation**:
    - Filtering specific labels from resources.
    - Verifying that filtered labels align with predefined criteria and expectations.


<!-- generated part -->
**Tests:**
- [testCustomBridgeLabelsAreProperlySet](../../.././development-docs/systemtests/io.strimzi.systemtest.bridge.HttpBridgeST.md)
