### Annotation

#### Label: `annotation`
The `annotation` label is used for tests that verify the correct application and validation of custom annotations in Kubernetes resources. This includes ensuring that custom annotations are properly set during resource creation and correctly retrieved and validated against expected values.

#### Key Aspects Covered
1. **Custom Annotation Creation**:
    - Applying custom annotations to Kubernetes resources during their creation.
    - Ensuring the resources are correctly annotated as specified.

2. **Annotation Retrieval**:
    - Retrieving Kubernetes resources with custom annotations.
    - Validating that the retrieved annotations match the expected values.

3. **Annotation Validation**:
    - Filtering specific annotations from resources.
    - Verifying that filtered annotations align with predefined criteria and expectations.



<!-- generated part -->
**Tests:**
- [testCustomBridgeLabelsAreProperlySet](../../.././development-docs/systemtests/io.strimzi.systemtest.bridge.HttpBridgeST.md)
