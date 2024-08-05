# Configuration

### Label Description
The `configuration` label encompasses all tests related to configuration management. This includes creating, updating, replacing, and dynamically updating configurations across all operands managed by Strimzi.

#### Label: `configuration`

This label is used for tests that verify the correct handling and management of configurations. The tests under this label ensure that the system processes configuration changes properly, covering aspects such as creation, updates, replacements, and dynamic configuration updates.

#### Key Aspects Covered
1. **Configuration Creation**:
    - Deploying necessary resources and configurations from scratch.
    - Ensuring initial configurations are correctly applied and functional.

2. **Configuration Updates**:
    - Updating existing configurations.
    - Verifying that updates are correctly applied and reflected in the system.

3. **Configuration Replacements**:
    - Replacing entire configurations with new ones.
    - Ensuring that replacement configurations are applied without issues.

4. **Dynamic Configuration Updates**:
    - Applying configuration changes dynamically without requiring restarts.
    - Verifying that dynamic updates are correctly handled and reflected immediately.


<!-- generated part -->
**Tests:**
- [testCustomAndUpdatedValues](../../.././development-docs/systemtests/io.strimzi.systemtest.bridge.HttpBridgeST.md)
