/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.annotations.KubeVersion;
import io.strimzi.crdgenerator.annotations.Crd;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

@Crd(spec = @Crd.Spec(group = "strimzi.io", names = @Crd.Spec.Names(kind = "Topic", plural = "topics", categories = "strimzi"), scope = "Namespaced"))
public class TopicCrd extends CustomResource<Object, Object> {

    public String name;

    public int partitions;

    public Map<Integer, List<Integer>> replicas;

    public static void main(String[] a) throws IOException {
        YAMLMapper m = CrdGenerator.YAML_MAPPER;
        TopicCrd x = new TopicCrd();
        x.name = "my-topic";
        x.partitions = 12;
        x.replicas = new HashMap<>();
        for (int i = 0; i < 12; i++) {
            x.replicas.put(i, asList((i + 1) % 7, (i + 2) % 7, (i + 3) % 7));
        }
        System.out.println(m.writeValueAsString(x));

        new CrdGenerator(KubeVersion.V1_11_PLUS, ApiVersion.V1BETA1, m, emptyMap(), new CrdGenerator.DefaultReporter(),
                emptyList(), null, null, new CrdGenerator.NoneConversionStrategy(), null, null)
            .generate(TopicCrd.class, new OutputStreamWriter(System.out));
    }
}
