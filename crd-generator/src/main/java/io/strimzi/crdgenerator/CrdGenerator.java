/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.annotations.KubeVersion;
import io.strimzi.api.annotations.VersionRange;
import io.strimzi.crdgenerator.annotations.CelValidation;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Example;
import io.strimzi.crdgenerator.annotations.Maximum;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.MinimumItems;
import io.strimzi.crdgenerator.annotations.OneOf;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.strimzi.crdgenerator.annotations.RequiredInVersions;
import io.strimzi.crdgenerator.annotations.Type;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.api.annotations.ApiVersion.V1;
import static io.strimzi.crdgenerator.Property.hasAnyGetterAndAnySetter;
import static io.strimzi.crdgenerator.Property.properties;
import static io.strimzi.crdgenerator.Property.sortedProperties;
import static io.strimzi.crdgenerator.Property.subtypes;
import static java.lang.Integer.parseInt;
import static java.lang.reflect.Modifier.isAbstract;
import static java.util.Arrays.asList;

/**
 * <p>Generates a Kubernetes {@code CustomResourceDefinition} YAML file
 * from an annotated Java model (POJOs).
 * The tool supports Jackson annotations and a few custom annotations in order
 * to generate a high-quality schema that's compatible with
 * K8S CRD validation schema support, which is more limited that the full OpenAPI schema
 * supported in the rest of K8S.</p>
 *
 * <p>The tool works by recursing through class properties (in the JavaBeans sense)
 * and the types of those properties, guided by the annotations.</p>
 *
 * <h2>Annotations</h2>
 * <dl>
 *     <dt>@{@link Crd}</dt>
 *     <dd>Annotates the top level class which represents an instance of the custom resource.
 *         This provides certain information used for the {@code CustomResourceDefinition}
 *         such as API group, version, singular and plural names etc.</dd>
 *
 *     <dt>@{@link Description}</dt>
 *     <dd>A description on a class or method: This gets added to the {@code description}
 *     of {@code property}s within the corresponding Schema Object.
 *
 *     <dt>@{@link Example}</dt>
 *     <dd>An example of usage. This gets added to the {@code example}
 *     of {@code property}s within the corresponding Schema Object.
 *
 *     <dt>@{@link Pattern}</dt>
 *     <dd>A pattern (regular expression) for checking the syntax of string-typed properties.
 *     This gets added to the {@code pattern}
 *     of {@code property}s within the corresponding Schema Object.
 *
 *     <dt>@{@link Minimum}</dt>
 *     <dd>A inclusive minimum for checking the bounds of integer-typed properties.
 *     This gets added to the {@code minimum}
 *     of {@code property}s within the corresponding Schema Object.</dd>
 *
 *     <dt>@{@link Maximum}</dt>
 *     <dd>A inclusive maximum for checking the bounds of integer-typed properties.
 *     This gets added to the {@code maximum}
 *     of {@code property}s within the corresponding Schema Object.</dd>
 *
 *     <dt>{@code @Deprecated}</dt>
 *     <dd>When present on a JavaBean property this marks the property as being deprecated within
 *     the corresponding Schema Object.
 *     When present on a Class this marks properties of that type as
 *     being deprecated within the corresponding Schema Object</dd>
 *
 *     <dt>{@code @JsonProperty.value}</dt>
 *     <dd>Overrides the default name (which is the JavaBean property name)
 *     with the given {@code value} in the {@code properties}
 *     of the corresponding Schema Object.</dd>
 *
 *     <dt>{@code @JsonProperty.required} and {@code @JsonTypeInfo.property}</dt>
 *     <dd>Marks a getter method as being required, adding it to the
 *     {@code required} of the corresponding Schema Object.</dd>
 *
 *     <dt>{@code @JsonIgnore}</dt>
 *     <dd>Marks a property as ignored, omitting it from the {@code properties}
 *     of the corresponding Schema Object.</dd>
 *
 *     <dt>{@code @JsonSubTypes}</dt>
 *     <dd>See following "Polymorphism" section.</dd>
 *
 *     <dt>{@code @JsonPropertyOrder}</dt>
 *     <dd>The declared order is reflected in ordering of the {@code properties}
 *     in the corresponding Schema Object.</dd>
 *
 * </dl>
 *
 * <h2>Polymorphism</h2>
 * <p>Although true OpenAPI Schema Objects have some support for polymorphism via
 * {@code discriminator} and {@code oneOf}, CRD validation schemas don't support
 * {@code discriminator} or references which means CRD validation
 * schemas don't support proper polymorphism.</p>
 *
 * <p>This tool provides some "fake" support for polymorphism by understanding
 * {@code @JsonTypeInfo.use = JsonTypeInfo.Id.NAME},
 * {@code @JsonTypeInfo.property} (which is Jackson's equivalent of a discriminator)
 * and {@code @JsonSubTypes} (which enumerates on the supertype the allowed subtypes
 * and their logical names).</p>
 *
 * <p>The tool will "fake" {@code oneOf} by constructing the union of the properties
 * of all the subtypes. This means that different subtypes cannot have (JavaBean)
 * properties of the same name but different types.
 * It also means that you have to include in the property {@code @Description}
 * which values of the {@code discriminator} (i.e. which subtype) the property
 * applies to.</p>
 *
 * @see <a href="https://github.com/OAI/OpenAPI-Specification/blob/OpenAPI.next/versions/3.0.0.md#schema-object"
 * >OpenAPI Specification Schema Object doc</a>
 * @see <a href="https://kubernetes.io/docs/tasks/access-kubernetes-api/extend-api-custom-resource-definitions/#validation"
 * >Additional restriction for CRDs</a>
 */
@SuppressWarnings("ClassFanOutComplexity")
class CrdGenerator {
    public static final YAMLMapper YAML_MAPPER = new YAMLMapper()
            .configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, true)
            .configure(YAMLGenerator.Feature.SPLIT_LINES, false)
            .configure(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE, true)
            .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false);
    public static final ObjectMapper JSON_MATTER = new ObjectMapper();
    private final ApiVersion crdApiVersion;
    private final List<ApiVersion> generateVersions;
    private final ApiVersion storageVersion;
    private final VersionRange<ApiVersion> servedVersion;
    private final VersionRange<ApiVersion> describeVersions;

    public interface Reporter {
        void err(String s);
    }

    public static class DefaultReporter implements Reporter {
        public void err(String s) {
            System.err.println("CrdGenerator: error: " + s);
        }
    }

    Reporter reporter;

    public static void argParseErr(String s) {
        System.err.println("CrdGenerator: error: " + s);
    }

    public void err(String s) {
        reporter.err(s);
        numErrors++;
    }

    public interface ConversionStrategy {

    }

    public static class NoneConversionStrategy implements ConversionStrategy {

    }

    public static class WebhookConversionStrategy implements ConversionStrategy {
        private final String url;
        private final String name;
        private final String namespace;
        private final String path;
        private final int port;
        private final String caBundle;

        public WebhookConversionStrategy(String url, String caBundle) {
            Objects.requireNonNull(url);
            Objects.requireNonNull(caBundle);
            this.url = url;
            this.name = null;
            this.namespace = null;
            this.path = null;
            this.port = -1;
            this.caBundle = caBundle;
        }

        public WebhookConversionStrategy(String name, String namespace, String path, int port, String caBundle) {
            Objects.requireNonNull(name);
            Objects.requireNonNull(namespace);
            Objects.requireNonNull(path);
            if (port <= 0) {
                throw new IllegalArgumentException();
            }
            Objects.requireNonNull(caBundle);
            this.url = null;
            this.name = name;
            this.namespace = namespace;
            this.path = path;
            this.port = port;
            this.caBundle = caBundle;
        }

        public boolean isUrl() {
            return url != null;
        }
    }

    // Currently unused, but it might be hande in the future so it is not removed
    @SuppressFBWarnings("URF_UNREAD_FIELD")
    @SuppressWarnings("unused")
    private final VersionRange<KubeVersion> targetKubeVersions;
    private final ObjectMapper mapper;
    private final JsonNodeFactory nf;
    private final Map<String, String> labels;
    private final ConversionStrategy conversionStrategy;

    private int numErrors;

    /**
     * @param targetKubeVersions The targeted version(s) of Kubernetes.
     * @param crdApiVersion The version of the CRD API for which to generate the CRD.
     * @param mapper The object mapper.
     * @param labels The labels to add to the CRD.
     * @param reporter The error reporter.
     * @param apiVersions The API versions to generate (allows selecting a subset of those in the @Crd annotation).
     * @param storageVersion If not null, override the storageVersion to the given value.
     * @param servedVersions If not null, override the served versions according to the given range.
     * @param conversionStrategy The conversion strategy.
     * @param describeVersions The range of API versions for which descriptions should be added
     */
    public CrdGenerator(VersionRange<KubeVersion> targetKubeVersions, ApiVersion crdApiVersion,
                 ObjectMapper mapper, Map<String, String> labels, Reporter reporter,
                 List<ApiVersion> apiVersions,
                 ApiVersion storageVersion,
                 VersionRange<ApiVersion> servedVersions,
                 ConversionStrategy conversionStrategy,
                 VersionRange<ApiVersion> describeVersions) {
        this.reporter = reporter;
        if (targetKubeVersions.isEmpty()
            || targetKubeVersions.isAll()) {
            err("Target kubernetes version cannot be empty or all");
        }
        this.targetKubeVersions = targetKubeVersions;
        this.crdApiVersion = crdApiVersion;
        this.mapper = mapper;
        this.nf = mapper.getNodeFactory();
        this.labels = labels;
        this.generateVersions = apiVersions;
        this.describeVersions = describeVersions;
        this.storageVersion = storageVersion;
        this.servedVersion = servedVersions;
        this.conversionStrategy = conversionStrategy;
    }

    public int generate(Class<? extends CustomResource> crdClass, Writer out) throws IOException {
        ObjectNode node = nf.objectNode();
        Crd crd = crdClass.getAnnotation(Crd.class);
        if (crd == null) {
            err(crdClass + " is not annotated with @Crd");
        } else {
            node.put("apiVersion", "apiextensions.k8s.io/" + crdApiVersion)
                    .put("kind", "CustomResourceDefinition")
                    .putObject("metadata")
                    .put("name", crd.spec().names().plural() + "." + crd.spec().group());

            if (!labels.isEmpty()) {
                ((ObjectNode) node.get("metadata"))
                    .putObject("labels")
                        .setAll(labels.entrySet().stream()
                        .collect(Collectors.<Map.Entry<String, String>, String, JsonNode, LinkedHashMap<String, JsonNode>>toMap(
                            Map.Entry::getKey,
                            e -> new TextNode(
                                e.getValue()
                                    .replace("%group%", crd.spec().group())
                                    .replace("%plural%", crd.spec().names().plural())
                                    .replace("%singular%", crd.spec().names().singular())),
                            (x, y) -> x,
                            LinkedHashMap::new)));
            }

            node.set("spec", buildSpec(crdApiVersion, crd.spec(), crdClass));
        }
        mapper.writeValue(out, node);
        return numErrors;
    }

    @SuppressWarnings("NPathComplexity")
    private ObjectNode buildSpec(ApiVersion crdApiVersion,
                                 Crd.Spec crd, Class<? extends CustomResource> crdClass) {
        ObjectNode result = nf.objectNode();
        result.put("group", crd.group());

        ArrayNode versions = nf.arrayNode();
        Map<ApiVersion, ObjectNode> subresources = buildSubresources(crd);
        Map<ApiVersion, ObjectNode> schemas = buildSchemas(crd, crdClass);
        Map<ApiVersion, ArrayNode> printerColumns = buildPrinterColumns(crd);

        result.set("names", buildNames(crd.names()));
        result.put("scope", crd.scope());

        if (conversionStrategy instanceof WebhookConversionStrategy) {
            // "Webhook": must be None if spec.preserveUnknownFields is true
            result.put("preserveUnknownFields", false);
        }
        result.set("conversion", buildConversion());

        for (Crd.Spec.Version version : crd.versions()) {
            ApiVersion crApiVersion = ApiVersion.parse(version.name());
            if (!shouldIncludeVersion(crApiVersion)) {
                continue;
            }
            ObjectNode versionNode = versions.addObject();
            versionNode.put("name", crApiVersion.toString());
            versionNode.put("served", servedVersion != null ? servedVersion.contains(crApiVersion) : version.served());
            versionNode.put("storage", storageVersion != null ? crApiVersion.equals(storageVersion) : version.storage());

            // Deprecation -> we add it only when the version is deprecated to avoid issues in ArgoCD when diffing the CRDs
            if (version.deprecated()) {
                versionNode.put("deprecated", true);
                if (version.deprecationWarning() != null && !version.deprecationWarning().isEmpty()) {
                    versionNode.put("deprecationWarning", version.deprecationWarning());
                }
            }

            // Subresources
            ObjectNode subresourcesForVersion = subresources.get(crApiVersion);
            if (!subresourcesForVersion.isEmpty()) {
                versionNode.set("subresources", subresourcesForVersion);
            }

            // Printer columns
            ArrayNode cols = printerColumns.get(crApiVersion);
            if (!cols.isEmpty()) {
                versionNode.set("additionalPrinterColumns", cols);
            }

            versionNode.set("schema", schemas.get(crApiVersion));
        }

        result.set("versions", versions);

        return result;
    }

    private ObjectNode buildConversion() {
        ObjectNode conversion = nf.objectNode();
        if (conversionStrategy instanceof NoneConversionStrategy) {
            conversion.put("strategy", "None");
        } else if (conversionStrategy instanceof WebhookConversionStrategy) {
            conversion.put("strategy", "Webhook");
            WebhookConversionStrategy webhookStrategy = (WebhookConversionStrategy) conversionStrategy;
            ObjectNode webhook = conversion.putObject("webhook");
            webhook.putArray("conversionReviewVersions").add("v1").add("v1beta1");
            ObjectNode webhookClientConfig = webhook.putObject("clientConfig");
            webhookClientConfig.put("caBundle", webhookStrategy.caBundle);
            if (webhookStrategy.isUrl()) {
                webhookClientConfig.put("url", webhookStrategy.url);
            } else {
                webhookClientConfig.putObject("service")
                    .put("name", webhookStrategy.name)
                    .put("namespace", webhookStrategy.namespace)
                    .put("path", webhookStrategy.path)
                    .put("port", webhookStrategy.port);
            }
        } else {
            throw new IllegalStateException();
        }
        return conversion;
    }

    private Map<ApiVersion, ObjectNode> buildSchemas(Crd.Spec crd, Class<? extends CustomResource> crdClass) {
        return Arrays.stream(crd.versions())
            .map(version -> ApiVersion.parse(version.name()))
            .filter(this::shouldIncludeVersion)
            .collect(Collectors.toMap(Function.identity(),
                version -> buildValidation(crdClass, version, shouldDescribeVersion(version))));
    }

    private Map<ApiVersion, ObjectNode> buildSubresources(Crd.Spec crd) {
        return Arrays.stream(crd.versions())
            .map(version -> ApiVersion.parse(version.name()))
            .filter(this::shouldIncludeVersion)
            .collect(Collectors.toMap(Function.identity(),
                version -> buildSubresources(crd, version)));
    }

    private boolean shouldIncludeVersion(ApiVersion version) {
        return generateVersions == null
            || generateVersions.isEmpty()
            || generateVersions.contains(version);
    }

    private boolean shouldDescribeVersion(ApiVersion version) {
        return describeVersions == null
                || describeVersions.isEmpty()
                || describeVersions.isAll()
                || describeVersions.contains(version);
    }

    private Map<ApiVersion, ArrayNode> buildPrinterColumns(Crd.Spec crd) {
        return Arrays.stream(crd.versions())
            .map(version -> ApiVersion.parse(version.name()))
            .filter(this::shouldIncludeVersion)
            .collect(Collectors.toMap(Function.identity(),
                version -> buildAdditionalPrinterColumns(crd, version)));
    }

    private ObjectNode buildSubresources(Crd.Spec crd, ApiVersion crApiVersion) {
        ObjectNode subresources = nf.objectNode();
        if (crd.subresources().status().length != 0) {
            ObjectNode status = buildStatus(crd, crApiVersion);
            if (status != null) {
                subresources.set("status", status);
            }

            ObjectNode scaleNode = buildScale(crd, crApiVersion);
            if (scaleNode != null) {
                subresources.set("scale", scaleNode);
            }
        }
        return subresources;
    }

    private ObjectNode buildStatus(Crd.Spec crd, ApiVersion crApiVersion) {
        ObjectNode status;
        long length = Arrays.stream(crd.subresources().status())
                .filter(st -> ApiVersion.parseRange(st.apiVersion()).contains(crApiVersion))
                .count();
        if (length == 1) {
            status = nf.objectNode();
        } else if (length > 1)  {
            err("Each custom resource definition can have only one status sub-resource.");
            status = null;
        } else {
            status = null;
        }
        return status;
    }

    private ObjectNode buildScale(Crd.Spec crd, ApiVersion crApiVersion) {
        ObjectNode scaleNode;
        Crd.Spec.Subresources.Scale[] scales = crd.subresources().scale();
        List<Crd.Spec.Subresources.Scale> filteredScales = Arrays.stream(scales)
                .filter(sc -> ApiVersion.parseRange(sc.apiVersion()).contains(crApiVersion))
                .collect(Collectors.toList());
        if (filteredScales.size() == 1) {
            scaleNode = nf.objectNode();
            Crd.Spec.Subresources.Scale scale = filteredScales.get(0);

            scaleNode.put("specReplicasPath", scale.specReplicasPath());
            scaleNode.put("statusReplicasPath", scale.statusReplicasPath());

            if (!scale.labelSelectorPath().isEmpty()) {
                scaleNode.put("labelSelectorPath", scale.labelSelectorPath());
            }
        } else if (filteredScales.size() > 1)  {
            throw new RuntimeException("Each custom resource definition can have only one scale sub-resource.");
        } else {
            scaleNode = null;
        }
        return scaleNode;
    }

    private ArrayNode buildAdditionalPrinterColumns(Crd.Spec crd, ApiVersion crApiVersion) {
        ArrayNode cols = nf.arrayNode();
        if (crd.additionalPrinterColumns().length != 0) {
            for (Crd.Spec.AdditionalPrinterColumn col : Arrays.stream(crd.additionalPrinterColumns())
                    .filter(col -> crApiVersion == null || ApiVersion.parseRange(col.apiVersion()).contains(crApiVersion))
                    .collect(Collectors.toList())) {
                ObjectNode colNode = cols.addObject();
                colNode.put("name", col.name());
                colNode.put("description", col.description());
                colNode.put(crdApiVersion.compareTo(V1) >= 0 ? "jsonPath" : "JSONPath", col.jsonPath());
                colNode.put("type", col.type());
                if (col.priority() != 0) {
                    colNode.put("priority", col.priority());
                }
                if (!col.format().isEmpty()) {
                    colNode.put("format", col.format());
                }
            }
        }
        return cols;
    }

    private JsonNode buildNames(Crd.Spec.Names names) {
        ObjectNode result = nf.objectNode();
        String kind = names.kind();
        result.put("kind", kind);
        String listKind = names.listKind();
        if (listKind.isEmpty()) {
            listKind = kind + "List";
        }
        result.put("listKind", listKind);
        String singular = names.singular();
        if (singular.isEmpty()) {
            singular = kind.toLowerCase(Locale.US);
        }
        result.put("singular", singular);
        result.put("plural", names.plural());

        if (names.shortNames().length > 0) {
            result.set("shortNames", stringArray(asList(names.shortNames())));
        }

        if (names.categories().length > 0) {
            result.set("categories", stringArray(asList(names.categories())));
        }
        return result;
    }

    private ObjectNode buildValidation(Class<? extends CustomResource> crdClass, ApiVersion crApiVersion, boolean description) {
        ObjectNode result = nf.objectNode();
        result.set("openAPIV3Schema", buildObjectSchema(crApiVersion, crdClass, description));
        return result;
    }

    private ObjectNode buildObjectSchema(ApiVersion crApiVersion, Class<?> crdClass, boolean description) {
        ObjectNode result = nf.objectNode();
        buildObjectSchema(crApiVersion, result, crdClass, description);
        return result;
    }

    private void buildObjectSchema(ApiVersion crApiVersion, ObjectNode result, Class<?> crdClass, boolean description) {
        if (!crdClass.getName().startsWith("java.lang.")) {
            // java.lang.* class does not require class validation as i.e. JsonIgnore and Builder does not apply
            checkClass(crdClass);
        }

        result.put("type", "object");

        result.set("properties", buildSchemaProperties(crApiVersion, crdClass, description));
        ArrayNode oneOf = buildSchemaOneOf(crdClass);
        if (oneOf != null) {
            result.set("oneOf", oneOf);
        }
        ArrayNode required = buildSchemaRequired(crApiVersion, crdClass);
        if (!required.isEmpty()) {
            result.set("required", required);
        }
    }

    private ArrayNode buildSchemaOneOf(Class<?> crdClass) {
        ArrayNode alternatives;
        OneOf oneOf = crdClass.getAnnotation(OneOf.class);
        if (oneOf != null && oneOf.value().length > 0) {
            alternatives = nf.arrayNode();
            for (OneOf.Alternative alt : oneOf.value()) {
                ObjectNode alternative = alternatives.addObject();
                ObjectNode properties = alternative.putObject("properties");
                for (OneOf.Alternative.Property prop: alt.value()) {
                    properties.putObject(prop.value());
                }

                ArrayNode required = nf.arrayNode();
                for (OneOf.Alternative.Property prop: alt.value()) {
                    if (prop.required()) {
                        required.add(prop.value());
                    }
                }
                // We attach only non-empty array. Empty arrays would be removed by Kubernetes and might confuse various
                // tools when diffing the resources (such as ArgoCD)
                if (!required.isEmpty())    {
                    alternative.set("required", required);
                }
            }
        } else {
            alternatives = null;
        }
        return alternatives;
    }

    private void checkClass(Class<?> crdClass) {
        if (!isAbstract(crdClass.getModifiers())) {
            checkForBuilderClass(crdClass, crdClass.getName() + "Builder");
            checkForBuilderClass(crdClass, crdClass.getName() + "Fluent");

            checkClassOverrides(crdClass, "hashCode");
            hasAnyGetterAndAnySetter(crdClass);
        } else {
            for (Class<?> c : subtypes(null, crdClass)) {
                hasAnyGetterAndAnySetter(c);
                checkDiscriminatorIsIncluded(crdClass, c);
                checkJsonPropertyOrder(c);
            }
        }

        if (crdClass.getName().startsWith("io.strimzi.")) {
            checkInherits(crdClass, "io.strimzi.api.kafka.model.common.UnknownPropertyPreserving");
            checkJsonInclude(crdClass);
            checkJsonPropertyOrder(crdClass);
        }

        checkClassOverrides(crdClass, "equals", Object.class);
    }

    private void checkJsonInclude(Class<?> crdClass) {
        if (!crdClass.isAnnotationPresent(JsonInclude.class)) {
            err(crdClass + " is missing @JsonInclude");
        } else if (!crdClass.getAnnotation(JsonInclude.class).value().equals(JsonInclude.Include.NON_NULL)
                && !crdClass.getAnnotation(JsonInclude.class).value().equals(JsonInclude.Include.NON_DEFAULT)) {
            err(crdClass + " has a @JsonInclude value other than Include.NON_NULL");
        }
    }

    private void checkJsonPropertyOrder(Class<?> crdClass) {
        if (!isAbstract(crdClass.getModifiers())
                && !crdClass.isAnnotationPresent(JsonPropertyOrder.class)) {
            err(crdClass + " is missing @JsonPropertyOrder");
        }
    }

    private void checkDiscriminatorIsIncluded(Class<?> crdClass, Class<?> c) {
        try {
            String typePropertyName = crdClass.getAnnotation(JsonTypeInfo.class).property();
            String methodName = "get" + typePropertyName.substring(0, 1).toUpperCase(Locale.ENGLISH) + typePropertyName.substring(1).toLowerCase(Locale.ENGLISH);
            Method method = c.getMethod(methodName);

            if (!isAnnotatedWithIncludeNonNull(method)) {
                err(c.getCanonicalName() + "#" + methodName + " is not annotated with @JsonInclude(JsonInclude.Include.NON_NULL)");
            }
        } catch (NoSuchMethodException e) {
            err(e.getMessage());
        }
    }

    private boolean isAnnotatedWithIncludeNonNull(Method method) {
        JsonInclude ann = method.getAnnotation(JsonInclude.class);
        return ann != null && ann.value().equals(JsonInclude.Include.NON_NULL);
    }

    private void checkInherits(Class<?> crdClass, String className) {
        if (!inherits(crdClass, className)) {
            err(crdClass + " does not inherit " + className);
        }
    }

    private boolean inherits(Class<?> crdClass, String className) {
        Class<?> c = crdClass;
        boolean found = false;
        outer: do {
            if (className.equals(c.getName())) {
                found = true;
                break outer;
            }
            for (Class<?> i : c.getInterfaces()) {
                if (inherits(i, className)) {
                    found = true;
                    break outer;
                }
            }
            c = c.getSuperclass();
        } while (c != null);
        return found;
    }

    private void checkForBuilderClass(Class<?> crdClass, String builderClass) {
        try {
            Class.forName(builderClass, false, crdClass.getClassLoader());
        } catch (ClassNotFoundException e) {
            err(crdClass + " is not annotated with @Buildable (" + builderClass + " does not exist)");
        }
    }

    private void checkClassOverrides(Class<?> crdClass, String methodName, Class<?>... parameterTypes) {
        try {
            crdClass.getDeclaredMethod(methodName, parameterTypes);
        } catch (NoSuchMethodException e) {
            err(crdClass + " does not override " + methodName);
        }
    }

    private Collection<Property> unionOfSubclassProperties(ApiVersion crApiVersion, Class<?> crdClass) {
        JsonPropertyOrder order = crdClass.getAnnotation(JsonPropertyOrder.class);

        TreeMap<String, Property> result = new TreeMap<>();
        for (Class<?> subtype : Property.subtypes(crApiVersion, crdClass)) {
            Map<String, Property> properties = properties(crApiVersion, subtype);
            checkPropertiesInJsonPropertyOrder(subtype, properties.keySet());
            result.putAll(properties);
        }

        Map<String, Property> properties = properties(crApiVersion, crdClass);
        checkPropertiesInJsonPropertyOrder(crdClass, properties.keySet());
        result.putAll(properties);

        return sortedProperties(order != null ? order.value() : null, result).values();
    }

    private void checkPropertiesInJsonPropertyOrder(Class<?> crdClass, Set<String> properties) {
        if (!isAbstract(crdClass.getModifiers())) {
            JsonPropertyOrder order = crdClass.getAnnotation(JsonPropertyOrder.class);
            if (order == null) {
                // Skip as the error is already tracked in checkClass
                return;
            }

            List<String> expectedOrder = asList(order.value());
            for (String property : properties) {
                if (!expectedOrder.contains(property)) {
                    err(crdClass + " has a property " + property + " which is not in the @JsonPropertyOrder");
                }
            }
        }
    }

    private ArrayNode buildSchemaRequired(ApiVersion crApiVersion, Class<?> crdClass) {
        ArrayNode result = nf.arrayNode();

        for (Property property : unionOfSubclassProperties(crApiVersion, crdClass)) {
            if (property.isAnnotationPresent(JsonProperty.class)
                    && property.getAnnotation(JsonProperty.class).required()
                || property.isDiscriminator()) {
                result.add(property.getName());
            } else if (property.isAnnotationPresent(RequiredInVersions.class)
                    && ApiVersion.parseRange(property.getAnnotation(RequiredInVersions.class).value()).contains(crApiVersion)) {
                result.add(property.getName());
            }
        }
        return result;
    }

    private ObjectNode buildSchemaProperties(ApiVersion crApiVersion, Class<?> crdClass, boolean description) {
        ObjectNode properties = nf.objectNode();

        buildKindApiVersionAndMetadata(properties, crdClass);

        for (Property property : unionOfSubclassProperties(crApiVersion, crdClass)) {
            buildProperty(crApiVersion, properties, property, description);
        }
        return properties;
    }

    private void buildKindApiVersionAndMetadata(ObjectNode properties, Class<?> crdClass)   {
        if (crdClass.isAnnotationPresent(Crd.class))    {
            // Add metadata to the CRD class root
            ObjectNode apiVersion = properties.putObject("apiVersion");
            apiVersion.put("type", "string");
            apiVersion.put("description", "APIVersion defines the versioned schema of this representation of an object. " +
                    "Servers should convert recognized schemas to the latest internal value, and may reject unrecognized values. " +
                    "More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources");

            ObjectNode kind = properties.putObject("kind");
            kind.put("type", "string");
            kind.put("description", "Kind is a string value representing the REST resource this object " +
                    "represents. Servers may infer this from the endpoint the client submits requests to. Cannot be updated. " +
                    "In CamelCase. More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds");

            ObjectNode metadata = properties.putObject("metadata");
            metadata.put("type", "object");
        }
    }

    private void buildProperty(ApiVersion crdApiVersion, ObjectNode properties, Property property, boolean description) {
        properties.set(property.getName(), buildSchema(crdApiVersion, property, description));
    }

    private ObjectNode buildSchema(ApiVersion crApiVersion, Property property, boolean description) {
        PropertyType propertyType = property.getType();
        Class<?> returnType = propertyType.getType();
        final ObjectNode schema;
        if (propertyType.getGenericType() instanceof ParameterizedType
                && ((ParameterizedType) propertyType.getGenericType()).getRawType().equals(Map.class)
                && ((ParameterizedType) propertyType.getGenericType()).getActualTypeArguments()[0].equals(Integer.class)) {
            System.err.println("It's OK");
            schema = nf.objectNode();
            schema.put("type", "object");
            schema.putObject("patternProperties").set("-?[0-9]+", buildArraySchema(crApiVersion, property, new PropertyType(null, ((ParameterizedType) propertyType.getGenericType()).getActualTypeArguments()[1]), description));
        } else if (propertyType.getGenericType() instanceof ParameterizedType
                && ((ParameterizedType) propertyType.getGenericType()).getRawType().equals(Map.class)
                && isMapOfTypes(propertyType, String.class, Quantity.class)) {
            schema = buildQuantityTypeSchema();
        } else if (Schema.isJsonScalarType(returnType)
                || Map.class.equals(returnType)) {            
            schema = addSimpleTypeConstraints(crApiVersion, buildBasicTypeSchema(property, returnType), property);
        } else if (returnType.isArray() || List.class.equals(returnType)) {
            schema = buildArraySchema(crApiVersion, property, property.getType(), description);
        } else {
            schema = buildObjectSchema(crApiVersion, returnType, description);
        }

        if (description) {
            addDescription(crApiVersion, schema, property);
        }

        celValidationRules(crApiVersion, property, schema);

        return schema;
    }

    @SuppressWarnings("unchecked")
    private ObjectNode buildArraySchema(ApiVersion crApiVersion, Property property, PropertyType propertyType, boolean description) {
        int arrayDimension = propertyType.arrayDimension();
        ObjectNode result = nf.objectNode();
        ObjectNode itemResult = result;
        for (int i = 0; i < arrayDimension; i++) {
            itemResult.put("type", "array");
            MinimumItems minimumItems = selectVersion(crApiVersion, property, MinimumItems.class);
            if (minimumItems != null) {
                result.put("minItems", minimumItems.value());
            }
            itemResult = itemResult.putObject("items");
        }
        Class<?> elementType = propertyType.arrayBase();
        if (String.class.equals(elementType)) {
            itemResult.put("type", "string");
        } else if (Integer.class.equals(elementType)
                || int.class.equals(elementType)
                || Long.class.equals(elementType)
                || long.class.equals(elementType)) {
            itemResult.put("type", "integer");
        } else if (Map.class.equals(elementType)) {
            if (isMapOfTypes(propertyType, String.class, String.class)) {
                preserveUnknownStringFields(itemResult);
            } else {
                preserveUnknownFields(itemResult);
            }

            itemResult.put("type", "object");
        } else if (elementType.isEnum()) {
            itemResult.put("type", "string");

            try {
                Method valuesMethod = elementType.getMethod("values");

                itemResult.set("enum", enumCaseArray((Enum[]) valuesMethod.invoke(null)));
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        } else  {
            buildObjectSchema(crApiVersion, itemResult, elementType, description);
        }
        return result;
    }

    /**
     * Utility method to check if Map key-value pair match specific types.
     * @param propertyType property to check
     * @param keyType key Class
     * @param valueType value Class
     * @return true if key-value types are equal to specified types, false otherwise.
     */
    private boolean isMapOfTypes(PropertyType propertyType, Class<?> keyType, Class<?> valueType) {
        java.lang.reflect.Type[] types = ((ParameterizedType) propertyType.getGenericType()).getActualTypeArguments();
        return keyType.equals(types[0]) && valueType.equals(types[1]);
    }

    private ObjectNode buildBasicTypeSchema(Property element, Class<?> type) {
        ObjectNode result = nf.objectNode();

        String typeName;
        Type typeAnno = element.getAnnotation(Type.class);
        if (typeAnno == null) {
            typeName = typeName(type);
            if (Map.class.equals(type)) {
                if (isMapOfTypes(element.getType(), String.class, String.class)) {
                    preserveUnknownStringFields(result);
                } else {
                    preserveUnknownFields(result);
                }
            }
        } else {
            typeName = typeAnno.value();
        }
        result.put("type", typeName);

        return result;
    }

    private ObjectNode buildQuantityTypeSchema() {
        ObjectNode result = nf.objectNode();

        ObjectNode additionalProperties = result.putObject("additionalProperties");
        ArrayNode anyOf = additionalProperties.putArray("anyOf");
        anyOf.addObject().put("type", "integer");
        anyOf.addObject().put("type", "string");
        additionalProperties.put("pattern", "^(\\+|-)?(([0-9]+(\\.[0-9]*)?)|(\\.[0-9]+))(([KMGTPE]i)|[numkMGTPE]|([eE](\\+|-)?(([0-9]+(\\.[0-9]*)?)|(\\.[0-9]+))))?$");
        additionalProperties.put("x-kubernetes-int-or-string", true);
        result.put("type", "object");
        return result;
    }

    private void preserveUnknownFields(ObjectNode result)    {
        if (crdApiVersion.compareTo(V1) >= 0) {
            result.put("x-kubernetes-preserve-unknown-fields", true);
        }
    }

    private void preserveUnknownStringFields(ObjectNode result)    {
        if (crdApiVersion.compareTo(V1) >= 0) {
            ObjectNode additionalProperties = result.putObject("additionalProperties");
            additionalProperties.put("type", "string");
        }
    }

    /**
     * Adds CEL validation rules to the CRD for additional validation
     *
     * @param crApiVersion  Strimzi API version for which the validation rules should be generated
     * @param element       The element where the CEL validation annotation will be checked
     * @param result        The JSON Object where the CEL validation rules should be added
     */
    private void celValidationRules(ApiVersion crApiVersion, Property element, ObjectNode result)    {
        // Annotation from the field has the priority. But if it does not exist, we try the type
        CelValidation celValidation = element.getAnnotation(CelValidation.class);
        if (celValidation == null) {
            celValidation = element.getType().getType().getAnnotation(CelValidation.class);
        }

        if (celValidation != null
                && celValidation.rules() != null
                && celValidation.rules().length > 0) {
            List<ObjectNode> ruleObjects = new ArrayList<>();

            for (CelValidation.CelValidationRule rule : celValidation.rules()) {
                if (rule.versions().isEmpty() || ApiVersion.parseRange(rule.versions()).contains(crApiVersion)) {
                    ObjectNode celRule = nf.objectNode();
                    celRule.put("rule", rule.rule());

                    if (!rule.message().isEmpty())  {
                        celRule.put("message", rule.message());
                    }
                    if (!rule.messageExpression().isEmpty())  {
                        celRule.put("messageExpression", rule.messageExpression());
                    }
                    if (!rule.fieldPath().isEmpty())  {
                        celRule.put("fieldPath", rule.fieldPath());
                    }
                    if (!rule.reason().isEmpty())  {
                        celRule.put("reason", rule.reason());
                    }

                    ruleObjects.add(celRule);
                }
            }

            // There might be API versions where there is no rule. So we create the validations array only if there are any rules
            if (!ruleObjects.isEmpty()) {
                ArrayNode rules = result.putArray("x-kubernetes-validations");
                rules.addAll(ruleObjects);
            }
        }
    }

    private void addDescription(ApiVersion crApiVersion, ObjectNode result, AnnotatedElement element) {
        Description description = selectVersion(crApiVersion, element, Description.class);
        if (description != null) {
            result.put("description", DocGenerator.getDescription(description));
        }
    }

    @SuppressWarnings("unchecked")
    private ObjectNode addSimpleTypeConstraints(ApiVersion crApiVersion, ObjectNode result, Property property) {

        Example example = property.getAnnotation(Example.class);
        // TODO make support versions
        if (example != null) {
            result.put("example", example.value());
        }

        Minimum minimum = selectVersion(crApiVersion, property, Minimum.class);
        if (minimum != null) {
            result.put("minimum", minimum.value());
        }
        Maximum maximum = selectVersion(crApiVersion, property, Maximum.class);
        if (maximum != null) {
            result.put("maximum", maximum.value());
        }

        Pattern first = selectVersion(crApiVersion, property, Pattern.class);
        if (first != null) {
            result.put("pattern", first.value());
        }

        if (property.getType().isEnum()) {
            result.set("enum", enumCaseArray(property.getType().getEnumElements()));
        }

        if (property.getDeclaringClass().isAnnotationPresent(JsonTypeInfo.class)
            && property.getName().equals(property.getDeclaringClass().getAnnotation(JsonTypeInfo.class).property())) {
            result.set("enum", stringArray(Property.subtypeNames(crApiVersion, property.getDeclaringClass())));
        }

        return result;
    }

    private <T extends Annotation> T selectVersion(ApiVersion crApiVersion, AnnotatedElement element, Class<T> cls) {
        T[] wrapperAnnotation = element.getAnnotationsByType(cls);
        if (wrapperAnnotation == null) {
            return null;
        }
        checkDisjointVersions(element, wrapperAnnotation, cls);
        return Arrays.stream(wrapperAnnotation)
                // TODO crApiVersion == null does not really imply we should return the first description.
                .filter(element1 -> crApiVersion == null || apiVersion(element1, cls).contains(crApiVersion))
                .findFirst().orElse(null);
    }

    @SuppressWarnings("unchecked")
    private <T> void checkDisjointVersions(AnnotatedElement annotated, T[] wrapperAnnotation, Class<T> annotationClass) {
        long count = Arrays.stream(wrapperAnnotation).count();

        long distinctCount = Arrays.stream(wrapperAnnotation)
                .map(element -> apiVersion(element, annotationClass)).distinct().count();
        if (count != distinctCount) {
            err("Duplicate version ranges on " + annotated);
        }
        Arrays.stream(wrapperAnnotation)
                .map(element -> apiVersion(element, annotationClass))
                .flatMap(x -> Arrays.stream(wrapperAnnotation)
                        .map(y -> apiVersion(y, annotationClass))
                        .filter(y -> !y.equals(x))
                        .map(y -> new VersionRange[]{x, y}))
                .forEach(pair -> {
                    if (pair[0].intersects(pair[1])) {
                        err(pair[0] + " and " + pair[1] + " are not disjoint on " + annotated);
                    }
                });
    }

    private static <T> VersionRange<ApiVersion> apiVersion(T element, Class<T> annotationClass) {
        try {
            Method apiVersionsMethod = annotationClass.getDeclaredMethod("apiVersions");
            String apiVersions = (String) apiVersionsMethod.invoke(element);
            return ApiVersion.parseRange(apiVersions);
        } catch (ReflectiveOperationException | ClassCastException e) {
            throw new RuntimeException(e);
        }
    }

    private <E extends Enum<E>> ArrayNode enumCaseArray(E[] values) {
        ArrayNode arrayNode = nf.arrayNode();
        arrayNode.addAll(Schema.enumCases(values));
        return arrayNode;
    }

    private String typeName(Class<?> type) {
        if (String.class.equals(type)) {
            return "string";
        } else if (int.class.equals(type)
                || Integer.class.equals(type)
                || long.class.equals(type)
                || Long.class.equals(type)
                || short.class.equals(type)
                || Short.class.equals(type)) {
            return "integer";
        } else if (boolean.class.equals(type)
                || Boolean.class.equals(type)) {
            return "boolean";
        } else if (Map.class.equals(type)) {
            return "object";
        } else if (List.class.equals(type)
                || type.isArray()) {
            return "array";
        } else if (type.isEnum()) {
            return "string";
        } else if (Double.class.equals(type)
                || double.class.equals(type)
                || float.class.equals(type)
                || Float.class.equals(type)) {
            return "number";
        } else {
            throw new RuntimeException(type.getName());
        }
    }

    ArrayNode stringArray(Iterable<String> list) {
        ArrayNode arrayNode = nf.arrayNode();
        for (String sn : list) {
            arrayNode.add(sn);
        }
        return arrayNode;
    }

    static class CommandOptions {
        private boolean yaml = false;
        private final LinkedHashMap<String, String> labels = new LinkedHashMap<>();
        VersionRange<KubeVersion> targetKubeVersions = null;
        ApiVersion crdApiVersion = null;
        List<ApiVersion> apiVersions = null;
        VersionRange<ApiVersion> describeVersions = null;
        ApiVersion storageVersion = null;
        Map<String, Class<? extends CustomResource>> classes = new HashMap<>();
        private final ConversionStrategy conversionStrategy;

        @SuppressWarnings({"unchecked", "CyclomaticComplexity", "JavaNCSS", "MethodLength"})
        public CommandOptions(String[] args) throws ClassNotFoundException, IOException {
            String conversionServiceUrl = null;
            String conversionServiceName = null;
            String conversionServiceNamespace = null;
            String conversionServicePath = null;
            int conversionServicePort = -1;
            String conversionServiceCaBundle = null;
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg.startsWith("--")) {
                    switch (arg) {
                        case "--yaml":
                            yaml = true;
                            break;
                        case "--label":
                            i++;
                            int index = args[i].indexOf(":");
                            if (index == -1) {
                                argParseErr("Invalid --label " + args[i]);
                            }
                            labels.put(args[i].substring(0, index), args[i].substring(index + 1));
                            break;
                        case "--target-kube":
                            if (targetKubeVersions != null) {
                                argParseErr("--target-kube can only be specified once");
                            } else if (i >= arg.length() - 1) {
                                argParseErr("--target-kube needs an argument");
                            } else {
                                targetKubeVersions = KubeVersion.parseRange(args[++i]);
                            }
                            break;
                        case "--crd-api-version":
                            if (crdApiVersion != null) {
                                argParseErr("--crd-api-version can only be specified once");
                            } else if (i >= arg.length() - 1) {
                                argParseErr("--crd-api-version needs an argument");
                            } else {
                                crdApiVersion = ApiVersion.parse(args[++i]);
                            }
                            break;
                        case "--api-versions":
                            if (apiVersions != null) {
                                argParseErr("--api-versions can only be specified once");
                            } else if (i >= arg.length() - 1) {
                                argParseErr("--api-versions needs an argument");
                            } else {
                                apiVersions = Arrays.stream(args[++i].split(",")).map(v -> ApiVersion.parse(v)).collect(Collectors.toList());
                            }
                            break;
                        case "--describe-api-versions":
                            if (describeVersions != null) {
                                argParseErr("--describe-api-versions can only be specified once");
                            } else if (i >= arg.length() - 1) {
                                argParseErr("--describe-api-versions needs an argument");
                            } else {
                                describeVersions = ApiVersion.parseRange(args[++i]);
                            }
                            break;
                        case "--storage-version":
                            if (storageVersion != null) {
                                argParseErr("--storage-version can only be specified once");
                            } else if (i >= arg.length() - 1) {
                                argParseErr("--storage-version needs an argument");
                            } else {
                                storageVersion = ApiVersion.parse(args[++i]);
                            }
                            break;
                        case "--conversion-service-url":
                            if (conversionServiceUrl != null) {
                                argParseErr("--conversion-service-url can only be specified once");
                            } else if (i >= arg.length() - 1) {
                                argParseErr("--conversion-service-url needs an argument");
                            } else {
                                conversionServiceUrl = args[++i];
                            }
                            break;
                        case "--conversion-service-name":
                            if (conversionServiceName != null) {
                                argParseErr("--conversion-service-name can only be specified once");
                            } else if (i >= arg.length() - 1) {
                                argParseErr("--conversion-service-name needs an argument");
                            } else {
                                conversionServiceName = args[++i];
                            }
                            break;
                        case "--conversion-service-namespace":
                            if (conversionServiceNamespace != null) {
                                argParseErr("--conversion-service-namespace can only be specified once");
                            } else if (i >= arg.length() - 1) {
                                argParseErr("--conversion-service-namespace needs an argument");
                            } else {
                                conversionServiceNamespace = args[++i];
                            }
                            break;
                        case "--conversion-service-path":
                            if (conversionServicePath != null) {
                                argParseErr("--conversion-service-path can only be specified once");
                            } else if (i >= arg.length() - 1) {
                                argParseErr("--conversion-service-path needs an argument");
                            } else {
                                conversionServicePath = args[++i];
                            }
                            break;
                        case "--conversion-service-port":
                            if (conversionServicePort > 0) {
                                argParseErr("--conversion-service-port can only be specified once");
                            } else if (i >= arg.length() - 1) {
                                argParseErr("--conversion-service-port needs an argument");
                            } else {
                                conversionServicePort = parseInt(args[++i]);
                            }
                            break;
                        case "--conversion-service-ca-bundle":
                            if (conversionServiceCaBundle != null) {
                                argParseErr("--conversion-service-ca-bundle can only be specified once");
                            } else if (i >= arg.length() - 1) {
                                argParseErr("--conversion-service-ca-bundle needs an argument");
                            } else {
                                // TODO read file and base64
                                File file = new File(args[++i]);
                                byte[] bundleBytes = Files.readAllBytes(file.toPath());
                                conversionServiceCaBundle = new String(bundleBytes, StandardCharsets.UTF_8);
                                if (!conversionServiceCaBundle.contains("-----BEGIN CERTIFICATE-----")) {
                                    throw new IllegalStateException("File " + file + " given by --conversion-service-ca-bundle should be PEM encoded");
                                }
                                conversionServiceCaBundle = Base64.getEncoder().encodeToString(bundleBytes);
                            }
                            break;
                        default:
                            throw new RuntimeException("Unsupported command line option " + arg);
                    }
                } else {
                    String className = arg.substring(0, arg.indexOf('='));
                    String fileName = arg.substring(arg.indexOf('=') + 1).replace("/", File.separator);
                    Class<?> cls = Class.forName(className);
                    if (!CustomResource.class.equals(cls)
                            && CustomResource.class.isAssignableFrom(cls)) {
                        classes.put(fileName, (Class<? extends CustomResource>) cls);
                    } else {
                        argParseErr(cls + " is not a subclass of " + CustomResource.class.getName());
                    }
                }
            }
            if (targetKubeVersions == null) {
                targetKubeVersions = KubeVersion.V1_16_PLUS;
            }
            if (crdApiVersion == null) {
                crdApiVersion = ApiVersion.V1;
            }
            if (conversionServiceName != null) {
                conversionStrategy = new WebhookConversionStrategy(conversionServiceName, conversionServiceNamespace, conversionServicePath, conversionServicePort, conversionServiceCaBundle);
            } else if (conversionServiceUrl != null) {
                conversionStrategy = new WebhookConversionStrategy(conversionServiceUrl, conversionServiceCaBundle);
            } else {
                conversionStrategy = new NoneConversionStrategy();
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        CommandOptions opts = new CommandOptions(args);

        CrdGenerator generator = new CrdGenerator(opts.targetKubeVersions, opts.crdApiVersion,
                opts.yaml ? YAML_MAPPER.configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, true) : JSON_MATTER,
                opts.labels, new DefaultReporter(),
                opts.apiVersions, opts.storageVersion, null, opts.conversionStrategy, opts.describeVersions);
        for (Map.Entry<String, Class<? extends CustomResource>> entry : opts.classes.entrySet()) {
            File file = new File(entry.getKey());
            if (file.getParentFile().exists()) {
                if (!file.getParentFile().isDirectory()) {
                    generator.err(file.getParentFile() + " is not a directory");
                }
            } else if (!file.getParentFile().mkdirs()) {
                generator.err(file.getParentFile() + " does not exist and could not be created");
            }
            try (Writer w = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8)) {
                generator.generate(entry.getValue(), w);
            }
        }

        if (generator.numErrors > 0) {
            System.err.println("There were " + generator.numErrors + " errors");
            System.exit(1);
        } else {
            System.exit(0);
        }
    }
}
