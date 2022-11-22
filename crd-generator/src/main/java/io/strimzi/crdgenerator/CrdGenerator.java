/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.annotations.KubeVersion;
import io.strimzi.api.annotations.VersionRange;
import io.strimzi.crdgenerator.annotations.Alternation;
import io.strimzi.crdgenerator.annotations.Alternative;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Example;
import io.strimzi.crdgenerator.annotations.Maximum;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.MinimumItems;
import io.strimzi.crdgenerator.annotations.OneOf;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.strimzi.crdgenerator.annotations.Type;

import static io.strimzi.api.annotations.ApiVersion.V1;
import static io.strimzi.crdgenerator.Property.hasAnyGetterAndAnySetter;
import static io.strimzi.crdgenerator.Property.properties;
import static io.strimzi.crdgenerator.Property.sortedProperties;
import static io.strimzi.crdgenerator.Property.subtypes;
import static java.lang.Integer.parseInt;
import static java.lang.reflect.Modifier.isAbstract;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

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
    // TODO CrdValidator
    // extraProperties
    // @Buildable

    public interface Reporter {
        void warn(String s);

        void err(String s);
    }

    public static class DefaultReporter implements Reporter {

        public void warn(String s) {
            System.err.println("CrdGenerator: warn: " + s);
        }

        public void err(String s) {
            System.err.println("CrdGenerator: error: " + s);
        }
    }

    Reporter reporter = new DefaultReporter();

    public void warn(String s) {
        reporter.warn(s);
    }

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


    private final VersionRange<KubeVersion> targetKubeVersions;
    private final ObjectMapper mapper;
    private final JsonNodeFactory nf;
    private final Map<String, String> labels;
    private final ConversionStrategy conversionStrategy;

    private int numErrors;

    public CrdGenerator(VersionRange<KubeVersion> targetKubeVersions, ApiVersion crdApiVersion) {
        this(targetKubeVersions, crdApiVersion, CrdGenerator.YAML_MAPPER, emptyMap(), new DefaultReporter(),
                emptyList(), null, null, new NoneConversionStrategy(), null);
    }

    /**
     * @param targetKubeVersions The targetted version(s) of Kubernetes.
     * @param crdApiVersion The version of the CRD API for which to generate the CRD.
     * @param mapper The object mapper.
     * @param labels The labels to add to the CRD.
     * @param reporter The error reporter.
     * @param apiVersions The API versions to generate (allows selecting a subset of those in the @Crd annotation).
     * @param storageVersion If not null, override the storageVersion to the given value.
     * @param servedVersions If not null, override the serverd versions according to the given range.
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
        checkKubeVersionsSupportCrdVersion(crdApiVersion);
        ObjectNode result = nf.objectNode();
        result.put("group", crd.group());

        ArrayNode versions = nf.arrayNode();

        // Kube apiserver with CRD v1beta1 is picky about only using per-version subresources, schemas and printercolumns
        // if they actually differ across the versions. If they're the same, it insists these things are
        // declared top level
        // With CRD v1 they have to be per-version :face-with-rolling-eyes:
        Map<ApiVersion, ObjectNode> subresources = buildSubresources(crd);
        boolean perVersionSubResources = needsPerVersion("subresources", subresources);
        Map<ApiVersion, ObjectNode> schemas = buildSchemas(crd, crdClass);
        boolean perVersionSchemas = needsPerVersion("schemas", schemas);
        Map<ApiVersion, ArrayNode> printerColumns = buildPrinterColumns(crd);
        boolean perVersionPrinterColumns = needsPerVersion("additionalPrinterColumns", printerColumns);

        result.set("names", buildNames(crd.names()));
        result.put("scope", crd.scope());

        if (!perVersionPrinterColumns) {
            ArrayNode cols = printerColumns.values().iterator().next();
            if (!cols.isEmpty()) {
                result.set("additionalPrinterColumns", cols);
            }
        }
        if (!perVersionSubResources) {
            ObjectNode subresource = subresources.values().iterator().next();
            if (!subresource.isEmpty()) {
                result.set("subresources", subresource);
            }
        }
        if (conversionStrategy instanceof WebhookConversionStrategy) {
            // "Webhook": must be None if spec.preserveUnknownFields is true
            result.put("preserveUnknownFields", false);
        }
        result.set("conversion", buildConversion(crdApiVersion));

        for (Crd.Spec.Version version : crd.versions()) {
            ApiVersion crApiVersion = ApiVersion.parse(version.name());
            if (!shouldIncludeVersion(crApiVersion)) {
                continue;
            }
            ObjectNode versionNode = versions.addObject();
            versionNode.put("name", crApiVersion.toString());
            versionNode.put("served", servedVersion != null ? servedVersion.contains(crApiVersion) : version.served());
            versionNode.put("storage", storageVersion != null ? crApiVersion.equals(storageVersion) : version.storage());

            if (perVersionSubResources) {
                ObjectNode subresourcesForVersion = subresources.get(crApiVersion);
                if (!subresourcesForVersion.isEmpty()) {
                    versionNode.set("subresources", subresourcesForVersion);
                }
            }
            if (perVersionPrinterColumns) {
                ArrayNode cols = printerColumns.get(crApiVersion);
                if (!cols.isEmpty()) {
                    versionNode.set("additionalPrinterColumns", cols);
                }
            }
            if (perVersionSchemas) {
                versionNode.set("schema", schemas.get(crApiVersion));
            }
        }
        result.set("versions", versions);

        if (crdApiVersion.compareTo(V1) < 0
                && targetKubeVersions.intersects(KubeVersion.parseRange("1.11-1.15"))) {
            result.put("version", Arrays.stream(crd.versions())
                    .map(v -> ApiVersion.parse(v.name()))
                    .filter(this::shouldIncludeVersion)
                    .findFirst()
                    .map(ApiVersion::toString)
                    .orElseThrow());
        }

        if (!perVersionSchemas) {
            result.set("validation", schemas.values().iterator().next());
        }

        return result;
    }

    private ObjectNode buildConversion(ApiVersion crdApiVersion) {
        ObjectNode conversion = nf.objectNode();
        if (conversionStrategy instanceof NoneConversionStrategy) {
            conversion.put("strategy", "None");
        } else if (conversionStrategy instanceof WebhookConversionStrategy) {
            boolean v1Beta1CrdApi = crdApiVersion.compareTo(V1) < 0;
            conversion.put("strategy", "Webhook");
            WebhookConversionStrategy webhookStrategy = (WebhookConversionStrategy) conversionStrategy;
            ObjectNode webhook = conversion.putObject(v1Beta1CrdApi ? "webhookClientConfig" : "webhook");
            (v1Beta1CrdApi ? conversion : webhook).putArray("conversionReviewVersions").add("v1").add("v1beta1");
            ObjectNode webhookClientConfig = (v1Beta1CrdApi ? conversion : webhook).putObject(v1Beta1CrdApi ? "webhookClientConfig" : "clientConfig");
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

    private boolean needsPerVersion(String property, Map<ApiVersion, ? extends ContainerNode<?>> subresources) {
        if (crdApiVersion.compareTo(V1) >= 0) {
            return true;
        }
        HashSet<? extends ContainerNode<?>> set = new HashSet<>(subresources.values());
        int distinct = set.size();
        boolean perVersionSubResources;
        if (KubeVersion.supportsSchemaPerVersion(targetKubeVersions)) {
            perVersionSubResources = distinct > 1;
        } else {
            if (distinct > 1) {
                err("The " + property + " are per-version, but that's not supported " +
                        "by at least one Kubernetes version in " + targetKubeVersions);
            }
            perVersionSubResources = false;
        }
        return perVersionSubResources;
    }

    /**
     * Prevent things like v1 CRD API on kube &lt; 1.16, or v1beta1 on kube &gt; 2.21
     * @param crdApiVersion The version of the CRD API being generated.
     */
    private void checkKubeVersionsSupportCrdVersion(ApiVersion crdApiVersion) {
        if (!targetKubeVersions.lower().supportsCrdApiVersion(crdApiVersion)) {
            err("Kubernetes version " + targetKubeVersions.lower() +
                    " doesn't support CustomResourceDefinition API at " + crdApiVersion);
        }
        if (targetKubeVersions.upper() != null &&
                !targetKubeVersions.upper().supportsCrdApiVersion(crdApiVersion)) {
            err("Kubernetes version " + targetKubeVersions.upper() +
                    " doesn't support CustomResourceDefinition API at " + crdApiVersion);
        }
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
        if (scales.length > 1 && !KubeVersion.supportsSchemaPerVersion(targetKubeVersions)) {
            err("Multiple scales specified but " + targetKubeVersions.lower() + " doesn't support schema per version");
        }
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
        } else if (filteredScales.size() > 1 && KubeVersion.supportsSchemaPerVersion(targetKubeVersions))  {
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
        // OpenShift Origin 3.10-rc0 doesn't like the `type: object` in schema root
        boolean noTopLevelTypeProperty = targetKubeVersions.intersects(KubeVersion.parseRange("1.11-1.15"));
        result.set("openAPIV3Schema", buildObjectSchema(crApiVersion, crdClass, crdClass, crdApiVersion.compareTo(V1) >= 0 || !noTopLevelTypeProperty, description));
        return result;
    }

    private ObjectNode buildObjectSchema(ApiVersion crApiVersion, AnnotatedElement annotatedElement, Class<?> crdClass, boolean description) {
        return buildObjectSchema(crApiVersion, annotatedElement, crdClass, true, description);
    }

    private ObjectNode buildObjectSchema(ApiVersion crApiVersion, AnnotatedElement annotatedElement, Class<?> crdClass, boolean printType, boolean description) {
        ObjectNode result = nf.objectNode();
        buildObjectSchema(crApiVersion, result, crdClass, printType, description);
        return result;
    }

    private void buildObjectSchema(ApiVersion crApiVersion, ObjectNode result, Class<?> crdClass, boolean printType, boolean description) {
        checkClass(crdClass);
        if (printType) {
            result.put("type", "object");
        }

        result.set("properties", buildSchemaProperties(crApiVersion, crdClass, description));
        ArrayNode oneOf = buildSchemaOneOf(crdClass);
        if (oneOf != null) {
            result.set("oneOf", oneOf);
        }
        ArrayNode required = buildSchemaRequired(crApiVersion, crdClass);
        if (required.size() > 0) {
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
                ArrayNode required = alternative.putArray("required");
                for (OneOf.Alternative.Property prop: alt.value()) {
                    if (prop.required()) {
                        required.add(prop.value());
                    }
                }
            }
        } else {
            alternatives = null;
        }
        return alternatives;
    }

    private void checkClass(Class<?> crdClass) {
        if (!crdClass.isAnnotationPresent(JsonInclude.class)) {
            err(crdClass + " is missing @JsonInclude");
        } else if (!crdClass.getAnnotation(JsonInclude.class).value().equals(JsonInclude.Include.NON_NULL)
                && !crdClass.getAnnotation(JsonInclude.class).value().equals(JsonInclude.Include.NON_DEFAULT)) {
            err(crdClass + " has a @JsonInclude value other than Include.NON_NULL");
        }
        if (!isAbstract(crdClass.getModifiers())) {
            checkForBuilderClass(crdClass, crdClass.getName() + "Builder");
            checkForBuilderClass(crdClass, crdClass.getName() + "Fluent");
            checkForBuilderClass(crdClass, crdClass.getName() + "FluentImpl");
        }
        if (!Modifier.isAbstract(crdClass.getModifiers())) {
            hasAnyGetterAndAnySetter(crdClass);
        } else {
            for (Class c : subtypes(crdClass)) {
                hasAnyGetterAndAnySetter(c);
                checkDiscriminatorIsIncluded(crdClass, c);
            }
        }
        checkInherits(crdClass, "java.io.Serializable");
        if (crdClass.getName().startsWith("io.strimzi.api.")) {
            checkInherits(crdClass, "io.strimzi.api.kafka.model.UnknownPropertyPreserving");
        }
        if (!Modifier.isAbstract(crdClass.getModifiers())) {
            checkClassOverrides(crdClass, "hashCode");
        }
        checkClassOverrides(crdClass, "equals", Object.class);
    }

    private void checkDiscriminatorIsIncluded(Class<?> crdClass, Class c) {
        try {
            String typePropertyName = crdClass.getAnnotation(JsonTypeInfo.class).property();
            String methodName = "get" + typePropertyName.substring(0, 1).toUpperCase(Locale.ENGLISH) + typePropertyName.substring(1).toLowerCase(Locale.ENGLISH);
            @SuppressWarnings("unchecked")
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
        TreeMap<String, Property> result = new TreeMap<>();
        for (Class subtype : Property.subtypes(crdClass)) {
            result.putAll(properties(crApiVersion, subtype));
        }
        result.putAll(properties(crApiVersion, crdClass));
        JsonPropertyOrder order = crdClass.getAnnotation(JsonPropertyOrder.class);
        return sortedProperties(order != null ? order.value() : null, result).values();
    }

    private ArrayNode buildSchemaRequired(ApiVersion crApiVersion, Class<?> crdClass) {
        ArrayNode result = nf.arrayNode();

        for (Property property : unionOfSubclassProperties(crApiVersion, crdClass)) {
            if (property.isAnnotationPresent(JsonProperty.class)
                    && property.getAnnotation(JsonProperty.class).required()
                || property.isDiscriminator()) {
                result.add(property.getName());
            }
        }
        return result;
    }

    private ObjectNode buildSchemaProperties(ApiVersion crApiVersion, Class<?> crdClass, boolean description) {
        ObjectNode properties = nf.objectNode();
        for (Property property : unionOfSubclassProperties(crApiVersion, crdClass)) {
            if (property.getType().getType().isAnnotationPresent(Alternation.class)) {
                List<Property> alternatives = property.getAlternatives(crApiVersion, targetKubeVersions);
                if (alternatives.size() == 1) {
                    properties.set(property.getName(), buildSchema(crApiVersion, alternatives.get(0), description));
                } else if (alternatives.size() == 0) {
                    err("Class " + property.getType().getType().getName() + " is annotated with " +
                            "@" + Alternation.class.getSimpleName() + " but has no " +
                            "@" + Alternative.class.getSimpleName() + "-annotated properties");
                } else {
                    // TODO strictly speaking this is completely wrong if multiple versions aren't supported
                    buildMultiTypeProperty(crApiVersion, properties, property, alternatives, description);
                }
            } else {
                buildProperty(crApiVersion, properties, property, description);
            }
        }
        return properties;
    }

    private void buildMultiTypeProperty(ApiVersion crApiVersion, ObjectNode properties, Property property, List<Property> alternatives, boolean description) {
        ArrayNode oneOfAlternatives = nf.arrayNode(alternatives.size());

        for (Property alternative : alternatives)   {
            oneOfAlternatives.add(buildSchema(crApiVersion, alternative, description));
        }

        ObjectNode oneOf = nf.objectNode();
        oneOf.set("oneOf", oneOfAlternatives);
        properties.set(property.getName(), oneOf);
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
        } else if (Schema.isJsonScalarType(returnType)
                || Map.class.equals(returnType)) {            
            schema = addSimpleTypeConstraints(crApiVersion, buildBasicTypeSchema(property, returnType), property);
        } else if (returnType.isArray() || List.class.equals(returnType)) {
            schema = buildArraySchema(crApiVersion, property, property.getType(), description);
        } else {
            schema = buildObjectSchema(crApiVersion, property, returnType, description);
        }

        if (description) {
            addDescription(crApiVersion, schema, property);
        }

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
            preserveUnknownFields(itemResult);
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
            buildObjectSchema(crApiVersion, itemResult, elementType, true, description);
        }
        return result;
    }

    private ObjectNode buildBasicTypeSchema(AnnotatedElement element, Class type) {
        ObjectNode result = nf.objectNode();

        String typeName;
        Type typeAnno = element.getAnnotation(Type.class);
        if (typeAnno == null) {
            typeName = typeName(type);
            if (Map.class.equals(type)) {
                preserveUnknownFields(result);
            }
        } else {
            typeName = typeAnno.value();
        }
        result.put("type", typeName);

        return result;
    }

    private void preserveUnknownFields(ObjectNode result)    {
        if (crdApiVersion.compareTo(V1) >= 0) {
            result.put("x-kubernetes-preserve-unknown-fields", true);
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
            result.set("enum", stringArray(Property.subtypeNames(property.getDeclaringClass())));
        }

        // The deprecated field cannot be set in Kube OpenAPI v3 schema. But we should keep this code for future when it might be possible.
        /*Deprecated deprecated = property.getAnnotation(Deprecated.class);
        if (deprecated == null) {
            deprecated = property.getType().getType().getAnnotation(Deprecated.class);
        }
        if (deprecated != null) {
            result.put("deprecated", true);
        }*/

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
        long count = Arrays.stream(wrapperAnnotation)
                .map(element -> apiVersion(element, annotationClass)).count();
        if (count > 1 && !KubeVersion.supportsSchemaPerVersion(targetKubeVersions)) {
            err("Target kubernetes versions " + targetKubeVersions + " don't support schema-per-version, but multiple versions present on " + annotated);
        }

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

    @SuppressWarnings("unchecked")
    private static <T> VersionRange<ApiVersion> apiVersion(T element, Class<T> annotationClass) {
        try {
            Method apiVersionsMethod = annotationClass.getDeclaredMethod("apiVersions");
            String apiVersions = (String) apiVersionsMethod.invoke(element);
            return ApiVersion.parseRange(apiVersions);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        } catch (ClassCastException e) {
            throw new RuntimeException(e);
        }
    }

    private <E extends Enum<E>> ArrayNode enumCaseArray(E[] values) {
        ArrayNode arrayNode = nf.arrayNode();
        arrayNode.addAll(Schema.enumCases(values));
        return arrayNode;
    }

    private String typeName(Class type) {
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
        private LinkedHashMap<String, String> labels = new LinkedHashMap<>();
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
                targetKubeVersions = KubeVersion.parseRange("1.11+");
            }
            if (crdApiVersion == null) {
                crdApiVersion = ApiVersion.V1BETA1;
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
