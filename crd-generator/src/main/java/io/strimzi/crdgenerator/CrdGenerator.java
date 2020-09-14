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
import io.fabric8.kubernetes.client.CustomResource;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static io.strimzi.crdgenerator.Property.hasAnyGetterAndAnySetter;
import static io.strimzi.crdgenerator.Property.properties;
import static io.strimzi.crdgenerator.Property.sortedProperties;
import static io.strimzi.crdgenerator.Property.subtypes;
import static java.lang.reflect.Modifier.isAbstract;
import static java.util.Arrays.asList;
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
public class CrdGenerator {
    private final KubeVersion targetKubeVersion;


    // TODO CrdValidator
    // extraProperties
    // @Buildable

    private void warn(String s) {
        System.err.println("CrdGenerator: warn: " + s);
    }

    private static void argParseErr(String s) {
        System.err.println("CrdGenerator: error: " + s);
    }

    private void err(String s) {
        System.err.println("CrdGenerator: error: " + s);
        numErrors++;
    }

    private final ObjectMapper mapper;
    private final JsonNodeFactory nf;
    private final Map<String, String> labels;
    private int numErrors;

    @Deprecated
    CrdGenerator(ObjectMapper mapper) {
        this(KubeVersion.V1_11, mapper, emptyMap());
    }

    @Deprecated
    CrdGenerator(ObjectMapper mapper, Map<String, String> labels) {
        this(KubeVersion.V1_11, mapper, labels);
    }

    CrdGenerator(KubeVersion targetKubeVersion, ObjectMapper mapper) {
        this(targetKubeVersion, mapper, emptyMap());
    }

    CrdGenerator(KubeVersion targetKubeVersion, ObjectMapper mapper, Map<String, String> labels) {
        this.targetKubeVersion = targetKubeVersion;
        this.mapper = mapper;
        this.nf = mapper.getNodeFactory();
        this.labels = labels;
    }

    void generate(Class<? extends CustomResource> crdClass, Writer out) throws IOException {
        ObjectNode node = nf.objectNode();
        Crd crd = crdClass.getAnnotation(Crd.class);
        if (crd == null) {
            err(crdClass + " is not annotated with @Crd");
        } else {
            String apiVersion = crd.apiVersion();
            if (!apiVersion.startsWith("apiextensions.k8s.io")) {
                warn("@ Crd.apiVersion is the API version of the CustomResourceDefinition," +
                        "not the version of instances of the custom resource. " +
                        "It should almost certainly be apiextensions.k8s.io/${some-version}.");
            }
            ApiVersion crdApiVersion = ApiVersion.parse(apiVersion.substring(apiVersion.indexOf("/") + 1));
            node.put("apiVersion", apiVersion)
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
    }

    @SuppressWarnings("deprecation")
    private ObjectNode buildSpec(ApiVersion crdApiVersion,
                                 Crd.Spec crd, Class<? extends CustomResource> crdClass) {
        if (!targetKubeVersion.supportsCrdApiVersion(crdApiVersion)) {
            throw new RuntimeException("Kubernetes version " + targetKubeVersion + " doesn't support CustomResourceDefinition API at " + crdApiVersion);
        }
        ObjectNode result = nf.objectNode();
        result.put("group", crd.group());

        if (crd.versions().length != 0) {
            ArrayNode versions = nf.arrayNode();
            for (Crd.Spec.Version version : crd.versions()) {
                ObjectNode versionNode = versions.addObject();
                ApiVersion crApiVersion = ApiVersion.parse(version.name());
                versionNode.put("name", crApiVersion.toString());
                versionNode.put("served", version.served());
                versionNode.put("storage", version.storage());
                if (targetKubeVersion.supportsSchemaPerVersion()) {
                    ObjectNode subresources = buildSubresources(crd, crApiVersion);
                    if (subresources != null) {
                        versionNode.set("subresources", subresources);
                    }
                    ArrayNode cols = buildAdditionalPrinterColumns(crd, crApiVersion);
                    if (!cols.isEmpty()) {
                        versionNode.set("additionalPrinterColumns", cols);
                    }
                    versionNode.set("schema", buildValidation(crdClass, crApiVersion));
                }
                // addn printer columns
            }
            result.set("versions", versions);
        }

        if (crdApiVersion.compareTo(ApiVersion.V1) < 0
                && !crd.version().isEmpty()) {
            result.put("version", crd.version());
        }
        result.put("scope", crd.scope());
        result.set("names", buildNames(crd.names()));

        if (!targetKubeVersion.supportsSchemaPerVersion()) {
            ArrayNode cols = buildAdditionalPrinterColumns(crd, null);
            if (!cols.isEmpty()) {
                result.set("additionalPrinterColumns", cols);
            }

            ObjectNode subresources = buildSubresources(crd, null);
            if (subresources != null) {
                result.set("subresources", subresources);
            }
            result.set("validation", buildValidation(crdClass, null));
        }
        return result;
    }

    private ObjectNode buildSubresources(Crd.Spec crd, ApiVersion crApiVersion) {
        ObjectNode subresources;
        if (crd.subresources().status().length != 0) {
            subresources = nf.objectNode();
            ObjectNode status = buildStatus(crd, crApiVersion);
            if (status != null) {
                subresources.set("status", status);
            }

            ObjectNode scaleNode = buildScale(crd, crApiVersion);
            if (scaleNode != null) {
                subresources.set("scale", scaleNode);
            }
        } else {
            subresources = null;
        }
        return subresources;
    }

    private ObjectNode buildStatus(Crd.Spec crd, ApiVersion crApiVersion) {
        ObjectNode status;
        int length = Arrays.stream(crd.subresources().status())
                .filter(st -> crApiVersion == null || ApiVersion.parseRange(st.apiVersion()).contains(crApiVersion))
                .collect(Collectors.toList()).size();
        if (length == 1) {
            status = nf.objectNode();
        } else if (length > 1)  {
            throw new RuntimeException("Each custom resource definition can have only one status sub-resource.");
        } else {
            status = null;
        }
        return status;
    }

    private ObjectNode buildScale(Crd.Spec crd, ApiVersion crApiVersion) {
        ObjectNode scaleNode;
        List<Crd.Spec.Subresources.Scale> scale1 = Arrays.stream(crd.subresources().scale())
                .filter(sc -> crApiVersion == null || ApiVersion.parseRange(sc.apiVersion()).contains(crApiVersion))
                .collect(Collectors.toList());
        if (scale1.size() == 1) {
            scaleNode = nf.objectNode();
            Crd.Spec.Subresources.Scale scale = scale1.get(0);

            scaleNode.put("specReplicasPath", scale.specReplicasPath());
            scaleNode.put("statusReplicasPath", scale.statusReplicasPath());

            if (!scale.labelSelectorPath().isEmpty()) {
                scaleNode.put("labelSelectorPath", scale.labelSelectorPath());
            }
        } else if (scale1.size() > 1)  {
            throw new RuntimeException("Each custom resource definition can have only one scale sub-resource.");
        } else {
            scaleNode = null;
        }
        return scaleNode;
    }

    private ArrayNode buildAdditionalPrinterColumns(Crd.Spec crd, ApiVersion crdApiVersion) {
        ArrayNode cols = nf.arrayNode();
        if (crd.additionalPrinterColumns().length != 0) {
            for (Crd.Spec.AdditionalPrinterColumn col : Arrays.stream(crd.additionalPrinterColumns())
                    .filter(col -> crdApiVersion == null || ApiVersion.parseRange(col.apiVersion()).contains(crdApiVersion))
                    .collect(Collectors.toList())) {
                ObjectNode colNode = cols.addObject();
                colNode.put("name", col.name());
                colNode.put("description", col.description());
                colNode.put("JSONPath", col.jsonPath());
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

    private ObjectNode buildValidation(Class<? extends CustomResource> crdClass, ApiVersion crdApiVersion) {
        ObjectNode result = nf.objectNode();
        // OpenShift Origin 3.10-rc0 doesn't like the `type: object` in schema root
        result.set("openAPIV3Schema", buildObjectSchema(crdApiVersion, crdClass, crdClass, false));
        return result;
    }

    private ObjectNode buildObjectSchema(ApiVersion crdApiVersion, AnnotatedElement annotatedElement, Class<?> crdClass) {
        return buildObjectSchema(crdApiVersion, annotatedElement, crdClass, true);
    }

    private ObjectNode buildObjectSchema(ApiVersion crdApiVersion, AnnotatedElement annotatedElement, Class<?> crdClass, boolean printType) {

        ObjectNode result = nf.objectNode();

        buildObjectSchema(crdApiVersion, result, crdClass, printType);
        return result;
    }

    private void buildObjectSchema(ApiVersion crdApiVersion, ObjectNode result, Class<?> crdClass, boolean printType) {
        checkClass(crdClass);
        if (printType) {
            result.put("type", "object");
        }

        result.set("properties", buildSchemaProperties(crdApiVersion, crdClass));
        ArrayNode oneOf = buildSchemaOneOf(crdClass);
        if (oneOf != null) {
            result.set("oneOf", oneOf);
        }
        ArrayNode required = buildSchemaRequired(crdApiVersion, crdClass);
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

    private ObjectNode buildSchemaProperties(ApiVersion crdApiVersion, Class<?> crdClass) {
        ObjectNode properties = nf.objectNode();
        for (Property property : unionOfSubclassProperties(crdApiVersion, crdClass)) {
            if (property.getType().getType().isAnnotationPresent(Alternation.class)) {
                List<Property> alternatives = property.getAlternatives(crdApiVersion);
                if (alternatives.size() == 1) {
                    properties.set(property.getName(), buildSchema(crdApiVersion, alternatives.get(0)));
                } else if (alternatives.size() == 0) {
                    err("Class " + property.getType().getType().getName() + " is annotated with " +
                            "@" + Alternation.class.getSimpleName() + " but has less than two " +
                            "@" + Alternative.class.getSimpleName() + "-annotated properties");
                } else {
                    buildMultiTypeProperty(crdApiVersion, properties, property, alternatives);
                }
            } else {
                buildProperty(crdApiVersion, properties, property);
            }
        }
        return properties;
    }



    private void buildMultiTypeProperty(ApiVersion crdApiVersion, ObjectNode properties, Property property, List<Property> alternatives) {
        ArrayNode oneOfAlternatives = nf.arrayNode(alternatives.size());

        for (Property alternative : alternatives)   {
            oneOfAlternatives.add(buildSchema(crdApiVersion, alternative));
        }

        ObjectNode oneOf = nf.objectNode();
        oneOf.set("oneOf", oneOfAlternatives);
        properties.set(property.getName(), oneOf);
    }

    private void buildProperty(ApiVersion crdApiVersion, ObjectNode properties, Property property) {
        properties.set(property.getName(), buildSchema(crdApiVersion, property));
    }

    private ObjectNode buildSchema(ApiVersion crdApiVersion, Property property) {
        PropertyType propertyType = property.getType();
        Class<?> returnType = propertyType.getType();
        final ObjectNode schema;
        if (propertyType.getGenericType() instanceof ParameterizedType
                && ((ParameterizedType) propertyType.getGenericType()).getRawType().equals(Map.class)
                && ((ParameterizedType) propertyType.getGenericType()).getActualTypeArguments()[0].equals(Integer.class)) {
            System.err.println("It's OK");
            schema = nf.objectNode();
            schema.put("type", "object");
            schema.putObject("patternProperties").set("-?[0-9]+", buildArraySchema(crdApiVersion, property, new PropertyType(null, ((ParameterizedType) propertyType.getGenericType()).getActualTypeArguments()[1])));
        } else if (Schema.isJsonScalarType(returnType)
                || Map.class.equals(returnType)) {            
            schema = addSimpleTypeConstraints(crdApiVersion, buildBasicTypeSchema(property, returnType), property);
        } else if (returnType.isArray() || List.class.equals(returnType)) {
            schema = buildArraySchema(crdApiVersion, property, property.getType());
        } else {
            schema = buildObjectSchema(crdApiVersion, property, returnType);
        }
        addDescription(crdApiVersion, schema, property);
        return schema;
    }

    private ObjectNode buildArraySchema(ApiVersion crdApiVersion, Property property, PropertyType propertyType) {
        int arrayDimension = propertyType.arrayDimension();
        ObjectNode result = nf.objectNode();
        ObjectNode itemResult = result;
        for (int i = 0; i < arrayDimension; i++) {
            itemResult.put("type", "array");
            MinimumItems minimumItems = selectVersion(crdApiVersion, property, MinimumItems.class);
            if (minimumItems != null) {
                result.put("minimum", minimumItems.value());
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
            itemResult.put("type", "object");
        } else  {
            buildObjectSchema(crdApiVersion, itemResult, elementType, true);
        }
        return result;
    }

    private ObjectNode buildBasicTypeSchema(AnnotatedElement element, Class type) {
        ObjectNode result = nf.objectNode();

        String typeName;
        Type typeAnno = element.getAnnotation(Type.class);
        if (typeAnno == null) {
            typeName = typeName(type);
        } else {
            typeName = typeAnno.value();
        }
        result.put("type", typeName);


        return result;
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
        // TODO make support verions
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
        checkDisjointVersions(wrapperAnnotation, cls);
        return Arrays.stream(wrapperAnnotation)
                // TODO crApiVersion == null does not really imply we should return the first description.
                .filter(element1 -> crApiVersion == null || apiVersion(element1, cls).contains(crApiVersion))
                .findFirst().orElse(null);
    }

    @SuppressWarnings("unchecked")
    private static <T> void checkDisjointVersions(T[] wrapperAnnotation, Class<T> annotationClass) {
        long count = Arrays.stream(wrapperAnnotation)
                .map(element -> apiVersion(element, annotationClass)).count();
        long distinctCount = Arrays.stream(wrapperAnnotation)
                .map(element -> apiVersion(element, annotationClass)).distinct().count();
        if (count != distinctCount) {
            throw new RuntimeException("Duplicate version ranges");
        }
        Arrays.stream(wrapperAnnotation)
                .map(element -> apiVersion(element, annotationClass))
                .flatMap(x -> Arrays.stream(wrapperAnnotation)
                        .map(y -> apiVersion(y, annotationClass))
                        .filter(y -> !y.equals(x))
                        .map(y -> new VersionRange[]{x, y}))
                .forEach(pair -> {
                    if (pair[0].intersects(pair[1])) {
                        throw new RuntimeException(pair[0] + " and " + pair[1] + " are not disjoint");
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

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        boolean yaml = false;
        Map<String, String> labels = new LinkedHashMap<>();
        Map<String, Class<? extends CustomResource>> classes = new HashMap<>();
        KubeVersion targetKubeVersion = null;
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                if (arg.equals("--yaml")) {
                    yaml = true;
                } else if (arg.equals("--label")) {
                    i++;
                    int index = args[i].indexOf(":");
                    if (index == -1) {
                        argParseErr("Invalid --label " + args[i]);
                    }
                    labels.put(args[i].substring(0, index), args[i].substring(index + 1));
                } else if (arg.equals("--target-kube")) {
                    if (targetKubeVersion != null) {
                        argParseErr("--target-kube can only be specified once");
                    } else if (i >= arg.length() - 1) {
                        argParseErr("--target-kube needs an argument");
                    }
                    targetKubeVersion = KubeVersion.parse(args[++i]);
                } else {
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
        if (targetKubeVersion == null) {
            targetKubeVersion = KubeVersion.V1_11;
        }

        CrdGenerator generator = new CrdGenerator(targetKubeVersion, yaml ?
                new YAMLMapper().configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, true).configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false) :
                new ObjectMapper(), labels);
        for (Map.Entry<String, Class<? extends CustomResource>> entry : classes.entrySet()) {
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
