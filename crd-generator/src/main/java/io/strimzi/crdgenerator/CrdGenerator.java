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
import io.strimzi.crdgenerator.annotations.OneOf;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.strimzi.crdgenerator.annotations.Type;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.StandardCharsets;
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

    CrdGenerator(ObjectMapper mapper) {
        this(mapper, emptyMap());
    }

    CrdGenerator(ObjectMapper mapper, Map<String, String> labels) {
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

            node.set("spec", buildSpec(crd.spec(), crdClass));
        }
        mapper.writeValue(out, node);
    }

    private ObjectNode buildSpec(Crd.Spec crd, Class<? extends CustomResource> crdClass) {
        ObjectNode result = nf.objectNode();
        result.put("group", crd.group());
        if (crd.versions().length != 0) {
            ArrayNode versions = nf.arrayNode();
            for (Crd.Spec.Version version : crd.versions()) {
                ObjectNode versionNode = versions.addObject();
                versionNode.put("name", version.name());
                versionNode.put("served", version.served());
                versionNode.put("storage", version.storage());
            }
            result.set("versions", versions);
        }

        if (!crd.version().isEmpty()) {
            result.put("version", crd.version());
        }
        result.put("scope", crd.scope());
        result.set("names", buildNames(crd.names()));
        if (crd.additionalPrinterColumns().length != 0) {
            ArrayNode cols = nf.arrayNode();
            for (Crd.Spec.AdditionalPrinterColumn col : crd.additionalPrinterColumns()) {
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
            result.set("additionalPrinterColumns", cols);
        }
        if (crd.subresources().status().length != 0) {
            ObjectNode subresources = nf.objectNode();

            if (crd.subresources().status().length == 1) {
                subresources.set("status", nf.objectNode());
            } else if (crd.subresources().status().length > 1)  {
                throw new RuntimeException("Each custom resource definition can have only one status sub-resource.");
            }

            if (crd.subresources().scale().length == 1) {
                Crd.Spec.Subresources.Scale scale = crd.subresources().scale()[0];

                ObjectNode scaleNode = nf.objectNode();
                scaleNode.put("specReplicasPath", scale.specReplicasPath());
                scaleNode.put("statusReplicasPath", scale.statusReplicasPath());

                if (!scale.labelSelectorPath().isEmpty()) {
                    scaleNode.put("labelSelectorPath", scale.labelSelectorPath());
                }

                subresources.set("scale", scaleNode);
            } else if (crd.subresources().scale().length > 1)  {
                throw new RuntimeException("Each custom resource definition can have only one scale sub-resource.");
            }

            result.set("subresources", subresources);
        }
        result.set("validation", buildValidation(crdClass));
        return result;
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

    private ObjectNode buildValidation(Class<? extends CustomResource> crdClass) {
        ObjectNode result = nf.objectNode();
        // OpenShift Origin 3.10-rc0 doesn't like the `type: object` in schema root
        result.set("openAPIV3Schema", buildObjectSchema(crdClass, crdClass, false));
        return result;
    }

    private ObjectNode buildObjectSchema(AnnotatedElement annotatedElement, Class<?> crdClass) {
        return buildObjectSchema(annotatedElement, crdClass, true);
    }

    private ObjectNode buildObjectSchema(AnnotatedElement annotatedElement, Class<?> crdClass, boolean printType) {

        ObjectNode result = nf.objectNode();

        buildObjectSchema(result, crdClass, printType);
        return result;
    }

    private void buildObjectSchema(ObjectNode result, Class<?> crdClass, boolean printType) {
        checkClass(crdClass);
        if (printType) {
            result.put("type", "object");
        }

        result.set("properties", buildSchemaProperties(crdClass));
        ArrayNode oneOf = buildSchemaOneOf(crdClass);
        if (oneOf != null) {
            result.set("oneOf", oneOf);
        }
        ArrayNode required = buildSchemaRequired(crdClass);
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

    private Collection<Property> unionOfSubclassProperties(Class<?> crdClass) {
        TreeMap<String, Property> result = new TreeMap<>();
        for (Class subtype : Property.subtypes(crdClass)) {
            result.putAll(properties(subtype));
        }
        result.putAll(properties(crdClass));
        JsonPropertyOrder order = crdClass.getAnnotation(JsonPropertyOrder.class);
        return sortedProperties(order != null ? order.value() : null, result).values();
    }

    private ArrayNode buildSchemaRequired(Class<?> crdClass) {
        ArrayNode result = nf.arrayNode();

        for (Property property : unionOfSubclassProperties(crdClass)) {
            if (property.isAnnotationPresent(JsonProperty.class)
                    && property.getAnnotation(JsonProperty.class).required()
                || property.isDiscriminator()) {
                result.add(property.getName());
            }
        }
        return result;
    }

    private ObjectNode buildSchemaProperties(Class<?> crdClass) {
        ObjectNode properties = nf.objectNode();
        for (Property property : unionOfSubclassProperties(crdClass)) {
            if (property.getType().getType().isAnnotationPresent(Alternation.class)) {
                List<Property> alternatives = property.getAlternatives();
                if (alternatives.size() < 2) {
                    err("Class " + property.getType().getType().getName() + " is annotated with " +
                            "@" + Alternation.class.getSimpleName() + " but has less than two " +
                            "@" + Alternative.class.getSimpleName() + "-annotated properties");
                }
                buildMultiTypeProperty(properties, property, alternatives);
            } else {
                buildProperty(properties, property);
            }
        }
        return properties;
    }



    private void buildMultiTypeProperty(ObjectNode properties, Property property, List<Property> alternatives) {
        ArrayNode oneOfAlternatives = nf.arrayNode(alternatives.size());

        for (Property alternative : alternatives)   {
            oneOfAlternatives.add(buildSchema(alternative));
        }

        ObjectNode oneOf = nf.objectNode();
        oneOf.set("oneOf", oneOfAlternatives);
        properties.set(property.getName(), oneOf);
    }

    private void buildProperty(ObjectNode properties, Property property) {
        properties.set(property.getName(), buildSchema(property));
    }

    private ObjectNode buildSchema(Property property) {
        PropertyType propertyType = property.getType();
        Class<?> returnType = propertyType.getType();
        final ObjectNode schema;
        if (propertyType.getGenericType() instanceof ParameterizedType
                && ((ParameterizedType) propertyType.getGenericType()).getRawType().equals(Map.class)
                && ((ParameterizedType) propertyType.getGenericType()).getActualTypeArguments()[0].equals(Integer.class)) {
            System.err.println("It's OK");
            schema = nf.objectNode();
            schema.put("type", "object");
            schema.putObject("patternProperties").set("-?[0-9]+", buildArraySchema(new PropertyType(null, ((ParameterizedType) propertyType.getGenericType()).getActualTypeArguments()[1])));
        } else if (Schema.isJsonScalarType(returnType)
                || Map.class.equals(returnType)) {            
            schema = addSimpleTypeConstraints(buildBasicTypeSchema(property, returnType), property);
        } else if (returnType.isArray() || List.class.equals(returnType)) {
            schema = buildArraySchema(property.getType());
        } else {
            schema = buildObjectSchema(property, returnType);
        }
        addDescription(schema, property);
        return schema;
    }

    private ObjectNode buildArraySchema(PropertyType propertyType) {
        int arrayDimension = propertyType.arrayDimension();
        ObjectNode result = nf.objectNode();
        ObjectNode itemResult = result;
        for (int i = 0; i < arrayDimension; i++) {
            itemResult.put("type", "array");
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
            buildObjectSchema(itemResult, elementType, true);
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

    private void addDescription(ObjectNode result, AnnotatedElement element) {
        if (element.isAnnotationPresent(Description.class)) {
            Description description = element.getAnnotation(Description.class);
            result.put("description", DocGenerator.getDescription(description));
        }
    }

    @SuppressWarnings("unchecked")
    private ObjectNode addSimpleTypeConstraints(ObjectNode result, Property property) {

        Example example = property.getAnnotation(Example.class);
        if (example != null) {
            result.put("example", example.value());
        }

        Minimum minimum = property.getAnnotation(Minimum.class);
        if (minimum != null) {
            result.put("minimum", minimum.value());
        }
        Maximum maximum = property.getAnnotation(Maximum.class);
        if (maximum != null) {
            result.put("maximum", maximum.value());
        }
        Pattern pattern = property.getAnnotation(Pattern.class);
        if (pattern != null) {
            result.put("pattern", pattern.value());
        }

        if (property.getType().isEnum()) {
            result.set("enum", enumCaseArray(property.getType().getEnumElements()));
        }

        if (property.getDeclaringClass().isAnnotationPresent(JsonTypeInfo.class)
            && property.getName().equals(property.getDeclaringClass().getAnnotation(JsonTypeInfo.class).property())) {
            result.set("enum", stringArray(Property.subtypeNames(property.getDeclaringClass())));
        }

        Deprecated deprecated = property.getAnnotation(Deprecated.class);
        if (deprecated == null) {
            deprecated = property.getType().getType().getAnnotation(Deprecated.class);
        }
        if (deprecated != null) {
            result.put("deprecated", true);
        }

        return result;
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

        CrdGenerator generator = new CrdGenerator(yaml ?
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
