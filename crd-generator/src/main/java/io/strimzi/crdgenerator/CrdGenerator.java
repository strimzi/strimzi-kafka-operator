/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Example;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.strimzi.crdgenerator.annotations.Pattern;
import io.strimzi.crdgenerator.annotations.Type;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static io.strimzi.crdgenerator.Property.properties;
import static io.strimzi.crdgenerator.Property.sortedProperties;
import static java.util.Arrays.asList;

/**
 * <p>Generates a Kubernetes {@code CustomResourceDefinition} YAML file
 * from an annotated Java model (POJOs).
 * The tool supports Jackson annotations and a few custom annotations in order
 * to generate a high-quality schema that's compatible with
 * K8S CRD validation schema support, which is more limited that the full OpenAPI schema
 * supported in the rest of K8S.</p>
 *
 * <p>The tool works be recursing through class properties (in the JavaBeans sense)
 * and the types of those properties, guided by the annotations.</p>
 *
 * <h3>Annotations</h3>
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
 * <h3>Polymorphism</h3>
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

    private static void warn(String s) {
        System.err.println("CrdGenerator: warn: " + s);
    }

    private static void err(String s) {
        System.err.println("CrdGenerator: error: " + s);
    }

    private final ObjectMapper mapper;
    private final JsonNodeFactory nf;

    CrdGenerator(ObjectMapper mapper) {
        this.mapper = mapper;
        this.nf = mapper.getNodeFactory();
    }

    void generate(Class<? extends CustomResource> crdClass, Writer out) throws IOException {
        ObjectNode node = nf.objectNode();
        Crd crd = crdClass.getAnnotation(Crd.class);
        if (crd == null) {
            err(crdClass + " is not annotated with @Crd");
        } else {
            String apiVersion = crd.apiVersion();
            if (!apiVersion.startsWith("apiextensions.k8s.io")) {
                warn("@Crd.apiVersion the the API version of the CustomResourceDefinition," +
                        "not the version of instances of the custom resource. " +
                        "It should almost certainly be apiextensions.k8s.io/${some-version}.");
            }
            node.put("apiVersion", apiVersion)
                    .put("kind", "CustomResourceDefinition")
                    .putObject("metadata")
                    .put("name", crd.spec().names().plural() + "." + crd.spec().group());
            node.set("spec", buildSpec(crd.spec(), crdClass));
        }
        mapper.writeValue(out, node);
    }

    private ObjectNode buildSpec(Crd.Spec crd, Class<? extends CustomResource> crdClass) {
        ObjectNode result = nf.objectNode();
        result.put("group", crd.group());
        result.put("version", crd.version());
        result.put("scope", crd.scope());
        result.set("names", buildNames(crd.names()));
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
        return result;
    }

    private ObjectNode buildValidation(Class<? extends CustomResource> crdClass) {
        ObjectNode result = nf.objectNode();
        result.set("openAPIV3Schema", buildObjectSchema(crdClass, crdClass));
        return result;
    }

    private ObjectNode buildObjectSchema(AnnotatedElement annotatedElement, Class<?> crdClass) {
        ObjectNode result = nf.objectNode();
        addDescription(result, annotatedElement);
        result.put("type", "object");
        result.set("properties", buildSchemaProperties(crdClass));
        ArrayNode required = buildSchemaRequired(crdClass);
        if (required.size() > 0) {
            result.set("required", required);
        }
        return result;
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
            buildProperty(properties, property);
        }
        return properties;
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
                || int.class.equals(elementType)) {
            itemResult.put("type", "integer");
        } else  {
            itemResult.put("type", "object");
            itemResult.set("properties", buildSchemaProperties(elementType));
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

    private ObjectNode addSimpleTypeConstraints(ObjectNode result, Property property) {

        addDescription(result, property);

        Example example = property.getAnnotation(Example.class);
        if (example != null) {
            result.put("example", example.value());
        }

        Minimum minimum = property.getAnnotation(Minimum.class);
        if (minimum != null) {
            result.put("minimum", minimum.value());
        }
        Pattern pattern = property.getAnnotation(Pattern.class);
        if (pattern != null) {
            result.put("pattern", pattern.value());
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

    private void addDescription(ObjectNode result, AnnotatedElement element) {
        Description description = element.getAnnotation(Description.class);
        if (description != null) {
            result.put("description", description.value());
        } else {
            warn("Missing @Description on " + element);
        }
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

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        boolean yaml = false;
        Map<String, Class<? extends CustomResource>> classes = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("--")) {
                if (arg.equals("--yaml")) {
                    yaml = true;
                } else {
                    throw new RuntimeException("Unsupported command line option " + arg);
                }
            } else {
                String className = arg.substring(0, arg.indexOf('='));
                String fileName = arg.substring(arg.indexOf('=') + 1);
                Class<?> cls = Class.forName(className);
                if (!CustomResource.class.equals(cls)
                        && CustomResource.class.isAssignableFrom(cls)) {
                    classes.put(fileName, (Class<? extends CustomResource>) cls);
                } else {
                    err(cls + " is not a subclass of " + CustomResource.class.getName());
                }
            }
        }
        CrdGenerator generator = new CrdGenerator(yaml ?
                new YAMLMapper().configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, true) :
                new ObjectMapper());
        for (Map.Entry<String, Class<? extends CustomResource>> entry : classes.entrySet()) {
            File file = new File(entry.getKey());
            if (file.getParentFile().exists()
                    || !file.getParentFile().mkdirs()) {
                err(file.getParentFile() + " does not exist and could not be created");
            }
            try (Writer w = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8)) {
                generator.generate(entry.getValue(), w);
            }
        }
    }
}
