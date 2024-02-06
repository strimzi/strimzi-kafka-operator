/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.databind.JsonNode;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.annotations.DeprecatedType;
import io.strimzi.crdgenerator.annotations.AddedIn;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.KubeLink;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.crdgenerator.Property.discriminator;
import static io.strimzi.crdgenerator.Property.isPolymorphic;
import static io.strimzi.crdgenerator.Property.properties;
import static io.strimzi.crdgenerator.Property.subtypeMap;
import static io.strimzi.crdgenerator.Property.subtypeNames;
import static io.strimzi.crdgenerator.Property.subtypes;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;

class DocGenerator {

    private static final String NL = System.lineSeparator();
    private static final Pattern DOT_AT_THE_END = Pattern.compile(".*[.!?]$", Pattern.DOTALL);

    private final int headerDepth;
    private final Appendable out;
    private final ApiVersion crApiVersion;

    private final Set<Class<?>> documentedTypes = new HashSet<>();
    private final HashMap<Class<?>, Set<Class<?>>> usedIn;
    private final Linker linker;
    private int numErrors = 0;

    public DocGenerator(ApiVersion crApiVersion, int headerDepth, Iterable<Class<? extends CustomResource>> crdClasses, Appendable out, Linker linker) {
        this.crApiVersion = crApiVersion;
        this.out = out;
        this.headerDepth = headerDepth;
        this.linker = linker;
        this.usedIn = new HashMap<>();
        for (Class<? extends CustomResource> crdClass: crdClasses) {
            usedIn(crdClass, usedIn);
        }
    }

    private void appendAnchor(Class<?> anchor) throws IOException {
        out.append("[id='").append(anchor(anchor)).append("']").append(NL);
    }

    private String anchor(Class<?> anchor) {
        return "type-" + anchor.getSimpleName() + "-{context}";
    }

    private void appendHeading(String name) throws IOException {
        appendRepeated('=', headerDepth);
        out.append(' ');
        out.append(name);
        out.append(" schema reference");

        out.append(NL);
        out.append(NL);
    }

    private void usedIn(Class<?> cls, Map<Class<?>, Set<Class<?>>> usedIn) {
        Set<Property> memorableProperties = new HashSet<>();

        for (Property property : properties(crApiVersion, cls).values()) {
            if (property.isAnnotationPresent(KubeLink.class)) {
                continue;
            }

            memorableProperties.add(property);
        }

        for (Property property : memorableProperties) {
            PropertyType propertyType = property.getType();
            Class<?> type = propertyType.isArray() ? propertyType.arrayBase() : propertyType.getType();
            for (Class<?> c : subtypesOrSelf(type)) {
                if (Schema.isJsonScalarType(c)) {
                    continue;
                }

                Set<Class<?>> classes = getOrCreateClassesSet(c, usedIn);

                classes.add(cls);

                usedIn(c, usedIn);
            }
        }
    }

    private Set<Class<?>> getOrCreateClassesSet(Class<?> c, Map<Class<?>, Set<Class<?>>> usedIn)   {
        return usedIn.computeIfAbsent(c, cls -> new HashSet<>(1));
    }

    private List<? extends Class<?>> subtypesOrSelf(Class<?> returnType) {
        return isPolymorphic(returnType) ? subtypes(returnType) : singletonList(returnType);
    }

    public void generate(Class<? extends CustomResource> crdClass) throws IOException {
        Crd crd = crdClass.getAnnotation(Crd.class);
        appendAnchor(crdClass);
        appendHeading("`" + crd.spec().names().kind() + "`");
        appendCommonTypeDoc(crd, crdClass);
    }

    private void appendedNestedTypeDoc(Crd crd, Class<?> cls) throws IOException {
        appendAnchor(cls);
        appendHeading("`" + cls.getSimpleName() + "`");
        appendCommonTypeDoc(crd, cls);
    }

    private void appendCommonTypeDoc(Crd crd, Class<?> cls) throws IOException {
        appendTypeDeprecation(crd, cls);
        appendUsedIn(crd, cls);
        appendDescription(cls);
        appendDiscriminator(crd, cls);
    
        out.append("[cols=\"2,2,3a\",options=\"header\"]").append(NL);
        out.append("|====").append(NL);
        out.append("|Property |Property type |Description").append(NL);
    
        LinkedHashSet<Class<?>> types = new LinkedHashSet<>();
    
        for (Map.Entry<String, Property> entry : properties(crApiVersion, cls).entrySet()) {
            String propertyName = entry.getKey();
            final Property property = entry.getValue();
            PropertyType propertyType = property.getType();
        
            // Set the external link to Kubernetes docs or the link for fields distinguished by `type`
            KubeLink kubeLink = property.getAnnotation(KubeLink.class);
            String externalUrl = linker != null && kubeLink != null ? linker.link(kubeLink) : null;
        
            // Add the property name
            out.append("|").append(propertyName);
        
            // Add the property type description
            appendPropertyType(crd, out, propertyType, externalUrl);
        
            // Set warning message for deprecated fields
            addDeprecationWarning(property);
        
            // Add the version the field was added in
            addAddedIn(property);
        
            // Add the types to the `types` array to also generate the docs for the type itself
            Class<?> documentedType = propertyType.isArray() ? propertyType.arrayBase() : propertyType.getType();
        
            if (externalUrl == null
                && !Schema.isJsonScalarType(documentedType)
                && !documentedType.equals(Map.class)
                && !documentedType.equals(Object.class)) {
                types.add(documentedType);
            }
        
            // Add the property description
            addDescription(cls, property);
        
            out.append(NL);
        }
        
        out.append("|====").append(NL).append(NL);
    
        appendNestedTypes(crd, types);
    }    

    /**
     * Sets warning message for deprecated fields
     *
     * @param property  The property which will be checked for deprecation
     *
     * @throws IOException  Throws IOException when appending to the output fails
     */
    private void addDeprecationWarning(Property property) throws IOException {
        DeprecatedProperty strimziDeprecated = property.getAnnotation(DeprecatedProperty.class);
        Deprecated langDeprecated = property.getAnnotation(Deprecated.class);

        if (strimziDeprecated != null || langDeprecated != null) {
            if (strimziDeprecated == null || langDeprecated == null) {
                err(property + " must be annotated with both @" + Deprecated.class.getName()
                        + " and @" + DeprecatedProperty.class.getName());
            }
            if (strimziDeprecated != null) {
                out.append(getDeprecation(property, strimziDeprecated));
            }
        }
    }

    /**
     * Sets the description for given property
     *
     * @param cls   The Class (type) which is being documented
     * @param property  The property for which the description should be added
     *
     * @throws IOException  Throws IOException when appending to the output fails
     */
    private void addDescription(Class<?> cls, Property property) throws IOException {
        Description description = property.getAnnotation(Description.class);

        if (description == null) {
            if (cls.getName().startsWith("io.strimzi")) {
                err(property + " is not documented");
            }
        } else {
            out.append(getDescription(description));
        }
    }

    /**
     * Sets the version in which the property was added. It is done only for properties that have the AddedIn annotation.
     *
     * @param property  The property for which the version should be added
     *
     * @throws IOException  Throws IOException when appending to the output fails
     */
    private void addAddedIn(Property property) throws IOException {
        AddedIn addedIn = property.getAnnotation(AddedIn.class);

        if (addedIn != null) {
            out.append("Added in Strimzi " + addedIn.value() + ". ");
        }
    }

    /**
     * Sets the external link to Kubernetes docs or the link for fields distinguished by `type`
     *
     * @param property  The property for which the description should be added
     * @param kubeLink  The value of the KubeLink annotation or null if not set
     * @param externalUrl   The URL to the Kubernetes documentation
     *
     * @throws IOException  Throws IOException when appending to the output fails
     */
    private void addExternalUrl(Property property, KubeLink kubeLink, String externalUrl) throws IOException {
        if (externalUrl != null) {
            out.append(" For more information, see the ").append(externalUrl)
                    .append("[").append("external documentation for ").append(kubeLink.group()).append("/").append(kubeLink.version()).append(" ").append(kubeLink.kind()).append("].").append(NL).append(NL);
        } else if (isPolymorphic(property.getType().getType())) {
            out.append(" The type depends on the value of the `").append(property.getName()).append(".").append(discriminator(property.getType().getType()))
                    .append("` property within the given object, which must be one of ")
                    .append(subtypeNames(property.getType().getType()).toString()).append(".");
        }
    }

    private String getDeprecation(Property property, DeprecatedProperty deprecated) {
        String msg = String.format("**The `%s` property has been deprecated",
                property.getName());
        if (!deprecated.movedToPath().isEmpty()) {
            msg += ", and should now be configured using `" + deprecated.movedToPath() + "`";
        }
        if (!deprecated.removalVersion().isEmpty()) {
            msg += ". The property " + property.getName() + " is removed in API version `" + deprecated.removalVersion() + "`";
        }
        msg += ".** ";
        if (!deprecated.description().isEmpty()) {
            msg += deprecated.description() + " ";
        }

        return msg;
    }

    static String getDescription(Description description) {
        String doc = description.value();
        if (!DOT_AT_THE_END.matcher(doc.trim()).matches())  {
            doc = doc + ".";
        }
        doc = doc.replaceAll("[|]", "\\\\|");
        return doc;
    }

    private void err(String s) {
        System.err.println(DocGenerator.class.getSimpleName() + ": error: " + s);
        numErrors++;
    }

    private void appendNestedTypes(Crd crd, LinkedHashSet<Class<?>> types) throws IOException {
        for (Class<?> type : types) {
            for (Class<?> t2 : subtypesOrSelf(type)) {
                if (!documentedTypes.contains(t2)) {
                    appendedNestedTypeDoc(crd, t2);
                    this.documentedTypes.add(t2);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void appendPropertyType(Crd crd, Appendable out, PropertyType propertyType, String externalUrl) throws IOException {
        Class<?> propertyClass = propertyType.isArray() ? propertyType.arrayBase() : propertyType.getType();
        
        
        out.append(NL);
        out.append("|");
        
        // Now the type link
        if (externalUrl != null) {
            out.append(externalUrl).append("[").append(propertyClass.getSimpleName()).append("]");
        } else if (propertyType.isEnum()) {
            Set<String> strings = new HashSet<>();
            for (JsonNode n : Schema.enumCases(propertyType.getEnumElements())) {
                if (n.isTextual()) {
                    strings.add(n.asText());
                } else {
                    throw new RuntimeException("Enum case is not a string");
                }
            }
            out.append("string (one of " + strings + ")");
        } else if (propertyType.isArray() && propertyType.arrayBase().isEnum()) {
            try {
                Set<String> strings = new HashSet<>();
                Method valuesMethod = propertyType.arrayBase().getMethod("values");

                for (JsonNode n : Schema.enumCases((Enum[]) valuesMethod.invoke(null))) {
                    if (n.isTextual()) {
                        strings.add(n.asText());
                    } else {
                        throw new RuntimeException("Enum case is not a string");
                    }
                }
                out.append("string (one or more of " + strings + ")");
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        } else {
            typeLink(crd, out, propertyClass);
        }
        if (propertyType.isArray()) {
            out.append(" array");
            int dim = propertyType.arrayDimension();
            if (dim > 1) {
                out.append(" of dimension ").append(String.valueOf(dim));
            }
        }

        out.append(NL);
        out.append("|");

    }

    private void appendTypeDeprecation(Crd crd, Class<?> cls) throws IOException {
        DeprecatedType deprecatedType = cls.getAnnotation(DeprecatedType.class);
        Deprecated langDeprecated = cls.getAnnotation(Deprecated.class);

        if (deprecatedType != null || langDeprecated != null) {
            if (deprecatedType == null || langDeprecated == null) {
                err(cls.getName() + " must be annotated with both @" + Deprecated.class.getName()
                        + " and @" + DeprecatedType.class.getName());
            }
            if (deprecatedType != null
                    && deprecatedType.replacedWithType() != null) {
                Class<?> replacementClss = deprecatedType.replacedWithType();

                out.append("*The type `" + cls.getSimpleName() + "` has been deprecated");
                if (!deprecatedType.removalVersion().isEmpty()) {
                    out.append(" and is removed in API version `" + deprecatedType.removalVersion() + "`");
                }
                out.append(".*").append(NL);

                if (replacementClss != void.class) {
                    out.append("Please use ");
                    typeLink(crd, out, replacementClss);
                    out.append(" instead.").append(NL);
                }

                out.append(NL);
            }
        }
    }

    private void appendDescription(Class<?> cls) throws IOException {
        DescriptionFile descriptionFile = cls.getAnnotation(DescriptionFile.class);
        Description description = cls.getAnnotation(Description.class);

        if (descriptionFile != null)    {
            String filename = "api/" + cls.getCanonicalName() + ".adoc";
            File includeFile = new File(filename);

            if (!includeFile.isFile())   {
                throw new RuntimeException("Class " + cls.getCanonicalName() + " has @DescribeFile annotation, but file " + filename + " does not exist!");
            }

            out.append("xref:type-").append(cls.getSimpleName()).append("-schema-{context}[Full list of `").append(cls.getSimpleName()).append("` schema properties]").append(NL);
            out.append(NL);
            out.append("include::../" + filename + "[leveloffset=+1]").append(NL);
            out.append(NL);
            out.append("[id='type-").append(cls.getSimpleName()).append("-schema-{context}']").append(NL);
            out.append("== `").append(cls.getSimpleName()).append("` schema properties").append(NL);
            out.append(NL);

        } else if (description != null) {
            out.append(getDescription(description)).append(NL);
        }
        out.append(NL);
    }

    private void appendUsedIn(Crd crd, Class<?> cls) throws IOException {
        List<Class<?>> usedIn = new ArrayList<>(this.usedIn.getOrDefault(cls, emptySet()));
        usedIn.sort(Comparator.comparing(c -> c.getSimpleName().toLowerCase(Locale.ENGLISH)));
        if (!usedIn.isEmpty()) {
            out.append("Used in: ");
            boolean first = true;
            for (Class<?> usingClass : usedIn) {
                if (!first) {
                    out.append(", ");
                }
                typeLink(crd, out, usingClass);
                first = false;
            }
            out.append(NL);
            out.append(NL);
        }
    }

    private void appendDiscriminator(Crd crd, Class<?> cls) throws IOException {
        String discriminator = discriminator(cls.getSuperclass());
        if (discriminator != null) {
            String subtypeLinks = subtypes(cls.getSuperclass()).stream().filter(c -> !c.equals(cls)).map(c -> {
                try {
                    return typeLink(crd, c);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.joining(", "));
            out.append("The `").append(discriminator)
                    .append("` property is a discriminator that distinguishes use of the `")
                    .append(cls.getSimpleName()).append("` type from ");
            if (subtypeLinks.trim().isEmpty()) {
                out.append("other subtypes which may be added in the future.");
            } else {
                out.append(subtypeLinks).append(".");
            }
            out.append(NL);
            out.append("It must have the value `").append(subtypeMap(cls.getSuperclass()).get(cls)).append("` for the type `").append(cls.getSimpleName()).append("`.").append(NL);
        }
    }

    private void appendRepeated(char c, int num) throws IOException {
        for (int i = 0; i < num; i++) {
            out.append(c);
        }
    }

    public void typeLink(Crd crd, Appendable out, Class<?> cls) throws IOException {
        if (short.class.equals(cls)
                || Short.class.equals(cls)
                || int.class.equals(cls)
                || Integer.class.equals(cls)
                || long.class.equals(cls)
                || Long.class.equals(cls)) {
            out.append("integer");
        } else if (Float.class.equals(cls)
                || float.class.equals(cls)
                || Double.class.equals(cls)
                || double.class.equals(cls)) {
            out.append("number");
        } else if (Object.class.equals(cls)
                || String.class.equals(cls)
                || Map.class.equals(cls)
                || Boolean.class.equals(cls)
                || boolean.class.equals(cls)) {
            out.append(cls.getSimpleName().toLowerCase(Locale.ENGLISH));
        } else if (isPolymorphic(cls)) {
            out.append(subtypes(cls).stream().map(c -> {
                try {
                    return typeLink(crd, c);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.joining(", ")));
        } else {
            out.append("xref:").append(anchor(cls)).append("[`").append(cls.getSimpleName()).append("`]");
        }
    }

    public String typeLink(Crd crd, Class<?> cls) throws IOException {
        StringBuilder sb = new StringBuilder();
        typeLink(crd, sb, cls);
        return sb.toString();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Linker linker = null;
        File out = null;
        List<Class<? extends CustomResource>> classes = new ArrayList<>();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("-")) {
                if ("--linker".equals(arg)) {
                    String className = args[++i];
                    Class<? extends Linker> linkerClass = classInherits(Class.forName(className), Linker.class);
                    if (linkerClass != null) {
                        try {
                            linker = linkerClass.getConstructor(String.class).newInstance(args[++i]);
                        } catch (ReflectiveOperationException e) {
                            throw new RuntimeException("--linker option can't be handled", e);
                        }
                    } else {
                        System.err.println(className + " is not a subclass of " + Linker.class.getName());
                    }
                } else {
                    throw new RuntimeException("Unsupported option " + arg);
                }
            } else {
                if (out == null) {
                    out = new File(arg);
                } else {
                    // If we have fallen in the else branch here, the arg has to be a class name
                    Class<? extends CustomResource> cls = classInherits(Class.forName(arg), CustomResource.class);
                    if (cls != null) {
                        classes.add(cls);
                    } else {
                        System.err.println(arg + " is not a subclass of " + CustomResource.class.getName());
                    }
                }
            }
        }

        ApiVersion crApiVersion = ApiVersion.V1BETA2;

        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(out), StandardCharsets.UTF_8)) {
            writer.append("// This file is auto-generated by ")
                .append(DocGenerator.class.getName())
                .append(".").append(NL);
            writer.append("// To change this documentation you need to edit the Java sources.").append(NL);
            writer.append(NL);
            DocGenerator dg = new DocGenerator(crApiVersion, 1, classes, writer, linker);
            for (Class<? extends CustomResource> c : classes) {
                dg.generate(c);
            }
            if (dg.numErrors > 0) {
                System.err.println("There were " + dg.numErrors + " errors");
                System.exit(1);
            }
        }
    }

    @SuppressWarnings("unchecked")
    static <T> Class<? extends T> classInherits(Class<?> cls, Class<T> test) {
        if (test.isAssignableFrom(cls)) {
            return (Class<? extends T>) cls;
        }
        return null;
    }
}
