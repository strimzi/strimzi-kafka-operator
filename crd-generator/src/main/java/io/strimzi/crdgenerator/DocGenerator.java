/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.KubeLink;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.strimzi.crdgenerator.Property.discriminator;
import static io.strimzi.crdgenerator.Property.isPolymorphic;
import static io.strimzi.crdgenerator.Property.properties;
import static io.strimzi.crdgenerator.Property.subtypeMap;
import static io.strimzi.crdgenerator.Property.subtypeNames;
import static io.strimzi.crdgenerator.Property.subtypes;
import static java.lang.Integer.max;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;

public class DocGenerator {

    private static final String NL = System.lineSeparator();

    private final int headerDepth;
    private final Appendable out;

    private Set<Class<?>> documentedTypes = new HashSet<>();
    private HashMap<Class<?>, Set<Class<?>>> usedIn;
    private Linker linker;

    public DocGenerator(int headerDepth, Iterable<Class<? extends CustomResource>> crdClasses, Appendable out, Linker linker) {
        this.out = out;
        this.headerDepth = headerDepth;
        this.linker = linker;
        this.usedIn = new HashMap<>();
        for (Class<? extends CustomResource> crdClass: crdClasses) {
            usedIn(crdClass, usedIn);
        }
    }

    private void appendAnchor(Crd crd) throws IOException {
        out.append("[[").append(anchor(crd)).append("]]").append(NL);
    }

    private String anchor(Crd crd) {
        return "kind-" + crd.spec().names().kind();
    }

    private void appendAnchor(Crd crd, Class<?> anchor) throws IOException {
        out.append("[[").append(anchor(anchor)).append("]]").append(NL);
    }

    private String anchor(Class<?> anchor) {
        return "type-" + anchor.getSimpleName();
    }

    private void appendHeading(Crd crd, String name) throws IOException {
        appendRepeated('#', headerDepth);
        out.append(' ');
        out.append(name);
        out.append(' ');
        out.append(crd.spec().version());
        out.append(' ');
        out.append(crd.spec().group());
        out.append(NL);
        out.append(NL);
    }

    private void usedIn(Class<?> cls, Map<Class<?>, Set<Class<?>>> usedIn) {
        for (Property property : properties(cls).values()) {
            if (property.isAnnotationPresent(KubeLink.class)) {
                continue;
            }
            PropertyType propertyType = property.getType();
            Class<?> type = propertyType.isArray() ? propertyType.arrayBase() : propertyType.getType();
            for (Class<?> c : subtypesOrSelf(type)) {
                if (Schema.isJsonScalarType(c)) {
                    continue;
                }
                Set<Class<?>> classes = usedIn.get(c);
                if (classes == null) {
                    classes = new HashSet<>(2);
                    usedIn.put(c, classes);
                }
                classes.add(cls);
                usedIn(c, usedIn);
            }
        }
    }

    private List<? extends Class<?>> subtypesOrSelf(Class<?> returnType) {
        return isPolymorphic(returnType) ? subtypes(returnType) : singletonList(returnType);
    }

    public void generate(Class<? extends CustomResource> crdClass) throws IOException {
        Crd crd = crdClass.getAnnotation(Crd.class);
        appendAnchor(crd);
        appendAnchor(crd, crdClass);
        appendHeading(crd, "`" + crd.spec().names().kind() + "` kind");
        appendCommonTypeDoc(crd, crdClass);
    }

    private void appendedNestedTypeDoc(Crd crd, Class<?> cls) throws IOException {
        appendAnchor(crd, cls);
        appendHeading(crd, "`" + cls.getSimpleName() + "` type");
        appendCommonTypeDoc(crd, cls);
    }

    private void appendCommonTypeDoc(Crd crd, Class<?> cls) throws IOException {
        appendUsedIn(crd, cls);
        appendDescription(cls);
        appendDiscriminator(crd, cls);

        out.append("[options=\"header\"]").append(NL);
        out.append("|====").append(NL);
        out.append("|Field");
        String gunk = "1.2+<.<";
        final Map<String, Property> properties = properties(cls);
        int maxLen = computePadding(gunk, properties);
        appendRepeated(' ', maxLen - "Field".length() + 1);
        out.append("|Description");
        out.append(NL);

        LinkedHashSet<Class<?>> types = new LinkedHashSet<>();

        for (Map.Entry<String, Property> entry: properties.entrySet()) {
            String propertyName = entry.getKey();
            final Property property = entry.getValue();
            PropertyType propertyType = property.getType();
            Class<?> propertyClass = propertyType.getType();
            out.append("|").append(propertyName);
            appendRepeated(' ', maxLen - propertyName.length() - gunk.length());
            out.append(' ');
            out.append(gunk);
            out.append("|");

            Description description2 = property.getAnnotation(Description.class);
            if (description2 != null) {
                out.append(description2.value());
            }
            KubeLink kubeLink = property.getAnnotation(KubeLink.class);
            String externalUrl = linker != null && kubeLink != null ? linker.link(kubeLink) : null;
            if (externalUrl != null) {
                out.append("See external documentation of ").append(externalUrl)
                        .append("[").append(kubeLink.group()).append("/").append(kubeLink.version()).append(" ").append(kubeLink.kind()).append("]").append(NL).append(NL);
            } else if (isPolymorphic(propertyClass)) {
                out.append(" The type depends on the value of the `" + propertyName + "." + discriminator(propertyClass) + "` property within the given object, " +
                        "which must be one of " + subtypeNames(propertyClass));
            }

            Class<?> documentedType = propertyType.isArray() ? propertyType.arrayBase() : propertyClass;
            if (externalUrl == null
                    && !Schema.isJsonScalarType(documentedType)
                    && !documentedType.equals(Map.class)
                    && !documentedType.equals(Object.class)) {
                types.add(documentedType);
            }

            // TODO Deprecated, Minimum?, Pattern?
            appendPropertyType(crd, propertyType, externalUrl);
        }
        out.append("|====").append(NL).append(NL);

        appendNestedTypes(crd, types);
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

    private int computePadding(String gunk, Map<String, Property> properties) {
        int maxLen = 0;
        for (Map.Entry<String, Property> entry: properties.entrySet()) {
            maxLen = max(maxLen, entry.getKey().length() + 1 + gunk.length());
        }
        return maxLen;
    }

    private void appendPropertyType(Crd crd, PropertyType propertyType, String externalUrl) throws IOException {
        Class<?> propertyClass = propertyType.isArray() ? propertyType.arrayBase() : propertyType.getType();
        out.append(NL).append("|");
        // Now the type link
        if (externalUrl != null) {
            out.append(externalUrl).append("[").append(propertyClass.getSimpleName()).append("]");
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
    }

    private void appendDescription(Class<?> cls) throws IOException {
        Description description = cls.getAnnotation(Description.class);
        if (description != null) {
            out.append(description.value()).append(NL);
        }
        out.append(NL);
    }

    private void appendUsedIn(Crd crd, Class<?> cls) throws IOException {
        List<Class<?>> usedIn = this.usedIn.getOrDefault(cls, emptySet()).stream().collect(Collectors.toCollection(() -> new ArrayList<>()));
        Collections.sort(usedIn, Comparator.comparing(c -> c.getSimpleName().toLowerCase(Locale.ENGLISH)));
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
            out.append("The `").append(discriminator)
                    .append("` property is a discriminator that distinguishes the use of the type `")
                    .append(cls.getSimpleName()).append("` from ")
                    .append(subtypes(cls.getSuperclass()).stream().filter(c -> !c.equals(cls)).map(c -> {
                        try {
                            return typeLink(crd, c);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).collect(Collectors.joining(", ")))
                    .append(".")
                    .append(NL);
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
        } else if (cls.isAnnotationPresent(Crd.class)) {
            // <<KafkaType,`KafkaType`>>
            out.append("<<").append(anchor(crd)).append(",`").append(cls.getSimpleName()).append("`>>");
        } else {
            out.append("<<").append(anchor(cls)).append(",`").append(cls.getSimpleName()).append("`>>");
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
        outer: for (int i = 0; i < args.length; i++) {
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
                    String className = arg;
                    Class<? extends CustomResource> cls = classInherits(Class.forName(className), CustomResource.class);
                    if (cls != null) {
                        classes.add(cls);
                    } else {
                        System.err.println(className + " is not a subclass of " + CustomResource.class.getName());
                    }
                }
            }
        }

        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(out), StandardCharsets.UTF_8)) {
            DocGenerator dg = new DocGenerator(3, classes, writer, linker);
            for (Class<? extends CustomResource> c : classes) {
                dg.generate(c);
            }
        }
    }

    static <T> Class<? extends T> classInherits(Class<?> cls, Class<T> test) {
        if (test.isAssignableFrom(cls)) {
            return (Class<? extends T>) cls;
        }
        return null;
    }
}
