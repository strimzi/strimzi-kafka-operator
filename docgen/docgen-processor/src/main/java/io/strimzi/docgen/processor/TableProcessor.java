/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.processor;

import io.strimzi.docgen.annotations.Table;
import io.strimzi.docgen.model.ColumnModel;
import io.strimzi.docgen.model.TableInstanceKey;
import io.strimzi.docgen.model.TableInstanceModel;
import io.strimzi.docgen.model.TableKindModel;
import io.strimzi.docgen.renderer.AsciidocTableRenderer;
import io.strimzi.docgen.renderer.Renderer;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import javax.tools.FileObject;
import javax.tools.StandardLocation;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessor extends AbstractProcessor {

    private Filer filer;
    private Messager messager;
    private final Renderer renderer = new AsciidocTableRenderer();

    public TableProcessor() { }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latestSupported();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        Set<String> annotataions = new LinkedHashSet<String>();
        annotataions.add("*");
        return annotataions;
    }

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        filer = processingEnv.getFiler();
        messager = processingEnv.getMessager();
        messager.printMessage(Diagnostic.Kind.WARNING, "Hello, TableProcessor");
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        messager.printMessage(Diagnostic.Kind.WARNING, "Running, TableProcessor");
        System.err.println("Running, TableProcessor");
        // First get the user annotations
        Set<TableKindModel> tableKinds = userAnnotations(roundEnv);
        System.err.println("Table kinds " + tableKinds);

        // Now find everything annotated with those annotations, and build an instance model
        Map<TableInstanceKey, TableInstanceModel> tableInstances = tableInstances(roundEnv, tableKinds);

        // Generate a table for each of them
        for (Map.Entry<TableInstanceKey, TableInstanceModel> entry: tableInstances.entrySet()) {
            render(entry, renderer);
        }

        return true;
    }

    private void render(Map.Entry<TableInstanceKey, TableInstanceModel> entry, Renderer renderer) {
        try {
            TableInstanceKey key = entry.getKey();
            TableInstanceModel instance = entry.getValue();
            FileObject foo = filer.createResource(StandardLocation.CLASS_OUTPUT, outputPackage(key),
                    outputFilename(renderer, key));
            Writer writer = foo.openWriter();
            renderer.render(instance, writer);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Map<TableInstanceKey, TableInstanceModel> tableInstances(RoundEnvironment roundEnv, Set<TableKindModel> tableKinds) {
        Map<TableInstanceKey, TableInstanceModel> tableInstances = new HashMap<>();
        for (TableKindModel tableKind : tableKinds) {
            for (Element userAnnotated : roundEnv.getElementsAnnotatedWith(tableKind.typeElement())) {
                for (AnnotationMirror mirror : userAnnotated.getAnnotationMirrors()) {
                    if (tableKind.typeElement().equals(mirror.getAnnotationType().asElement())) {
                        // Collect by containing class or some other key
                        TableInstanceKey key = new TableInstanceKey(tableKind, containingType(userAnnotated).getSimpleName().toString());
                        TableInstanceModel instance = tableInstances.get(key);
                        if (instance == null) {
                            instance = new TableInstanceModel(tableKind);
                            tableInstances.put(key, instance);
                        }
                        instance.addRow(toRow(tableKind, mirror));
                    }
                }
            }
        }
        return tableInstances;
    }

    private List<String> toRow(TableKindModel tableKind, AnnotationMirror mirror) {
        // TODO cache this on a per-table kind basis
        Map<? extends ExecutableElement, ? extends AnnotationValue> rowValues = mirror.getElementValues();
        List<String> result = new ArrayList<>(tableKind.columns().size());
        OUTER: for (ColumnModel column : tableKind.columns()) {
            for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> x : rowValues.entrySet()) {
                if (x.getKey().getSimpleName().toString().equals(column.elementName())) {
                    result.add(String.valueOf(x.getValue().getValue()));
                    continue OUTER;
                }
            }
            // default values
            for (Element annotationMember : mirror.getAnnotationType().asElement().getEnclosedElements()) {
                ExecutableElement annotationMethod = (ExecutableElement) annotationMember;
                if (column.elementName().equals(annotationMethod.getSimpleName().toString())) {
                    result.add(String.valueOf(annotationMethod.getDefaultValue().getValue()));
                }
            }
        }
        return result;
    }

    /**
     * Get the package of the output file
     * @param key the key
     */
    private CharSequence outputPackage(TableInstanceKey key) {
        return key.kindModel().typeElement().getSimpleName();
    }

    /**
     * Get the (simple) filename of the output file
     * @param renderer the renderer
     * @param key the key
     */
    private CharSequence outputFilename(Renderer renderer, TableInstanceKey key) {
        return outputPackage(key) + "." + key.key() + "." + renderer.fileFxtension();
    }


    private Set<TableKindModel> userAnnotations(RoundEnvironment roundEnv) {
        Set<TableKindModel> tables = new HashSet<>();

        // TODO which columns to sort the rows on

        // When the user annotation(s) are defined in the same compilation as the user-annotated elements
        // We can just directly find the user annotation(s)
        for (Element tableAnnotated : roundEnv.getElementsAnnotatedWith(Table.class)) {
            //TreeMap<Integer, ColumnModel> columns = new TreeMap<>();
            // Now find all the things annotated with the given annotation
            tables.add(buildTableModel(tableAnnotated));
        }
        // TODO the other case, when the user annotation is not being compiled at the same time
        // Have to walk all declarations, inspecting annotations to find annotations
        // themselves annotated with @Table
        for (Element e : roundEnv.getRootElements()) {
            recurse(e, tables);
        }

        return tables;
    }

    private TableKindModel buildTableModel(Element tableAnnotated) {
        return new TableKindModel((TypeElement) tableAnnotated, buildColumnModels(tableAnnotated));
    }

    private List<ColumnModel> buildColumnModels(Element tableAnnotated) {
        List<ColumnModel> columns = new ArrayList<>();
        for (Element columnAnnotated : tableAnnotated.getEnclosedElements()) {
            Table.Column columnAnnotation = columnAnnotated.getAnnotation(Table.Column.class);
            if (columnAnnotation == null) {
                messager.printMessage(Diagnostic.Kind.NOTE, columnAnnotated
                        .getSimpleName() + " is not annotated with @" + Table.Column.class.getSimpleName(),
                        columnAnnotated);
                continue;
            }
            ColumnModel model = new ColumnModel(columnAnnotated, columnAnnotation);
            columns.add(model);
        }
        return columns;
    }

    private void recurse(Element e, Set<TableKindModel> tables) {
        System.err.println("recurse " + e);
        for (AnnotationMirror mirror : e.getAnnotationMirrors()) {
            Element element = mirror.getAnnotationType().asElement();
            if (element.getAnnotation(Table.class) != null) {
                System.err.println("found " + element);
                tables.add(buildTableModel(element));
            }
        }

        for (Element e2 : e.getEnclosedElements()) {
            recurse(e2, tables);
        }
    }

    private TypeElement containingType(Element userAnnotated) {
        Element e = userAnnotated;
        while (true) {
            Element e2 = e.getEnclosingElement();
            if (e2.getKind() == ElementKind.CLASS
                    || e2.getKind() == ElementKind.INTERFACE
                    || e2.getKind() == ElementKind.ENUM
                    || e2.getKind() == ElementKind.ANNOTATION_TYPE) {
                return (TypeElement) e2;
            }
            e = e2;
        }
    }
}
