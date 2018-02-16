/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Meta-annotation for annotating annotation types that should be processed into documentation.</p>
 *
 * <p>To generate documentation, an annotation type needs to be declared and annotated with {@code @Table}.
 * The {@code @interface}'s members are annotated with {@code @Table.Column}. This defines a logical table
 * whose columns are the members. The order of the columns is the declaration order of the annotation type's
 * members. The annotation type can have as many {@code @Table.Column}-annotated members as desired.</p>
 *
 * <p>For example, consider the need to document the environment variables that a Java program might use.
 * We first declare an annotation like this:</p>
 * <pre><code>
 * {@literal @}Table
 * {@literal @}Retention(RetentionPolicy.SOURCE)
 * {@literal @}Target({ElementType.FIELD})
 * {@literal @}Documented
 *  public{@literal @}interface EnvVar {
 *      {@literal @}Table.Column(heading = "Variable Name")
 *       String name();
 *
 *      {@literal @}Table.Column(heading = "Description")
 *       String doc();
 *
 *      {@literal @}Table.Column(heading = "Mandatory")
 *       boolean required() default true;
 * }
 * </code></pre>
 *
 * <p>(The {@code @Retention} can be {@code SOURCE} because this annotation only needs to be processed at
 * compile time and need not be retained in the compiled {@code .class} files.)</p>
 *
 * <p>Then you can annotate program elements in your program using your annotation:</p>
 *
 * <pre><code>
 *     class HelloWorld {
 *        {@literal @}EnvVar(name="USERNAME", doc="Who to greet")
 *         private static final String ENV_VAR_USERNAME = "USERNAME"
 *         public static void main(String[] args) {
 *             System.out.println("Hello, " + System.getenv(ENV_VAR_USERNAME));
 *         }
 *     }
 * </code></pre>
 *
 * <p>All the {@code @EnvVar} annotations in your class are processed into documentation
 * by the annotation processor. In the above example, the table would have a single row
 * with three columns, like this:</p>
 *
 * <table>
 *     <tbody>
 *         <tr>
 *             <th>Variable Name</th>
 *             <th>Description</th>
 *             <th>Mandatory</th>
 *         </tr>
 *         <tr>
 *             <td>USERNAME</td>
 *             <td>Who to greet</td>
 *             <td>true</td>
 *         </tr>
 *     </tbody>
 * </table>
 */
@Target(ElementType.ANNOTATION_TYPE)
@Retention(RetentionPolicy.CLASS)
public @interface Table {

    /**
     * A column in a table of documentation.
     */
    @Target(ElementType.METHOD)
    public @interface Column {
        /** The header title for this column in the table. */
        String heading();
    }

}
