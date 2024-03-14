/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.crdgenerator.annotations.Crd;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.MinimumItems;
import io.strimzi.crdgenerator.annotations.OneOf;

import java.util.List;
import java.util.Map;

@Crd(
    spec = @Crd.Spec(
        group = "crdgenerator.strimzi.io",
        names = @Crd.Spec.Names(
            kind = "Example",
            plural = "examples",
            categories = {"strimzi"}),
        scope = "Namespaced",
        versions = {
            @Crd.Spec.Version(name = "v1alpha1", served = true, storage = true),
            @Crd.Spec.Version(name = "v1beta1", served = true, storage = false)
        },
        additionalPrinterColumns = {
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Foo",
                description = "The foo",
                jsonPath = "...",
                type = "integer"),
            @Crd.Spec.AdditionalPrinterColumn(
                name = "configYaml",
                description = "nest: This is a nested yaml\n" +
                        "lines:\n" +
                        "  - 2nd\n" +
                        "  - 3rd",
                jsonPath = "...",
                type = "string")
        }
    )
)
@OneOf({@OneOf.Alternative(@OneOf.Alternative.Property("either")), @OneOf.Alternative(@OneOf.Alternative.Property("or")), @OneOf.Alternative({@OneOf.Alternative.Property("mapStringString"), @OneOf.Alternative.Property("mapStringObject")})})
public class ExampleCrdWithErrors<T, U extends Number, V extends U> extends CustomResource {

    private String ignored;

    private String string;

    private int intProperty;

    private long longProperty;

    private boolean booleanProperty;

    public NormalEnum normalEnum;

    public CustomisedEnum customisedEnum;

    private ObjectProperty objectProperty;

    private Affinity affinity;

    @Description("Example of field property.")
    public String fieldProperty;

    @MinimumItems(1)
    public String[] arrayProperty;

    public String[][] arrayProperty2;

    public List<Integer> listOfInts;

    public List<List<Integer>> listOfInts2;

    public List<ObjectProperty> listOfObjects;

    public List rawList;

    public List<List> listOfRawList;

    public List<String>[] arrayOfList;

    public List[] arrayOfRawList;

    public List<String[]> listOfArray;

    public T[] arrayOfTypeVar;

    public List<T> listOfTypeVar;

    public U[] arrayOfBoundTypeVar;

    public List<U> listOfBoundTypeVar;

    public V[] arrayOfBoundTypeVar2;

    public List<V> listOfBoundTypeVar2;

    public List<? extends String> listOfWildcardTypeVar1;

    public List<? extends V> listOfWildcardTypeVar2;

    public List<? extends U> listOfWildcardTypeVar3;

    public List<? extends List<? extends U>> listOfWildcardTypeVar4;

    public List<CustomisedEnum> listOfCustomizedEnum;

    public List<NormalEnum> listOfNormalEnum;

    public List<Map<String, Object>> listOfMaps;

    public String either;

    public String or;

    @Description("Example of complex type.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"foo"})
    public static class ObjectProperty {
        private String foo;
        private String bar;

        public String getFoo() {
            return foo;
        }

        public void setFoo(String foo) {
            this.foo = foo;
        }

        public String getBar() {
            return bar;
        }

        public void setBar(String bar) {
            this.bar = bar;
        }
    }
}
