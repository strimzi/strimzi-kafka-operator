/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.strimzi.crdgenerator.Property.properties;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class PropertyTest {

    private static PropertyType propertyType(Class<?> cls, String propertyNAme) {
        return properties(null, cls).get(propertyNAme).getType();
    }

    @Test
    public void testIsArray() {

        assertThat(propertyType(ExampleCrd.class, "arrayProperty").isArray(), is(true));
        assertThat(propertyType(ExampleCrd.class, "arrayProperty2").isArray(), is(true));
        assertThat(propertyType(ExampleCrd.class, "listOfInts").isArray(), is(true));
        assertThat(propertyType(ExampleCrd.class, "listOfInts2").isArray(), is(true));
        assertThat(propertyType(ExampleCrd.class, "listOfObjects").isArray(), is(true));
        assertThat(propertyType(ExampleCrd.class, "listOfPolymorphic").isArray(), is(true));
        assertThat(propertyType(ExampleCrd.class, "rawList").isArray(), is(true));
        assertThat(propertyType(ExampleCrd.class, "listOfRawList").isArray(), is(true));
        assertThat(propertyType(ExampleCrd.class, "arrayOfList").isArray(), is(true));
        assertThat(propertyType(ExampleCrd.class, "arrayOfRawList").isArray(), is(true));
        assertThat(propertyType(ExampleCrd.class, "listOfArray").isArray(), is(true));
    }

    @Test
    public void testArrayDimension() {
        assertThat(propertyType(ExampleCrd.class, "arrayProperty").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "arrayProperty2").arrayDimension(), is(2));
        assertThat(propertyType(ExampleCrd.class, "listOfInts").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "listOfInts2").arrayDimension(), is(2));
        assertThat(propertyType(ExampleCrd.class, "listOfObjects").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "listOfPolymorphic").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "rawList").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "listOfRawList").arrayDimension(), is(2));
        assertThat(propertyType(ExampleCrd.class, "arrayOfList").arrayDimension(), is(2));
        assertThat(propertyType(ExampleCrd.class, "arrayOfRawList").arrayDimension(), is(2));
        assertThat(propertyType(ExampleCrd.class, "listOfArray").arrayDimension(), is(2));
        assertThat(propertyType(ExampleCrd.class, "arrayOfTypeVar").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "listOfTypeVar").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "arrayOfBoundTypeVar").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "listOfBoundTypeVar").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "arrayOfBoundTypeVar2").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "listOfBoundTypeVar2").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "listOfWildcardTypeVar1").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "listOfWildcardTypeVar2").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "listOfWildcardTypeVar3").arrayDimension(), is(1));
        assertThat(propertyType(ExampleCrd.class, "listOfWildcardTypeVar4").arrayDimension(), is(2));
        assertThat(propertyType(ExampleCrd.class, "listOfMaps").arrayDimension(), is(1));
    }

    @Test
    public void testArrayComponentType() {
        assertThat(propertyType(ExampleCrd.class, "arrayProperty").arrayBase().getName(), is(String.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "arrayProperty2").arrayBase().getName(), is(String.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfInts").arrayBase().getName(), is(Integer.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfInts2").arrayBase().getName(), is(Integer.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfObjects").arrayBase().getName(), is(ExampleCrd.ObjectProperty.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfPolymorphic").arrayBase().getName(), is(ExampleCrd.PolymorphicTop.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "rawList").arrayBase().getName(), is(Object.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfRawList").arrayBase().getName(), is(Object.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "arrayOfList").arrayBase().getName(), is(String.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "arrayOfRawList").arrayBase().getName(), is(Object.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfArray").arrayBase().getName(), is(String.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "arrayOfTypeVar").arrayBase().getName(), is(Object.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfTypeVar").arrayBase().getName(), is(Object.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "arrayOfBoundTypeVar").arrayBase().getName(), is(Number.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfBoundTypeVar").arrayBase().getName(), is(Number.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "arrayOfBoundTypeVar2").arrayBase().getName(), is(Number.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfBoundTypeVar2").arrayBase().getName(), is(Number.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfWildcardTypeVar1").arrayBase().getName(), is(String.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfWildcardTypeVar2").arrayBase().getName(), is(Number.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfWildcardTypeVar3").arrayBase().getName(), is(Number.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfWildcardTypeVar4").arrayBase().getName(), is(Number.class.getName()));
        assertThat(propertyType(ExampleCrd.class, "listOfMaps").arrayBase().getName(), is(Map.class.getName()));
    }

}
