/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import org.junit.Test;

import static io.strimzi.crdgenerator.Property.properties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropertyTest {

    private static PropertyType propertyType(Class<?> cls, String propertyNAme) {
        return properties(cls).get(propertyNAme).getType();
    }

    @Test
    public void testIsArray() {

        assertTrue(propertyType(ExampleCrd.class, "arrayProperty").isArray());
        assertTrue(propertyType(ExampleCrd.class, "arrayProperty2").isArray());
        assertTrue(propertyType(ExampleCrd.class, "listOfInts").isArray());
        assertTrue(propertyType(ExampleCrd.class, "listOfInts2").isArray());
        assertTrue(propertyType(ExampleCrd.class, "listOfObjects").isArray());
        assertTrue(propertyType(ExampleCrd.class, "listOfPolymorphic").isArray());
        assertTrue(propertyType(ExampleCrd.class, "rawList").isArray());
        assertTrue(propertyType(ExampleCrd.class, "listOfRawList").isArray());
        assertTrue(propertyType(ExampleCrd.class, "arrayOfList").isArray());
        assertTrue(propertyType(ExampleCrd.class, "arrayOfRawList").isArray());
        assertTrue(propertyType(ExampleCrd.class, "listOfArray").isArray());
    }

    @Test
    public void testArrayDimension() {
        assertEquals(1, propertyType(ExampleCrd.class, "arrayProperty").arrayDimension());
        assertEquals(2, propertyType(ExampleCrd.class, "arrayProperty2").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "listOfInts").arrayDimension());
        assertEquals(2, propertyType(ExampleCrd.class, "listOfInts2").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "listOfObjects").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "listOfPolymorphic").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "rawList").arrayDimension());
        assertEquals(2, propertyType(ExampleCrd.class, "listOfRawList").arrayDimension());
        assertEquals(2, propertyType(ExampleCrd.class, "arrayOfList").arrayDimension());
        assertEquals(2, propertyType(ExampleCrd.class, "arrayOfRawList").arrayDimension());
        assertEquals(2, propertyType(ExampleCrd.class, "listOfArray").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "arrayOfTypeVar").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "listOfTypeVar").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "arrayOfBoundTypeVar").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "listOfBoundTypeVar").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "arrayOfBoundTypeVar2").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "listOfBoundTypeVar2").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "listOfWildcardTypeVar1").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "listOfWildcardTypeVar2").arrayDimension());
        assertEquals(1, propertyType(ExampleCrd.class, "listOfWildcardTypeVar3").arrayDimension());
        assertEquals(2, propertyType(ExampleCrd.class, "listOfWildcardTypeVar4").arrayDimension());
    }

    @Test
    public void testArrayComponentType() {
        assertEquals(String.class, propertyType(ExampleCrd.class, "arrayProperty").arrayBase());
        assertEquals(String.class, propertyType(ExampleCrd.class, "arrayProperty2").arrayBase());
        assertEquals(Integer.class, propertyType(ExampleCrd.class, "listOfInts").arrayBase());
        assertEquals(Integer.class, propertyType(ExampleCrd.class, "listOfInts2").arrayBase());
        assertEquals(ExampleCrd.ObjectProperty.class, propertyType(ExampleCrd.class, "listOfObjects").arrayBase());
        assertEquals(ExampleCrd.PolymorphicTop.class, propertyType(ExampleCrd.class, "listOfPolymorphic").arrayBase());
        assertEquals(Object.class, propertyType(ExampleCrd.class, "rawList").arrayBase());
        assertEquals(Object.class, propertyType(ExampleCrd.class, "listOfRawList").arrayBase());
        assertEquals(String.class, propertyType(ExampleCrd.class, "arrayOfList").arrayBase());
        assertEquals(Object.class, propertyType(ExampleCrd.class, "arrayOfRawList").arrayBase());
        assertEquals(String.class, propertyType(ExampleCrd.class, "listOfArray").arrayBase());
        assertEquals(Object.class, propertyType(ExampleCrd.class, "arrayOfTypeVar").arrayBase());
        assertEquals(Object.class, propertyType(ExampleCrd.class, "listOfTypeVar").arrayBase());
        assertEquals(Number.class, propertyType(ExampleCrd.class, "arrayOfBoundTypeVar").arrayBase());
        assertEquals(Number.class, propertyType(ExampleCrd.class, "listOfBoundTypeVar").arrayBase());
        assertEquals(Number.class, propertyType(ExampleCrd.class, "arrayOfBoundTypeVar2").arrayBase());
        assertEquals(Number.class, propertyType(ExampleCrd.class, "listOfBoundTypeVar2").arrayBase());
        assertEquals(String.class, propertyType(ExampleCrd.class, "listOfWildcardTypeVar1").arrayBase());
        assertEquals(Number.class, propertyType(ExampleCrd.class, "listOfWildcardTypeVar2").arrayBase());
        assertEquals(Number.class, propertyType(ExampleCrd.class, "listOfWildcardTypeVar3").arrayBase());
        assertEquals(Number.class, propertyType(ExampleCrd.class, "listOfWildcardTypeVar4").arrayBase());
    }

}
