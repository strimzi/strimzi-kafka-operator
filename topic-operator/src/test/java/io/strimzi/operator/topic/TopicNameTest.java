/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TopicNameTest {

    private void checkMappedName(String name, String expect) {
        assertEquals(expect, new TopicName(name).asKubeName().toString());
    }

    @Test
    public void testAsMapName() {
        checkMappedName("foo", "foo");
        checkMappedName("Foo", "foo---201a6b3053cc1422d2c3670b62616221d2290929");
        checkMappedName("FOO", "foo---feab40e1fca77c7360ccca1481bb8ba5f919ce3a");
        checkMappedName("foo.bar", "foo.bar");
        checkMappedName("foo.BAR", "foo.bar---2b539a9cb49ce7280d7ebfb93b08540a6535ba35");
        checkMappedName("foo-bar.baz", "foo-bar.baz");
        checkMappedName("__foo", "foo---117282ae03235561f215ee101800b2d5b3609f7");
        checkMappedName("foo__", "foo---47318fcc6fa7f277565de829dbd878e03530e078");
        checkMappedName("foo__bar", "foo-bar---423126006221ca9092992ce14121ee0a3eaed346");
        checkMappedName("foo_bar", "foo-bar---5d5f20e74771b852025c3edb66c9462eeb913d03");
        checkMappedName("--foo", "foo---5a5af07607cb6064c0183648a704188a457b1eb6");
        checkMappedName("foo--", "foo---b16c222dbf36b52253b205d0132eab9d7d879907");
        checkMappedName("foo--bar", "foo--bar");
        checkMappedName("..foo", "foo---cab6ff58f3d59a9f69aaa8c19603f7ce9bb34056");
        checkMappedName("foo..bar", "foo.bar---4683128189ed6ca9d78cb034fa97f327e46d7465");
        checkMappedName("foo..", "foo---ab2e4e62f3b8c288495eb2616ee2cd678f83a63f");
        checkMappedName("foo.-bar", "foo.bar---4f4325245861ad879d835e7fca95145c2f8c5eb8");
        checkMappedName("foo-.bar", "foo-bar---6ba1b74e7c34254171fabf7cdfbba9936356e940");
        checkMappedName("-", "3bc15c8aae3e4124dd409035f32ea2fd6835efc9");
        checkMappedName("--", "e6a9fc04320a924f46c7c737432bb0389d9dd095");
        checkMappedName("...", "6eae3a5b062c6d0d79f070c26e6d62486b40cb46");
        checkMappedName("-.", "d6aee8987a388b8b4c590e42011c8a18286f7ca8");
        checkMappedName(".-", "5166aaeb81d80edb1b34f9550e3c5cdf4edfe8a7");
        checkMappedName("012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678", "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678");
        checkMappedName("01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567-", "012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789---5d35cf300f0748c74f224496d9973d9657c36b57");
    }
}
