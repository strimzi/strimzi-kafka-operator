/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.strimzi.api.annotations.DeprecatedType;
import io.strimzi.crdgenerator.annotations.Alternation;
import io.strimzi.crdgenerator.annotations.Alternative;

import java.io.IOException;

@JsonDeserialize(using = Type1OrType2.Deserializer.class)
@JsonSerialize(using = Type1OrType2.Serializer.class)
@Alternation
public class Type1OrType2 {
    private Type1 type1Value;
    private Type2 type2Value;

    public Type1OrType2(Type1 type1Value)   {
        type1Value = type1Value;
        type2Value = null;
    }

    public Type1OrType2(Type2 type2Value)   {
        type1Value = null;
        type2Value = type2Value;
    }

    @Alternative
    public Type1 getMapValue()    {
        return type1Value;
    }

    @Alternative
    public Type2 getListValue()    {
        return type2Value;
    }

    public static class Serializer extends JsonSerializer<Type1OrType2> {
        @Override
        public void serialize(Type1OrType2 value, JsonGenerator generator, SerializerProvider provider) throws IOException {
            if (value != null) {
                if (value.type1Value != null)    {
                    generator.writeObject(value.type1Value);
                } else if (value.type2Value != null)  {
                    generator.writeObject(value.type2Value);
                } else {
                    generator.writeNull();
                }
            } else {
                generator.writeNull();
            }
        }
    }

    public static class Deserializer extends JsonDeserializer<Type1OrType2> {
        private static ObjectMapper mapper = new ObjectMapper();

        @Override
        public Type1OrType2 deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException {
            ObjectCodec oc = jsonParser.getCodec();
            JsonNode node = oc.readTree(jsonParser);
            Type1OrType2 type1OrType2;

            if (node.isObject() && node.fieldNames().hasNext() && node.fieldNames().next().equals("key1")) {
                ObjectReader reader = mapper.readerFor(new TypeReference<Type1>() { });
                Type1 value = reader.readValue(node);
                type1OrType2 = new Type1OrType2(value);
            } else if (node.isObject() && node.fieldNames().hasNext() && node.fieldNames().next().equals("key2")) {
                ObjectReader reader = mapper.readerFor(new TypeReference<Type2>() { });
                Type2 value = reader.readValue(node);
                type1OrType2 = new Type1OrType2(value);
            } else {
                return null;
            }

            return type1OrType2;
        }
    }

    @Deprecated
    @DeprecatedType(replacedWithType = io.strimzi.crdgenerator.Type1OrType2.Type2.class)
    public class Type1 {
        private String key1;

        public String getKey1() {
            return key1;
        }

        public void setKey1(String key1) {
            this.key1 = key1;
        }
    }

    public class Type2 {
        private String key2;

        public String getKey2() {
            return key2;
        }

        public void setKey2(String key2) {
            this.key2 = key2;
        }
    }
}
