/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.strimzi.crdgenerator.annotations.Alternation;
import io.strimzi.crdgenerator.annotations.Alternative;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@JsonDeserialize(using = VersionedMapOrList.Deserializer.class)
@JsonSerialize(using = VersionedMapOrList.Serializer.class)
@Alternation
public class VersionedMapOrList {
    private Map<String, String> mapValue;
    private List<String> listValue;

    public VersionedMapOrList(Map<String, String> map)   {
        mapValue = map;
        listValue = null;
    }

    public VersionedMapOrList(List<String> list)   {
        mapValue = null;
        listValue = list;
    }

    @Alternative(apiVersion = "v1")
    public Map<String, String> getMapValue()    {
        return mapValue;
    }

    @Alternative(apiVersion = "v1+")
    public List<String> getListValue()    {
        return listValue;
    }

    public static class Serializer extends JsonSerializer<VersionedMapOrList> {
        @Override
        public void serialize(VersionedMapOrList value, JsonGenerator generator, SerializerProvider provider) throws IOException {
            if (value != null) {
                if (value.listValue != null)    {
                    generator.writeObject(value.listValue);
                } else if (value.mapValue != null)  {
                    generator.writeObject(value.mapValue);
                } else {
                    generator.writeNull();
                }
            } else {
                generator.writeNull();
            }
        }
    }

    public static class Deserializer extends JsonDeserializer<VersionedMapOrList> {
        @Override
        public VersionedMapOrList deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException {
            VersionedMapOrList mapOrList;
            ObjectCodec oc = jsonParser.getCodec();
            if (jsonParser.currentToken() == JsonToken.START_ARRAY) {
                mapOrList = new VersionedMapOrList(oc.readValue(jsonParser, new TypeReference<List<String>>() { }));
            } else if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                mapOrList = new VersionedMapOrList(oc.readValue(jsonParser, new TypeReference<Map<String, String>>() { }));
            } else {
                throw new RuntimeException();
            }
            return mapOrList;
        }
    }
}
