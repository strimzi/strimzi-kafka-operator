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

@JsonDeserialize(using = MapOrList.Deserializer.class)
@JsonSerialize(using = MapOrList.Serializer.class)
@Alternation
public class MapOrList {
    private Map<String, String> mapValue;
    private List<String> listValue;

    public MapOrList(Map<String, String> map)   {
        mapValue = map;
        listValue = null;
    }

    public MapOrList(List<String> list)   {
        mapValue = null;
        listValue = list;
    }

    @Alternative
    public Map<String, String> getMapValue()    {
        return mapValue;
    }

    @Alternative
    public List<String> getListValue()    {
        return listValue;
    }

    public static class Serializer extends JsonSerializer<MapOrList> {
        @Override
        public void serialize(MapOrList value, JsonGenerator generator, SerializerProvider provider) throws IOException {
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

    public static class Deserializer extends JsonDeserializer<MapOrList> {
        @Override
        public MapOrList deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException {
            MapOrList mapOrList;
            ObjectCodec oc = jsonParser.getCodec();
            if (jsonParser.currentToken() == JsonToken.START_ARRAY) {
                mapOrList = new MapOrList(oc.readValue(jsonParser, new TypeReference<List<String>>() { }));
            } else if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                mapOrList = new MapOrList(oc.readValue(jsonParser, new TypeReference<Map<String, String>>() { }));
            } else {
                throw new RuntimeException();
            }
            return mapOrList;
        }
    }
}
