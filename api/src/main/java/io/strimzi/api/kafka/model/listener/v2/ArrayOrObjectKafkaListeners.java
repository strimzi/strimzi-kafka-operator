/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener.v2;

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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.api.kafka.model.Constants;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.crdgenerator.annotations.Alternation;
import io.strimzi.crdgenerator.annotations.Alternative;
import io.sundr.builder.annotations.Buildable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonDeserialize(using = ArrayOrObjectKafkaListeners.Deserializer.class)
@JsonSerialize(using = ArrayOrObjectKafkaListeners.Serializer.class)
@Alternation
public class ArrayOrObjectKafkaListeners implements Serializable {
    private static final long serialVersionUID = 1L;

    private final List<GenericKafkaListener> listValue;
    private final KafkaListeners objectValue;

    public ArrayOrObjectKafkaListeners(List<GenericKafkaListener> listValue)   {
        this.listValue = listValue;
        this.objectValue = null;
    }

    public ArrayOrObjectKafkaListeners(KafkaListeners objectValue)   {
        this.listValue = null;
        this.objectValue = objectValue;
    }

    @Alternative()
    public List<GenericKafkaListener> getListValue() {
        return listValue;
    }

    @Alternative()
    public KafkaListeners getObjectValue() {
        return objectValue;
    }

    /**
     * Convenience method which returns either the new listener format if set, or converted old format.
     *
     * @return  List of new listeners
     */
    public List<GenericKafkaListener> newOrConverted()  {
        if (listValue != null)  {
            return listValue;
        } else {
            return ListenersConvertor.convertToNewFormat(objectValue);
        }
    }

    public static class Serializer extends JsonSerializer<ArrayOrObjectKafkaListeners> {
        @Override
        public void serialize(ArrayOrObjectKafkaListeners value, JsonGenerator generator, SerializerProvider provider) throws IOException {
            if (value == null)  {
                generator.writeNull();
                return;
            }

            if (value.listValue != null)    {
                generator.writeObject(value.listValue);
            } else if (value.objectValue != null)  {
                generator.writeObject(value.objectValue);
            } else {
                generator.writeNull();
            }
        }
    }

    @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
    public static class Deserializer extends JsonDeserializer<ArrayOrObjectKafkaListeners> {
        @Override
        public ArrayOrObjectKafkaListeners deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException {
            ObjectCodec oc = jsonParser.getCodec();

            if (jsonParser.currentToken() == JsonToken.START_ARRAY) {
                return new ArrayOrObjectKafkaListeners(oc.readValue(jsonParser, new TypeReference<List<GenericKafkaListener>>() { }));
            } else if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                return new ArrayOrObjectKafkaListeners(oc.readValue(jsonParser, new TypeReference<KafkaListeners>() { }));
            } else {
                throw new RuntimeException("Failed to deserialize ArrayOrObjectKafkaListeners. Please check .spec.kafka.listeners configuration.");
            }
        }
    }
}
