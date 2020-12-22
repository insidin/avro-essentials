package io.insidin.avro.essentials.kafkaconnect.serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

//import static io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils.getSchema;

public class StandardKafkaAvroDeserializer implements Deserializer<Object> {

    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private Map<String, org.apache.avro.Schema> schemaMap;

    public StandardKafkaAvroDeserializer(Map<String, Schema> schemaMap) {
        this.schemaMap = schemaMap;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        return deserialize(bytes, getTopicSchema(topic));
    }


    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return deserialize(topic, data);
    }

    @Override
    public void close() {

    }

    private Schema getTopicSchema(String topic) {
        Schema s = schemaMap.get(topic);
        if (s == null) {
            throw new RuntimeException(String.format("No Avro Schema configured for topic %s ", topic));
        }
        return s;
    }

    private GenericContainer deserialize(byte[] bytes, Schema schema) {
        if (bytes == null) {
            return null;
        }
        try {
            DatumReader reader = new GenericDatumReader(schema);
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            BinaryDecoder decoder = this.decoderFactory.directBinaryDecoder(in, (BinaryDecoder) null);
            Object o = null;
            reader.read(o, decoder);
            return (GenericContainer)o;
        }
        catch (RuntimeException | IOException ioe) {
            throw new SerializationException("Error deserializing Avro GenericRecord", ioe);
        }
    }

}
