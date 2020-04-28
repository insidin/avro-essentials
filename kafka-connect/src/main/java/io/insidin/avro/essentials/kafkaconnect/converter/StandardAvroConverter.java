package io.insidin.avro.essentials.kafkaconnect.converter;

import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;


public class StandardAvroConverter implements Converter {

    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private AvroData avroData ;

    public void configure(Map<String, ?> config, boolean b) {
        avroData = new AvroData(100);
    }

    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            org.apache.avro.Schema avroSchema = this.avroData.fromConnectSchema(schema);
            Object o = this.avroData.fromConnectData(schema, value);
            if (o == null) {
                return null;
            }
            return serialize(o, avroSchema);
        }
        catch (SerializationException e) {
            throw new DataException(String.format("Failed to serialize Avro data from topic %s :", topic), e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] bytes) {
        try {
            GenericContainer container = deserialize(bytes);
            if (container == null) {
                return SchemaAndValue.NULL;
            }
            if (container instanceof IndexedRecord) {
                return avroData.toConnectData(container.getSchema(), container);
            }
            else if (container instanceof NonRecordContainer) {
                return avroData.toConnectData(container.getSchema(), ((NonRecordContainer) container).getValue());
            }
            throw new DataException(String.format("Unsupported type returned during deserialization of topic %s ", topic)
            );
        }
        catch (SerializationException e) {
            throw new DataException(String.format("Failed to deserialize data for topic %s to Avro: ", topic), e);
        }
    }

    private byte[] serialize(Object object, org.apache.avro.Schema schema) {
        Object value = object instanceof NonRecordContainer ? ((NonRecordContainer)object).getValue() : object;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            if (schema.getType().equals(org.apache.avro.Schema.Type.BYTES)) {
                if (value instanceof byte[]) {
                    out.write(((byte[]) value));
                } else {
                    if (!(value instanceof ByteBuffer)) {
                        throw new SerializationException("Unrecognized bytes object of type: " + value.getClass().getName());
                    }
                    out.write(((ByteBuffer) value).array());
                }
            }
            else {
                BinaryEncoder encoder = this.encoderFactory.directBinaryEncoder(out, (BinaryEncoder) null);
                Object writer;
                if (value instanceof SpecificRecord) {
                    writer = new SpecificDatumWriter(schema);
                } else {
                    writer = new GenericDatumWriter(schema);
                }
                ((DatumWriter) writer).write(value, encoder);
                encoder.flush();
            }
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        }
        catch (RuntimeException | IOException e) {
            throw new SerializationException("Error serializing Avro message", e);
        }
    }

    private GenericContainer deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            DatumReader reader = new GenericDatumReader();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            BinaryDecoder decoder = this.decoderFactory.directBinaryDecoder(in, (BinaryDecoder) null);
            Object o = null;
            reader.read(o, decoder);
            return (GenericContainer)o;
        } catch (RuntimeException | IOException ioe) {
            throw new SerializationException("Error deserializing Avro GenericRecord", ioe);
        }
    }


}
