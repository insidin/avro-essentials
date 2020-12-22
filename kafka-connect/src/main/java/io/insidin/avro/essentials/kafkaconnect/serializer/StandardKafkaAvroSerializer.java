package io.insidin.avro.essentials.kafkaconnect.serializer;

import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class StandardKafkaAvroSerializer implements Serializer {

    private final EncoderFactory encoderFactory = EncoderFactory.get();

    @Override
    public void configure(Map configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Object object) {
        return serialize(object, getSchema(object));
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Object object) {
        return serialize(topic, object);
    }

    @Override
    public void close() {

    }

    public byte[] serialize(Object object, Schema schema) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            if (schema.getType().equals(org.apache.avro.Schema.Type.BYTES)) {
                if (object instanceof byte[]) {
                    out.write(((byte[]) object));
                } else {
                    if (!(object instanceof ByteBuffer)) {
                        throw new SerializationException("Unrecognized bytes object of type: " + object.getClass().getName());
                    }
                    out.write(((ByteBuffer) object).array());
                }
            }
            else {
                BinaryEncoder encoder = this.encoderFactory.directBinaryEncoder(out, (BinaryEncoder) null);
                Object writer;
                if (object instanceof SpecificRecord) {
                    writer = new SpecificDatumWriter(schema);
                } else {
                    writer = new GenericDatumWriter(schema);
                }
                ((DatumWriter) writer).write(object, encoder);
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

    /////// Heavily inspired by https://github.com/confluentinc/schema-registry/blob/master/client/src/main/java/io/confluent/kafka/schemaregistry/avro/AvroSchemaUtils.java

    private static final Map<String, Schema> primitiveSchemas;

    static {
        primitiveSchemas = new HashMap();
        primitiveSchemas.put("Null", Schema.create(Schema.Type.NULL));
        primitiveSchemas.put("Boolean", Schema.create(Schema.Type.BOOLEAN));
        primitiveSchemas.put("Integer", Schema.create(Schema.Type.INT));
        primitiveSchemas.put("Long", Schema.create(Schema.Type.LONG));
        primitiveSchemas.put("Float", Schema.create(Schema.Type.FLOAT));
        primitiveSchemas.put("Double", Schema.create(Schema.Type.DOUBLE));
        primitiveSchemas.put("String", Schema.create(Schema.Type.STRING));
        primitiveSchemas.put("Bytes", Schema.create(Schema.Type.BYTES));
    }

    public static Schema getSchema(Object object) {
        return getSchema(object, false);
    }

    public static Schema getSchema(Object object, boolean useReflection) {
        if (object == null) {
            return (Schema)primitiveSchemas.get("Null");
        } else if (object instanceof Boolean) {
            return (Schema)primitiveSchemas.get("Boolean");
        } else if (object instanceof Integer) {
            return (Schema)primitiveSchemas.get("Integer");
        } else if (object instanceof Long) {
            return (Schema)primitiveSchemas.get("Long");
        } else if (object instanceof Float) {
            return (Schema)primitiveSchemas.get("Float");
        } else if (object instanceof Double) {
            return (Schema)primitiveSchemas.get("Double");
        } else if (object instanceof CharSequence) {
            return (Schema)primitiveSchemas.get("String");
        } else if (!(object instanceof byte[]) && !(object instanceof ByteBuffer)) {
            if (useReflection) {
                Schema schema = ReflectData.get().getSchema(object.getClass());
                if (schema == null) {
                    throw new SerializationException("Schema is null for object of class " + object.getClass().getCanonicalName());
                } else {
                    return schema;
                }
            } else if (object instanceof GenericContainer) {
                return ((GenericContainer)object).getSchema();
            } else if (object instanceof Map) {
                Map mapValue = (Map)object;
                if (mapValue.isEmpty()) {
                    return Schema.createMap((Schema)primitiveSchemas.get("Null"));
                } else {
                    Schema valueSchema = getSchema(mapValue.values().iterator().next());
                    return Schema.createMap(valueSchema);
                }
            } else {
                throw new IllegalArgumentException("Unsupported Avro type. Supported types are null, Boolean, Integer, Long, Float, Double, String, byte[] and IndexedRecord");
            }
        } else {
            return (Schema)primitiveSchemas.get("Bytes");
        }
    }


}
