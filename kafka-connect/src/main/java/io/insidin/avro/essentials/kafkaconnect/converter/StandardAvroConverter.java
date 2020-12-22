package io.insidin.avro.essentials.kafkaconnect.converter;

import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import io.insidin.avro.essentials.kafkaconnect.serializer.StandardKafkaAvroDeserializer;
import io.insidin.avro.essentials.kafkaconnect.serializer.StandardKafkaAvroSerializer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.HashMap;
import java.util.Map;


public class StandardAvroConverter implements Converter {

    private StandardKafkaAvroSerializer serializer;
    private StandardKafkaAvroDeserializer deserializer;

    private AvroData avroData ;

    public void configure(Map<String, ?> configs, boolean b) {
        this.avroData = new AvroData(100);
        Map<String, org.apache.avro.Schema> m = (Map<String, org.apache.avro.Schema>) configs.get("schemas");
        this.serializer = new StandardKafkaAvroSerializer();
        this.deserializer = new StandardKafkaAvroDeserializer(m != null ? m : new HashMap<>());

    }

    public byte[] fromConnectData(String topic, Schema schema, Object object) {
        try {
            org.apache.avro.Schema avroSchema = this.avroData.fromConnectSchema(schema);
            Object o = this.avroData.fromConnectData(schema, object);
            if (o == null) {
                return null;
            }
            return serializer.serialize(o, avroSchema);
        }
        catch (SerializationException e) {
            throw new DataException(String.format("Failed to serialize Avro data from topic %s :", topic), e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] bytes) {
        try {
            Object container = deserializer.deserialize(topic, bytes);
            if (container == null) {
                return SchemaAndValue.NULL;
            }
            if (container instanceof IndexedRecord) {
                return avroData.toConnectData(((IndexedRecord)container).getSchema(), container);
            }
            else if (container instanceof NonRecordContainer) {
                return avroData.toConnectData(((NonRecordContainer)container).getSchema(), ((NonRecordContainer) container).getValue());
            }
            throw new DataException(String.format("Unsupported type returned during deserialization of topic %s ", topic));
        }
        catch (SerializationException e) {
            throw new DataException(String.format("Failed to deserialize data for topic %s to Avro: ", topic), e);
        }
    }


}
