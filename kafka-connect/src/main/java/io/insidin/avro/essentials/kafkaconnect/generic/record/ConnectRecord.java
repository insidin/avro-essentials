package io.insidin.avro.essentials.kafkaconnect.generic.record;

import io.insidin.avro.essentials.generic.record.Record;
import org.apache.kafka.connect.data.Struct;

public class ConnectRecord implements Record<Struct> {

    private Struct record;

    public ConnectRecord(Struct record) {
        this.record = record;
    }

    @Override
    public Object get(String fieldName) {
        return record.get(fieldName);
    }

    @Override
    public Record getRecord(String fieldName) {
        return new ConnectRecord(record.getStruct(fieldName));
    }

}
