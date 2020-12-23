package io.insidin.avro.essentials.kafkaconnect.generic.schema;

import io.insidin.avro.essentials.generic.schema.Schema;
import io.insidin.avro.essentials.generic.schema.SchemaField;
import org.apache.kafka.connect.data.Field;

public class ConnectSchema implements Schema<org.apache.kafka.connect.data.Schema> {

    private org.apache.kafka.connect.data.Schema schema;

    public ConnectSchema(org.apache.kafka.connect.data.Schema schema) {
        this.schema = schema;
    }

    @Override
    public org.apache.kafka.connect.data.Schema getSchema() {
        return schema;
    }

    @Override
    public SchemaField getField(String fieldName) {
        Field field = schema.field(fieldName);
        return field != null ? new ConnectSchemaField(field) : null;
    }

    @Override
    public Schema getRequiredSchema() {
        if (!isOptional()) {
            return this;
        }
        // todo: clone via schemabuilder
        return this;

    }

    @Override
    public Boolean isOptional() {
        return schema.isOptional();
    }

    @Override
    public Boolean isUnion() {
        return isRecord() && schema.name().equals("io.confluent.connect.avro.Union");
    }

    @Override
    public Boolean isOptionalUnion() {
        return isOptional() && isUnion();
    }

    public Boolean isRecord() {
        return schema.type() == org.apache.kafka.connect.data.Schema.Type.STRUCT;
    }

    @Override
    public Boolean isCollection() {
        org.apache.kafka.connect.data.Schema.Type type = schema.type();
        return org.apache.kafka.connect.data.Schema.Type.ARRAY == type || org.apache.kafka.connect.data.Schema.Type.MAP == type;
    }

    @Override
    public Schema<org.apache.kafka.connect.data.Schema> getCollectionValueSchema() {
        return new ConnectSchema(schema.valueSchema());
    }

    @Override
    public Schema<org.apache.kafka.connect.data.Schema> getUnionValueSchema(int index) {
        return new ConnectSchema(schema.fields().get(index).schema());
    }
}
