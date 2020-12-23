package io.insidin.avro.essentials.kafkaconnect.generic.schema;

import io.insidin.avro.essentials.generic.schema.Schema;
import io.insidin.avro.essentials.generic.schema.SchemaField;
import org.apache.kafka.connect.data.Field;

public class ConnectSchemaField implements SchemaField {

    private Field field;

    public ConnectSchemaField(Field field) {
        this.field = field;
    }

    @Override
    public Schema getSchema() {
        return new ConnectSchema(field.schema());
    }
}
