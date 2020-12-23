package io.insidin.avro.essentials.kafkaconnect.smt;

import io.insidin.avro.essentials.kafkaconnect.generic.schema.ConnectSchema;
import io.insidin.avro.essentials.kafkaconnect.generic.record.ConnectRecord;
import io.insidin.avro.essentials.expr.AvroFieldExpression;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Map a nested field to a top-level field, optionally specifying a different target fieldname.
 * Note: for now, no expressions inside collections (map, array) are supported.
 *
 * @param <R>
 */
public abstract class MapRecordField <R extends org.apache.kafka.connect.connector.ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Map a nested field to an additional field at the root record";

    private interface ConfigName {
        String MAP_FIELD_EXPRESSION = "map.field.expression";
        String MAP_FIELD_TARGET = "map.field.target";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.MAP_FIELD_EXPRESSION, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Field expression")
            .define(ConfigName.MAP_FIELD_TARGET, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Field target name");

    private AvroFieldExpression fieldExpression;
    private String targetFieldName;

    private Cache<org.apache.kafka.connect.data.Schema, org.apache.kafka.connect.data.Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldExpression = AvroFieldExpression.parse(config.getString(ConfigName.MAP_FIELD_EXPRESSION));
        targetFieldName = config.getString(ConfigName.MAP_FIELD_TARGET);
        if (targetFieldName == null) {
            targetFieldName = fieldExpression.getFieldName();
        }
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            throw new DataException("MapRecordField requires an Avro record with schema");
        }
        return applyWithSchema(record);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), "mapping a record field");
        org.apache.kafka.connect.data.Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            ConnectSchema fieldSchema = fieldExpression.apply(new ConnectSchema(value.schema()));
            updatedSchema = makeUpdatedSchema(value.schema(), fieldSchema.getSchema(), targetFieldName);
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }
        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        updatedValue.put(targetFieldName, fieldExpression.apply(new ConnectRecord(value)));
        return newRecord(record, updatedSchema, updatedValue);
    }

    private org.apache.kafka.connect.data.Schema makeUpdatedSchema(org.apache.kafka.connect.data.Schema schema, org.apache.kafka.connect.data.Schema fieldSchema, String fieldName) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field: schema.fields()) {
            builder.field(field.name(), field.schema());
        }
        builder.field(fieldName, fieldSchema);
        return builder.build();
    }

    protected abstract org.apache.kafka.connect.data.Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, org.apache.kafka.connect.data.Schema updatedSchema, Object updatedValue);

    public static class Key<R extends org.apache.kafka.connect.connector.ConnectRecord<R>> extends MapRecordField<R> {

        @Override
        protected org.apache.kafka.connect.data.Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, org.apache.kafka.connect.data.Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends org.apache.kafka.connect.connector.ConnectRecord<R>> extends MapRecordField<R> {

        @Override
        protected org.apache.kafka.connect.data.Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, org.apache.kafka.connect.data.Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}
