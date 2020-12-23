package io.insidin.avro.essentials.kafkaconnect.smt;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import org.apache.kafka.common.record.RecordsUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class MapRecordFieldTest {

    private MapRecordField<SourceRecord> mrfSmt = new MapRecordField.Value<>();

    @AfterEach
    public void tearDown() throws Exception {
        mrfSmt.close();
    }

    @Test
    public void topLevelStructRequired() {
        mrfSmt.configure(Collections.singletonMap("map.field.expression", "a.b"));
        Assertions.assertThrows(DataException.class, () -> {
            mrfSmt.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
        });
    }

    @Test
    public void nestedFieldExpression() {
        mrfSmt.configure(Collections.singletonMap("map.field.expression", "f.s"));

        final Schema simpleNestedSchema = SchemaBuilder.struct().name("nested").version(1).doc("doc").field("s", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Schema simpleRecordSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("f", simpleNestedSchema).build();

        final Struct simpleStruct = new Struct(simpleRecordSchema).put("f", new Struct(simpleNestedSchema).put("s", "abcd"));

        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleRecordSchema, simpleStruct);
        final SourceRecord transformedRecord = mrfSmt.apply(record);

        assertEquals(simpleRecordSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleRecordSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleRecordSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(simpleNestedSchema, transformedRecord.valueSchema().field("f").schema());
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("s").schema());

        assertEquals("abcd", ((Struct) transformedRecord.value()).getString("s"));
        assertEquals("abcd", ((Struct) transformedRecord.value()).getStruct("f").getString("s"));

        final SourceRecord transformedRecord2 = mrfSmt.apply(new SourceRecord(null, null, "test", 1, simpleRecordSchema, simpleStruct));
        assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());
    }


    @Test
    public void nestedFieldExpressionWithTargetFieldName() {
        Map<String, String> configs = new HashMap<>();
        configs.put("map.field.expression", "f.s");
        configs.put("map.field.target", "s2");
        mrfSmt.configure(configs);

        final Schema simpleNestedSchema = SchemaBuilder.struct().name("nested").version(1).doc("doc").field("s", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Schema simpleRecordSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("f", simpleNestedSchema).build();

        final Struct simpleStruct = new Struct(simpleRecordSchema).put("f", new Struct(simpleNestedSchema).put("s", "abcd"));
        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleRecordSchema, simpleStruct);

        final SourceRecord transformedRecord = mrfSmt.apply(record);

        final Schema expectedSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
                .field("f", simpleNestedSchema)
                .field("s2", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Struct expectedStruct = new Struct(expectedSchema)
                .put("f", new Struct(simpleNestedSchema).put("s", "abcd"))
                .put("s2", "abcd");
        final SourceRecord expectedRecord = new SourceRecord(null, null, "test", 0, expectedSchema, expectedStruct);


        assertEquals(expectedRecord, transformedRecord);
    }


}
