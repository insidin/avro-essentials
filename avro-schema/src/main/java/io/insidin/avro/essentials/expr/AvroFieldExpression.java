package io.insidin.avro.essentials.expr;

import io.insidin.avro.essentials.generic.record.Record;
import io.insidin.avro.essentials.generic.schema.Schema;
import io.insidin.avro.essentials.generic.schema.SchemaField;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.stream.Collectors;

public class AvroFieldExpression {

    private static final String pathSeparator = "\\.";

    private final LinkedList<AvroFieldSpec> avroFieldSpecs;

    private AvroFieldExpression(LinkedList<AvroFieldSpec> avroFieldSpecs) {
        this.avroFieldSpecs = avroFieldSpecs;
    }

    public static AvroFieldExpression parse(String expression) {
        LinkedList<AvroFieldSpec> fieldParts =
                Arrays.stream(expression.split(pathSeparator))
                        .map(AvroFieldSpec::parseFieldExpression)
                        .collect(Collectors.toCollection(LinkedList::new));
        return new AvroFieldExpression(fieldParts);
    }

    public String getFieldName() {
        return avroFieldSpecs.peekLast().getFieldName();
    }

    public <S extends Schema> S apply(S schema) {
        return apply(schema, new LinkedList<>(avroFieldSpecs));
    }

    public <R extends Record> Object apply(R record) {
        return apply(record, new LinkedList<>(avroFieldSpecs));
    }

    private <R extends Record> Object apply(R record, LinkedList<AvroFieldSpec> avroFieldSpecs) {
        AvroFieldSpec avroFieldSpec = avroFieldSpecs.get(0);
        if (avroFieldSpecs.size() == 1) {
            return avroFieldSpec.getField(record);
        }
        else {
            avroFieldSpecs.removeFirst();
            return apply(avroFieldSpec.getFieldRecord(record), avroFieldSpecs);
        }
    }

    private <S extends Schema> S apply(S schema, LinkedList<AvroFieldSpec> avroFieldSpecs) {
        S fieldSchema = apply(schema, avroFieldSpecs.get(0));
        if (avroFieldSpecs.size() == 1) {
            return fieldSchema;
        }
        else {
            avroFieldSpecs.removeFirst();
            return apply(fieldSchema, avroFieldSpecs);
        }
    }

    private <S extends Schema> S apply(S schema, AvroFieldSpec fieldSpec) {
        return fieldSpec.getFieldSchema(schema);
    }

    private static class AvroFieldSpec {

        private final String fieldName;
        private final Integer index;

        private AvroFieldSpec(String fieldName, Integer index) {
            this.fieldName = fieldName;
            this.index = index;
        }

        private AvroFieldSpec(String expr) {
            this(expr, null);
        }

        static AvroFieldSpec parseFieldExpression(String fieldExpression) {
            if (fieldExpression.endsWith("]")) {
                int idx = fieldExpression.lastIndexOf("[");
                String field = fieldExpression.substring(0, idx);
                String index = fieldExpression.substring(idx + 1, fieldExpression.length() - 1);
                return new AvroFieldSpec(field, Integer.parseInt(index));
            }
            return new AvroFieldSpec(fieldExpression);
        }

        String getFieldName() {
            return fieldName;
        }

        <S extends Schema> S getFieldSchema(S schema) {
            SchemaField field = schema.getField(fieldName);
            if (field == null) {
                throw new RuntimeException(String.format("FieldName {} not found", fieldName));
            }
            Schema s = field.getSchema().getRequiredSchema();
            if (s.isCollection()) {
                return (S)s.getCollectionValueSchema();
            }
            if (s.isUnion()) {
                return (S)s.getUnionValueSchema(index);
            }
            return (S)s;
        }

        <R extends Record> Object getField(R record) {
            return record.get(fieldName);
        }

        <R extends Record> Record<R> getFieldRecord(R record) {
            return record.getRecord(fieldName);
        }
    }



}
