package io.insidin.avro.essentials.generic.schema;

public interface Schema<S> {

    S getSchema();

    SchemaField getField(String fieldName);

    Schema getRequiredSchema();

    Boolean isOptional();

    Boolean isUnion();

    Boolean isOptionalUnion();

    Boolean isRecord();

    Boolean isCollection();

    Schema<S> getCollectionValueSchema();

    Schema<S> getUnionValueSchema(int index);
}
