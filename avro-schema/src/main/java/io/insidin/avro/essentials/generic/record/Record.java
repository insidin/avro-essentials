package io.insidin.avro.essentials.generic.record;

public interface Record<R> {

    Object get(String field);

    Record getRecord(String fieldName);
}
