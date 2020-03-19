package com.tala;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.*;

public class AvroJsonToAvroFile2 {

  public static void main (String[] args) throws IOException {

    String json = "{\"someLong\":5, \"number\":{\"int\":100}}";

    Schema avroSchema = SchemaBuilder.record("TestObject")
        .namespace("com.tala")
        .fields().requiredLong("someLong")
        .optionalInt("number")
        .endRecord();

    File file = new File("a.avro");
    FileOutputStream stream = new FileOutputStream(file);
    //ByteArrayOutputStream stream = new ByteArrayOutputStream();

    DatumReader<Object> reader = new GenericDatumReader<>(avroSchema);

    DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());

    Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, json);

    writer.create(avroSchema, stream);

    Object datum;
    while (true) {
      try {
        datum = reader.read(null, decoder);
      } catch (EOFException e) {
        break;
      }
      writer.append(datum);
    }
    writer.close();

    //System.out.println(stream.toByteArray());
  }



}
