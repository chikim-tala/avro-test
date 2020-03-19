package com.tala;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

public class AvroJsonToAvroBinaryPrint {

  public static void main (String[] args) throws IOException {

    String json = "{\"someLong\":5, \"number\":{\"int\":100}}";

    Schema avroSchema = SchemaBuilder.record("TestObject")
        .namespace("com.tala")
        .fields().requiredLong("someLong")
        .optionalInt("number")
        .endRecord();




    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    DatumReader<Object> reader = new GenericDatumReader<>(avroSchema);

    //DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
    DatumWriter<Object> writer = new GenericDatumWriter<>(avroSchema);

    Decoder decoder = DecoderFactory.get().jsonDecoder(avroSchema, json);
    Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);


    //writer.create(avroSchema, stream);

    Object datum;
    while (true) {
      try {
        datum = reader.read(null, decoder);
      } catch (EOFException e) {
        break;
      }
      writer.write(datum, encoder);
    }

    System.out.println(stream.toByteArray());
  }



}
