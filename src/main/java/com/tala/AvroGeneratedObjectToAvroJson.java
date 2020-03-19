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

public class AvroGeneratedObjectToAvroJson {

  public static void main (String[] args) throws IOException {

    TestObject to = new TestObject();
    to.setSomeLong(5);
    to.setNumber(null);


    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    DatumWriter<TestObject> writer = new GenericDatumWriter<>(TestObject.getClassSchema());



    Encoder encoder = EncoderFactory.get().jsonEncoder(TestObject.getClassSchema(),stream);

    writer.write(to, encoder);
    encoder.flush();

    System.out.println(new String(stream.toByteArray()));

    System.out.println(to.getSchema().toString());



  }
}
