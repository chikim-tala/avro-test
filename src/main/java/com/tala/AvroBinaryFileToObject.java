package com.tala;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.*;

public class AvroBinaryFileToObject {

  public static void main (String[] args) throws IOException {

    Schema avroSchema = SchemaBuilder.record("TestObject")
        .namespace("com.tala")
        .fields().requiredLong("someLong")
        .optionalInt("number")
        .endRecord();

    File file = new File("purebinary.bin");
    FileInputStream stream = new FileInputStream(file);
    //ByteArrayOutputStream stream = new ByteArrayOutputStream();

    DatumReader<TestObject> reader = new SpecificDatumReader<>(avroSchema);

    Decoder decoder = DecoderFactory.get().binaryDecoder(stream, null);

    TestObject datum;
    while (true) {
      try {
        datum = reader.read(null, decoder);
      } catch (EOFException e) {
        break;
      }
      System.out.println (datum);
    }


    //System.out.println(stream.toByteArray());
  }
}
