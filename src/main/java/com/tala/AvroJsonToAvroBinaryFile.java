package com.tala;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.*;

import java.io.*;

public class AvroJsonToAvroBinaryFile {

  public static void main (String[] args) throws IOException {

    String json = "{\"someLong\":5, \"number\":{\"int\":100}}";

    Schema avroSchema = SchemaBuilder.record("TestObject")
        .namespace("com.tala")
        .fields().requiredLong("someLong")
        .optionalInt("number")
        .endRecord();



    File file = new File("purebinary.bin");
    FileOutputStream fos = new FileOutputStream(file);
    BufferedOutputStream stream = new BufferedOutputStream(fos);

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

    encoder.flush();
    stream.flush();
    stream.close();
    fos.flush();
    fos.close();

  }



}
