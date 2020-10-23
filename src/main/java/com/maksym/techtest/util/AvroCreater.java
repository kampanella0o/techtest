package com.maksym.techtest.util;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroCreater {

    public static void main(String[] args) throws IOException {
        Schema avroSchema = new Schema.Parser().parse(new File("src/main/resources/avro/schema/client.avsc"));

        Client client = new Client();
        client.setId(0L);
        client.setName("Test2");
        client.setAddress("address");
        client.setPhone("09810061");

        DatumWriter<Client> clientDatumWriter = new SpecificDatumWriter<>(Client.class);
        DataFileWriter<Client> dataFileWriter = new DataFileWriter<>(clientDatumWriter);
        final File file = new File("tmp/new_client.avro");

        try {
            dataFileWriter.create(avroSchema, file);
            dataFileWriter.append(client);
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
