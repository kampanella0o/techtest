package com.maksym.techtest.service;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.maksym.techtest.repository.BigQueryRepository;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Service
public class AvroParser {

    Logger logger = LoggerFactory.getLogger(AvroParser.class);

    @Autowired
    BigQueryRepository bigQueryRepository;

    public void parseAvroFile(String bucketFileName) {
        logger.info("Getting {} from storage", bucketFileName);
        Storage storage = StorageOptions.newBuilder().setProjectId("extreme-water-293016").build().getService();

        Blob blob = storage.get(BlobId.of("myermolenko-new-bucket", bucketFileName));
        Path localFile = Paths.get("src/main/java/temp/" + bucketFileName); //TODO replace with temp file
        blob.downloadTo(localFile);

        logger.info("Getting avro schema from {} and parsing it", bucketFileName);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<>(localFile.toFile(), datumReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Schema avroFileSchema = dataFileReader.getSchema();
        Map<String, String> fields = new HashMap<>();
        avroFileSchema.getFields().forEach(f -> fields.put(f.name(), f.schema().getType().getName()));
        GenericRecord record = null;
        try {
            record = dataFileReader.next(record);
        } catch (IOException e) {

            e.printStackTrace();
        }

        for (String field : fields.keySet()) {
            bigQueryRepository.insertIntoAllFields(bucketFileName, avroFileSchema.getName(), field, record.get(field).toString());
        }

        try {
            Files.delete(localFile);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage());
        }

    }
}
