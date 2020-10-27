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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class AvroParser {

    private Logger logger = LoggerFactory.getLogger(AvroParser.class);

    private final
    BigQueryRepository repository;

    public AvroParser(BigQueryRepository repository) {
        this.repository = repository;
    }

    public void parseAvroFile(String bucketFileName) throws IOException {

        final Path tempDir = Paths.get("tmp/");
        FileUtils.cleanDirectory(tempDir.toFile());

        logger.info("Setting storage connection");
        System.setProperty("https.protocols", "TLSv1,TLSv1.1,TLSv1.2");
        StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder().setHost("https://storage.googleapis.com/");
//        Storage storage = optionsBuilder.build().getService();
        Storage storage = optionsBuilder.setProjectId(System.getenv("ProjectID")).build().getService();

        logger.info("Getting {} from storage", bucketFileName);
        Blob blob = storage.get(BlobId.of(System.getenv("BucketID"), bucketFileName));

        logger.info("Checking if {} exists in storage", bucketFileName);
        if (blob == null) {
            logger.info("{} does not exist in storage", bucketFileName);
            return;
        }

        Path localFile = Files.createTempFile(tempDir, FilenameUtils.getName(bucketFileName), ".tmp");

        logger.info("Downloading {} from storage to {}", bucketFileName, localFile.getFileName());
        blob.downloadTo(localFile);

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(localFile.toFile(), datumReader)) {

            logger.info("Getting avro schema from {} and parsing it", bucketFileName);
            Schema avroFileSchema = dataFileReader.getSchema();

            Set<String> fields = avroFileSchema.getFields().stream()
                    .map(Schema.Field::name)
                    .collect(Collectors.toSet());

            GenericRecord record = dataFileReader.next();

            fields.parallelStream()
                    .forEach(field -> repository.insertField(checkMandatory(avroFileSchema, field), bucketFileName, avroFileSchema.getName(), field, String.valueOf(record.get(field))));

        } catch (IOException e) {
            throw new IOException(e);
        }

        Files.delete(localFile);

    }

    private boolean checkMandatory(Schema avroFileSchema, String field) {
        return !avroFileSchema.getField(field).schema().toString().contains("null");
    }
}
