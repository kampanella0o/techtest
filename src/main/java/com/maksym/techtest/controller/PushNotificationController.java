package com.maksym.techtest.controller;

import java.io.IOException;
import java.util.Base64;

import com.google.cloud.storage.StorageException;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.maksym.techtest.service.AvroParser;
import com.maksym.techtest.util.PushNotificationRequestBody;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class PushNotificationController {

    private Logger logger = LoggerFactory.getLogger(AvroParser.class);

    private final AvroParser avroParser;

    public PushNotificationController(AvroParser avroParser) {
        this.avroParser = avroParser;
    }

    @RequestMapping(value = "/", method = RequestMethod.POST)
    public ResponseEntity<String> receiveMessage(@RequestBody PushNotificationRequestBody pushNotificationRequestBody) {
        PushNotificationRequestBody.Message message = pushNotificationRequestBody.getMessage();
        if (message == null) {
            String msg = "Bad Request: invalid Pub/Sub message format";
            return new ResponseEntity<>(msg, HttpStatus.BAD_REQUEST);
        }

        String data = message.getData();

        JsonObject obj = JsonParser.parseString(new String(Base64.getDecoder().decode(data))).getAsJsonObject();

        String bucketFileName = obj.get("name").getAsString();

        logger.info("File '{}' was changed in storage", bucketFileName);

        if (FilenameUtils.getExtension(bucketFileName).equals("avro")){
            try {
                avroParser.parseAvroFile(bucketFileName);
            } catch (StorageException e) {
                logger.error("Failed to connect to Google Cloud Storage: ", e);
            } catch (IOException e) {
                logger.error("Failed to process the file: ", e);
            } catch (RuntimeException e) {
                logger.error("Something went wrong during parsing the file: ", e);
            }
        }

        return new ResponseEntity<>(obj.toString(), HttpStatus.OK);
    }


}
