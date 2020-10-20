package com.maksym.techtest.repository;

import com.maksym.techtest.service.AvroParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;

import java.util.UUID;

@Repository

public class BigQueryRepository {
    private Logger logger = LoggerFactory.getLogger(AvroParser.class);

    private final String QUERY_TEMPLATE = "INSERT INTO `%s" +
            "` VALUES (\"%s\", \"%s\", \"%s\", \"%s\");";
    private final String ALL_FIELDS_TABLE = "extreme-water-293016.AvroFilesFields.AllFields";
    private final String MANDATORY_FIELDS_TABLE = "extreme-water-293016.AvroFilesFields.MandatoryFields";
    private BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    public void insertIntoAllFields(String fileName, String objectName, String fieldName, String fieldValue){
        logger.info("Creating a query");
        String query = String.format(QUERY_TEMPLATE, ALL_FIELDS_TABLE, fileName, objectName, fieldName, fieldValue);
        sendQuery(query);
    }
    public void insertIntoMandatoryFields(String fileName, String objectName, String fieldName, String fieldValue){
        logger.info("Creating a query");
        String query = String.format(QUERY_TEMPLATE, MANDATORY_FIELDS_TABLE, fileName, objectName, fieldName, fieldValue);
        sendQuery(query);
    }

    private void sendQuery(String query){
        logger.info("Querying the BigQuery with '{}'", query);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query)
                        // Use standard SQL syntax for queries.
                        // See: https://cloud.google.com/bigquery/sql-reference/
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        try {
            queryJob = queryJob.waitFor();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // You can also look at queryJob.getStatus().getExecutionErrors() for all
            // errors, not just the latest one.
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
    }

}
