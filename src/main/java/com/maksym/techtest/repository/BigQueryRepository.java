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
    private final String ALL_FIELDS_TABLE = System.getenv("ProjectID")
            + "." + System.getenv("DataSetName")
            + "." + System.getenv("AllFieldsTableName");
    private final String MANDATORY_FIELDS_TABLE =  System.getenv("ProjectID")
            + "." + System.getenv("DataSetName")
            + "." + System.getenv("MandatoryFieldsTableName");
    private BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    /**
     * Inserts the received data to the proper table of BigQuery
     * @param isMandatory if true add the data also to the Mandatory Fields Table
     * @param fileName name of the file in a bucket
     * @param objectName name of the object according to the AVRO schema
     * @param fieldName name of the field according to the AVRO schema
     * @param fieldValue value of the field
     */
    public void insertField(boolean isMandatory, String fileName, String objectName, String fieldName, String fieldValue){
        String query = String.format(QUERY_TEMPLATE, ALL_FIELDS_TABLE, fileName, objectName, fieldName, fieldValue);
        sendQuery(query);
        if (isMandatory) {
            query = String.format(QUERY_TEMPLATE, MANDATORY_FIELDS_TABLE, fileName, objectName, fieldName, fieldValue);
            sendQuery(query);
        }
    }

    private void sendQuery(String query){
        logger.info("Querying the BigQuery with '{}'", query);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query)
                        .setUseLegacySql(false)
                        .build();

        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        try {
            queryJob = queryJob.waitFor();
        } catch (InterruptedException e) {
            logger.error("Query job failed", e);
        }

        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }
    }

}
