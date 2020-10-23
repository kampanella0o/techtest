# techtest
Practical task for "Java BigData engineer internship" in Rinf Tech

Build a Docker container with this app, deploy it to Goole Cloud Run and it will parse any *.avro uploaded to Cloud Storage bucket
and save all fields to two tables (all fields and mandatory fields) in the GCP BigQUery.

The next parameters should be set as environmental variables:
ProjectID
BucketID
SubscriptionID
DataSetName
AllFieldsTableName
MandatoryFieldsTableName
