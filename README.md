# Simple ETL process using Google Dataflow
This process reads in XML files from a GCS bucket, parses the XML using ElementTree, extracts desired data and writes it to a BigQuery table.

Some kinks need to be ironed out (e.g. replacing deprecated "BigQuerySink" method, writing side-outputs for any exceptions)
