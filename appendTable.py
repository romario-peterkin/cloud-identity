from google.cloud import bigquery

def appendBigQueryTable(dataset, table, uri):
    client = bigquery.Client()
    table_ref = client.dataset(dataset).table(table)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.time_partitioning = bigquery.table.TimePartitioning(
      bigquery.table.TimePartitioningType.DAY
    )
    load_job = client.load_table_from_uri(
    uri, table_ref, job_config=job_config
    )  # API request
    print("Starting BQ job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(table_ref)
    print("Loaded {} rows.".format(destination_table.num_rows))
