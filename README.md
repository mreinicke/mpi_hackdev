# MPI Alpha Pipelines
Dataflow pipelines, APIs, loggers, tests, and controllers to expose all necessary services needed for matching in the UDRC GCP system.


## Running Pipelines Locally via DataFlow
Requires significant CLI command


### pipeline_update_firestore_to_bigquery
Windows CLI

```bash
 python -m pipeline_update_firestore_to_bigquery --runner DataflowRunner --region us-central1 --network udrc-app-network --subnetwork regions/us-central1/subnetworks/central-subnet --setup_file C:\Users\vbrandon\Desktop\H_sync\bin\mpi_hackdev\setup.py --project ut-dws-udrc-dev --secret projects/319293654677/secrets/mpi-sa-key/versions/latest --bucket mpi-dev-bucket --collection “hackathon_pool” --vectable ut-dws-udrc-dev.MPI.mpi_vectors
```


## Building a GCR Image via gcloud
[ref](https://cloud.google.com/sdk/gcloud/reference/builds/submit)
```powershell
# Powershell
Set-Variable -Name "PROJECT" -Value "ut-dws-udrc-dev"
Set-Variable -Name "GCSLOGDIR" -VALUE "gs://mpi-dev-bucket/logging"
Set-Variable -Name "TEMPLATE_IMAGE" -Value "gcr.io/$PROJECT/dataflow/preprocess_table:latest"
gcloud builds submit --gcs-log-dir $GCSLOGDIR --tag $TEMPLATE_IMAGE .
```


## Building a Flex Template via gcloud
```powershell
Set-Variable -Name "TEMPLATE_PATH" -Value "gs://mpi-dev-bucket/dataflow/templates/<pipeline_name>.json"
gcloud dataflow flex-template build $TEMPLATE_PATH --image "$TEMPLATE_IMAGE" --sdk-language "PYTHON" --metadata-file "<insert_name_of_metdatada_file>.json"
```

## Run Flex Template
```powershell
Set-Variable -Name "REGION" -Value "us-central1"
```