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
.env files are git ignored.  They may not be available in the container once constructed unless the .gitignore is changed or the file is injected somehow.  Config may be more reliant on secrets (having to generate a secrets fetching client prior runtime) or other solution.

[ref](https://cloud.google.com/sdk/gcloud/reference/builds/submit)
```powershell
# Powershell
Set-Variable -Name "PROJECT" -Value ""
Set-Variable -Name "GCSLOGDIR" -VALUE ""
Set-Variable -Name "PIPELINE" -VALUE ""
Set-Variable -Name "TEMPLATE_IMAGE" -Value "gcr.io/$PROJECT/dataflow/$PIPELINE:latest"
gcloud builds submit --gcs-log-dir $GCSLOGDIR --tag $TEMPLATE_IMAGE .
```


## Building a Flex Template via gcloud
```powershell
Set-Variable -Name "REGION" -Value ""
Set-Variable -Name "NETWORK" -Value ""
Set-Variable -Name "SUBNETWORK" -Value ""
Set-Variable -Name "BUCKET" -Value ""
Set-Variable -Name "SERVICE_ACCOUNT_EMAIL" -Value ""
Set-Variable -Name "TEMPLATE_PATH" -Value "gs://$BUCKET/dataflow/templates/<pipeline_name>.json"
Set-Variable -Name "METADATA_FILE" -Value "<metadata_file>.json"
gcloud dataflow flex-template build $TEMPLATE_PATH --image "$TEMPLATE_IMAGE" --sdk-language "PYTHON" --metadata-file $METADATA_FILE --network $NETWORK --subnetwork $SUBNETWORK --project $PROJECT --worker-region $REGION --service-account-email $SERVICE_ACCOUNT_EMAIL
```

## Run Flex Template
```powershell

gcloud dataflow flex-template run "<pipeline-name>-`date +%Y%m%d-%H%M%S` " --template-file-gcs-location $TEMPLATE_PATH --parameters <parameter_name>=<parameter_value>...repeat_for_ea --region "$REGION" --project ut-dws-udrc-dev --setup-file setup.py
```