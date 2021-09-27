# MPI Alpha Pipelines
Dataflow pipelines, APIs, loggers, tests, and controllers to expose all necessary services needed for matching in the UDRC GCP system.


## Running Pipelines Locally via DataFlow
Requires significant CLI command


### pipeline_update_firestore_to_bigquery
Windows CLI

```bash
 python -m pipeline_update_firestore_to_bigquery --runner DataflowRunner --region us-central1 --network udrc-app-network --subnetwork regions/us-central1/subnetworks/central-subnet --setup_file C:\Users\vbrandon\Desktop\H_sync\bin\mpi_hackdev\setup.py --project ut-dws-udrc-dev --secret projects/319293654677/secrets/mpi-sa-key/versions/latest --bucket mpi-dev-bucket --collection “hackathon_pool” --vectable ut-dws-udrc-dev.MPI.mpi_vectors
```