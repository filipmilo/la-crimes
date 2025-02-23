# Los Angeles crime data analysis


### Architecture

![High level project architecture](diagrams/architecture.png)


### Running the cluster

`./scripts/cluster_up.sh`


### Shut off

`./scripts/cluster_down.sh`

### Notice

- Instead of HDFS, S3 will be used since my machine can't handle the entire cluster.
- Before running the cluster make sure to have an AWS account setup.
- Put the historical dataset into `./Airflow/files/`
