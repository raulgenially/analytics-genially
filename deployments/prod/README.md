## Config for the Production job

The job runs in [Google Cloud build](https://cloud.google.com/build). There is a trigger (`analytics-prod`) setup 
in the [GCP console](https://console.cloud.google.com/cloud-build/triggers?project=data-genially)
that is run on a [schedule](https://console.cloud.google.com/cloudscheduler?project=data-genially)
(`analytics-prod-schedule`). When the job runs, it executes a `dbt seed`, `run` and `test` in a BigQuery
dataset with the name `dbt_production`.

The job definition is in `cloudbuild.yaml` and it has two main parts:

- Download the `profiles.yml` necessary for the dbt commands. This file is
  [stored](https://console.cloud.google.com/security/secret-manager?project=data-genially) securely
  in google's [Secret Manager](https://cloud.google.com/secret-manager).

- Run `dbt` commands. All dbt commands are run from a dbt-ready docker image maintained by the dbt team and 
  tagged with the dbt version it has installed. Should we upgrade the dbt version we use for the project, we
  should also change the version of these images.

### Slack notifications

Slack notifications are enabled for cloud build triggers. See
[this](https://github.com/Genially/data-cloud-build-notifier) repo for more info.

### Testing `cloudbuild.yaml`

If you have to modify `cloudbuild.yaml` and you want to test that it works, you can use the `gcloud`
command to manually submit builds to Cloud build. From the root of the project you need to run:

```bash
 gcloud builds submit . --config=deployments/prod/cloudbuild.yaml
```

### Additional resources

[[Link](https://cloud.google.com/build/docs/automating-builds/create-manage-triggers)] - How to create a build trigger \
