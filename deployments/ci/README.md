## Config for the CI job

The job runs in [Google Cloud build](https://cloud.google.com/build). There is a trigger setup 
in the [GCP console](https://console.cloud.google.com/cloud-build/triggers?project=data-genially)
for this repository so that when a PR is opened with `master` as the base branch, the job is run.
When the job runs, it executes a `dbt seed`, `run` and `test` in a BigQuery dataset with the name
`dbt_ci_{issue-number}`.

The job definition is in `cloudbuild.yaml` and it has two main parts:

- Download the `profiles.yml` necessary for the dbt commands. This file is
  [stored](https://console.cloud.google.com/security/secret-manager?project=data-genially) securely
  in google's [Secret Manager](https://cloud.google.com/secret-manager) and it is
  [parameterized](https://docs.getdbt.com/reference/dbt-jinja-functions/env_var) with a `PULL_REQUEST`
  environment variable so that the target dataset name is dynamic.

- Run `dbt` commands. All dbt commands are run from a dbt-ready docker image maintained by the dbt team and 
  tagged with the dbt version it has installed. Should we upgrade the dbt version we use for the project, we
  should also change the version of these images.

There is another trigger setup to delete the dataset we just created for the CI build when the PR is
merged. The config for this job is in `cleanupbuild.yaml`.

### Testing `cloudbuild.yaml`

If you have to modify `cloudbuild.yaml` and you want to test that it works, you can use the `gcloud`
command to manually submit builds to Cloud build. From the root of the project you need to run:

```bash
 gcloud builds submit . --config=deployments/ci/cloudbuild.yaml --substitutions=_PR_NUMBER=1234
```

With the `--substitutions` [parameter](https://cloud.google.com/sdk/gcloud/reference/builds/submit)
you can feed environment variables that are normally injected when the build is run from a trigger.
See the variables that are injected [here](https://cloud.google.com/build/docs/configuring-builds/substitute-variable-values)


### Additional resources

[[Link](https://cloud.google.com/build/docs/automating-builds/create-manage-triggers)] - How to create a build trigger
