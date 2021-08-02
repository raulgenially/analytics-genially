## Config for Deploying the DBT web

The job runs in [Google Cloud build](https://cloud.google.com/build). There is a trigger (`analytics-docs`) setup 
in the [GCP console](https://console.cloud.google.com/cloud-build/triggers?project=data-genially)
that is run when a commit is pushed to the `master` branch. When the job runs, it executes a `dbt docs generate` and
uploads the web to AppEngine.

The job definition is in `cloudbuild.yaml` and it has three main parts:

- Download the production `profiles.yml` necessary for the dbt commands. This file is
  [stored](https://console.cloud.google.com/security/secret-manager?project=data-genially) securely
  in google's [Secret Manager](https://cloud.google.com/secret-manager).

- Generate the web with `dbt`. The command is run from a dbt-ready docker image maintained by the dbt team and 
  tagged with the dbt version it has installed. Should we upgrade the dbt version we use for the project, we
  should also change the version of these images.

- Deploy the web to AppEngine. The deployed version is tagged with the commit id. Only the last
  [5 versions](https://console.cloud.google.com/appengine/versions?project=data-genially&serviceId=default) of
  the web are kept.

The web is available [here](https://data-genially.appspot.com) and is only accessible to people within the
data@genial.ly email group. The access control has been implemented with
[Google's IAP](https://console.cloud.google.com/security/iap?project=data-genially).

### Testing `cloudbuild.yaml`

If you have to modify `cloudbuild.yaml` and you want to test that it works, you can use the `gcloud`
command to manually submit builds to Cloud build. From the root of the project you need to run:

```bash
 gcloud builds submit . --config=deployments/docs/cloudbuild.yaml --substitutions=COMMIT_SHA=1234
```

With the `--substitutions` [parameter](https://cloud.google.com/sdk/gcloud/reference/builds/submit)
you can feed environment variables that are normally injected when the build is run from a trigger.
See the variables that are injected [here](https://cloud.google.com/build/docs/configuring-builds/substitute-variable-values)

### Additional resources

[[Link](https://cloud.google.com/build/docs/automating-builds/create-manage-triggers)] - How to create a build trigger \
[[Link](https://codelabs.developers.google.com/codelabs/user-auth-with-iap)] - How to control access with IAP
