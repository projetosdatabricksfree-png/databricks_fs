# Lakeflow Job Orchestration Template

Reference YAML for a multi-task Lakeflow Job (Databricks Asset Bundles `resources/`).
Demonstrates `for_each_task`, retries, triggers, `pipeline_task`, task values, and
`condition_task` branching. Parameters flow from `databricks.yml` variables.

## Ingestion job: cron + for_each + retries

```yaml
resources:
  jobs:
    job_ingestao:
      name: "${var.env_prefix}_ingestao"
      trigger:
        pause_status: UNPAUSED
        periodic:
          interval: 1
          unit: DAYS
      tasks:
        - task_key: ingerir_series
          for_each_task:
            inputs: "${var.series_sgs}"        # list fanned out
            concurrency: 4
            task:
              task_key: ingerir_uma_serie
              notebook_task:
                notebook_path: ../src/ingestion/fetch_bcb_sgs.py
                base_parameters:
                  catalog: "${var.catalog}"
                  env_prefix: "${var.env_prefix}"
                  codigo_serie: "{{input}}"
              max_retries: 3
              min_retry_interval_millis: 30000
              retry_on_timeout: true
```

## Orchestration job: file-arrival trigger + pipeline + condition branch

```yaml
resources:
  jobs:
    job_orquestracao:
      name: "${var.env_prefix}_orquestracao"
      trigger:
        pause_status: UNPAUSED
        file_arrival:
          url: "/Volumes/${var.catalog}/${var.env_prefix}_bronze/landing/"
          min_time_between_triggers_seconds: 300
      tasks:
        - task_key: executar_pipeline
          pipeline_task:
            pipeline_id: "${resources.pipelines.pipeline_medallion.id}"

        - task_key: auditoria
          depends_on: [{ task_key: executar_pipeline }]
          notebook_task:
            notebook_path: ../src/pipelines/06_auditoria_qualidade.py
            base_parameters:
              catalog: "${var.catalog}"
              env_prefix: "${var.env_prefix}"

        - task_key: verificar_qualidade
          depends_on: [{ task_key: auditoria }]
          condition_task:
            op: EQUAL_TO
            left: "{{tasks.auditoria.values.status_qualidade}}"
            right: "OK"

        - task_key: publicar_gold
          depends_on: [{ task_key: verificar_qualidade, outcome: "true" }]
          notebook_task:
            notebook_path: ../src/pipelines/publicar_gold.py

        - task_key: alerta_qualidade
          depends_on: [{ task_key: verificar_qualidade, outcome: "false" }]
          notebook_task:
            notebook_path: ../src/pipelines/alerta_qualidade.py
```

## Notes

- `pause_status`/prefix `[dev <user>]` are handled automatically by `mode: development`;
  in `prod` triggers stay active.
- `condition_task` reads a **task value** set upstream via `dbutils.jobs.taskValues.set`.
- Use **Repair run** in the UI to re-execute only `alerta_qualidade`/failed tasks after a fix.
- `base_parameters` are read in notebooks with `dbutils.widgets.get(...)`.
