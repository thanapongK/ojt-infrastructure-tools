# Example

Airflow Worker + Spark Driver -- Papermill Operator --> Spark Executors

Airflow Worker -- Kubernetes Pod Operator --> Spark Driver (Pod) ---> Spark Executors

## code

PapermillOperator ----> เรียก notebook.ipynb -- เปิด spark + execute ---> Spark Executor

## sample

python /app/run.py ----> papermill execute notebook

```python
# /app/run.py

# opentelemetry configuration
# logger configuration
# other configuration

def execute():
  import papermill as pm

  pm.execute_notebook(
    target_notebook_path,
    "./output/output-{0}.ipynb".format(execute_timestamp),
    parameters={
      # config parse via airflow
    },
  )

if __name__ == "__main__":
  execute()
```
