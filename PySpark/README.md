# PySpark modules

## Modules included
* **mcc_filter.py** - Filter by MCC:
  - Filters the **OpenCellid** dataset to only include a specific country, using the passed mobile country code (MCC).
  - Loads the data to the specified Elasticsearch index.
  - Usage: python mcc_filter.py dataset_path target_mcc es_index where:
    - **data_path** is the path to the OpenCellid dataset
    - **taget_mcc** is the MCC of the taget country
    - **es_index** is the Elasticsearch index onto which the data will be loaded upon completion
