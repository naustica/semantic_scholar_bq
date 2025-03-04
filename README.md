# Workflow for Processing and Loading Semantic Scholar snapshots into Google BigQuery

This repository contains instructions on how to extract and transform Semantic Scholar data for data analysis with Google BigQuery.

## Requirements

- [Python3](https://www.python.org)
  - [gsutil](https://pypi.org/project/gsutil/)
  - [requests](https://pypi.org/project/requests/)

## Download snapshot

To download datasets from Semantic Scholar, replace the following placeholder with your API key.
```bash
$ export s2_key=YOUR_API_KEY
```

To download all datasets within a Semantic Scholar data snapshot, use:
```bash
$ python3 semantic_scholar.py --logging=info
```

To download individual datasets within a Semantic Scholar data snapshot, use:

```python
from semantic_scholar import SemanticScholar
import os

s2 = SemanticScholar(api_key=os.environ['s2_key'],
                     download_path='/scratch/users/haupka/semantic-scholar-snapshot',
                     snapshot_date='2025-02-25')
s2.download_papers()
s2.download_venues()
```

## Uploading Files to Google Bucket

```bash
$ gsutil -m cp -r /scratch/users/haupka/semantic-scholar-snapshot gs://bigschol
```

## Creating a BigQuery Table

```bash
$ bq load --ignore_unknown_values --source_format=NEWLINE_DELIMITED_JSON subugoe-collaborative.semantic_scholar.papers gs://bigschol/semantic-scholar-snapshot/papers/*.jsonl.gz papers_schema.json
```
