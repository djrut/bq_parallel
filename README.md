# BigQuery Parallel testing tool
---
## Dependencies

- Python 3.7

## Installation

pip install -r requirements.txt

## Usage

usage: bq_parallel.py [-h] [--query-file QUERY_FILE]
                      (--scenarios SCENARIOS | --threads THREADS)
                      [--samples SAMPLES] --output-file OUTPUT_FILE
                      [--format {csv}] [-C]

  -h, --help            show this help message and exit
  --query-file QUERY_FILE, -f QUERY_FILE
                        JSON file containing queries to execute
  --scenarios SCENARIOS
                        Comma separated list of concurrency scenarios to run
                        i.e. 10,20,30,40
  --threads THREADS     Number of concurrent threads/queries (ad-hoc usage)
  --samples SAMPLES     Number of samples to take for each thread
  --output-file OUTPUT_FILE, -O OUTPUT_FILE
                        Filename to output results to
  --format {csv}        Final results format
  -C, --use-cache       Enable query caching

## Examples

Single scenario run launching 3 concurrent queries:
```
python bq_parallel.py --query-file path/to/config/query.yaml --threads=3 --output-file=output.csv
```

Multi-scenario run:
```
python bq_parallel.py --query-file path/to/config/query.yaml --scenarios=2,4,6,8,10 --output-file=output.csv
```
