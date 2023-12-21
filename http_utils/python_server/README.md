# Install FastAPI and Uvicorn

```
pip install fastapi uvicorn
```

# Running

- `uvicorn program:app --reload`

This will launch a Python HTTP server listening on the following HTTP address:

- http://0.0.0.0:24478

# python_client files

## HTTP server stats

```
python watch.py
```

Prints statistics at a rate of once per second.

## Export

```
python export.py --export_path <path>
```

Exports all jobs (also called experiments), including all configuration and results.

## Testing

### Logic Tests

```
python test.py
```

Runs a set of simple tests on the server.

### Stress Testing

```
python submit.py -n <Simulations Per job> -m <Job Count>
```

Submits a set of jobs (experiments) to the server.

```
python stress_request.py
```

Starts a python client which requests pieces of work from the server. This client does not return any results.

```
python stress_with_finish.py
```

Starts a python client which requests pieces of work from the server. The client then returns mock results to the server and lets it know when pieces of work are finished.
This server simulates error rates where some pieces of work fail to finish. The server will automatically restart failed pieces of work.