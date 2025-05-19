# Salescience

Salescience is a modular, scalable platform for financial document analysis and reporting. It fetches SEC filings, processes and analyzes them, and generates human-readable reports. The architecture is designed for clarity, flexibility, and production-readiness.

## Features
- Fetch SEC filings for any company
- Extract and process financial documents
- Generate vector embeddings for semantic search and ML
- Analyze filings with LLMs or rule-based logic
- Output reports to file or database
- Modular, testable, and ready for cloud deployment

## Project Structure
```
├── main.py
├── config.py
├── data_acquisition/
├── processing/
├── storage/
├── analysis/
├── reporting/
├── api/
├── tests/
├── shell.nix
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── kubernetes/
```

## Getting Started

### 1. Development Environment (Nix)
This project uses [Nix](https://nixos.org/) for a reproducible dev environment.

- Install Nix: https://nixos.org/download.html
- Enter the dev shell:
  ```sh
  nix-shell
  ```
- This gives you Python, pip, Docker, Postgres, Redis, and more.

### 2. Install Python Dependencies
- Inside the nix-shell:
  ```sh
  pip install -r requirements.txt
  ```

### 3. Running Locally with Docker Compose
- Build and run all services:
  ```sh
  docker-compose up --build
  ```
- The API will be available at http://localhost:8000

### 4. Environment Variables
- Copy `.env.example` to `.env` and fill in your secrets/config.
- Key variables:
  - `DATABASE_URL` (if using a database)
  - `REDIS_URL` (if using Redis)

### 5. Production Deployment
- Use the Dockerfile to build your app image.
- Use the `kubernetes/` manifests to deploy to a Kubernetes cluster.
- Use a managed database (e.g., AWS RDS) in production.

#### Deploying Redis in Kubernetes (Production)
- The `kubernetes/redis-deployment.yaml` and `kubernetes/redis-service.yaml` files provide a production-ready Redis deployment:
  - `redis-deployment.yaml`: StatefulSet with persistent storage, resource limits, and health checks.
  - `redis-service.yaml`: Internal ClusterIP service exposing Redis on port 6379.
- To deploy Redis:
  ```sh
  kubectl apply -f kubernetes/redis-deployment.yaml
  kubectl apply -f kubernetes/redis-service.yaml
  ```
- Your app should use the following environment variable to connect to Redis:
  ```sh
  REDIS_URL=redis://redis:6379/0
  ```
  (This works for in-cluster communication; adjust as needed for your environment.)
- **Alternative:** For production, you may use a managed Redis service (e.g., AWS ElastiCache, Azure Cache for Redis). In that case, set `REDIS_URL` to the managed endpoint.

#### Running the Orchestrator API and Worker (Production)
- The orchestrator API (`data_acquisition/orchestrator_api.py`) submits jobs to all data sources by enqueuing them to a Redis Queue (RQ).
- The worker (`data_acquisition/worker.py`) listens to the queue, processes jobs, and updates Redis with real status/results.
- To run both:
  1. Ensure Redis is running (see Kubernetes setup above).
  2. Install dependencies:
     ```sh
     pip install fastapi uvicorn redis rq pydantic python-dotenv
     ```
  3. Start the orchestrator API:
     ```sh
     uvicorn data_acquisition.orchestrator_api:app --host 0.0.0.0 --port 8100
     ```
  4. In a separate terminal, start the worker:
     ```sh
     rq worker -w data_acquisition.worker.DataAcquisitionWorker data_acquisition
     ```
  5. Use the orchestrator API endpoints to submit jobs, check status, and get results.
- Set the `REDIS_URL` environment variable as needed (default: `redis://redis:6379/0`).

## Contributing
- PRs and issues welcome!
- Please keep code modular and well-documented.

## License
MIT
