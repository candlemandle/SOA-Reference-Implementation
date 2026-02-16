# Marketplace Architecture

In this assignment, I designed the architecture for a Marketplace system using microservices and implemented a basic service containerized with Docker.

## 1. C4 Diagram 
The architecture is described using the C4 model notation and **Mermaid.js**. It outlines the main containers: Web App, API Gateway, and various microservices.

![C4 Diagram](C4_Diagram.png)

*(The diagram is available in the file C4_Diagram.png)*

## 2. Service Implementation
I implemented a basic **Notification Service** using Python (FastAPI), which is containerized using Docker.

### How to Run
Ensure you have **Docker Desktop** installed and running.

1. Navigate to the task directory:
   ```bash
   cd "Task 1"
   docker compose up --build
   ```
   Exprected output: {"status": "OK", "service": "Notification Service"}