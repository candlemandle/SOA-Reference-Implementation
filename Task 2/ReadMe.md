# Marketplace API

## Overview
This project is a backend service for a marketplace, built from scratch. The main goal of the project was to design and implement a Marketplace API with a contract-first approach and complex business logic.

## Task Description
The project required fulfilling a strict set of technical and architectural requirements:

* I had to design an OpenAPI specification for a `Product` entity with full CRUD operations, pagination, and filtering.
* The data schemas had strict validation rules (e.g., boundaries, enums) described directly in the spec.
* I needed to ensure that DTOs were generated automatically from the OpenAPI spec using a single command.
* The use of manually written DTOs was prohibited.
* The project required PostgreSQL and a strict migration tool like Flyway or Liquibase to manage the database schema.
* Deleting a product had to be implemented as a "soft delete" by changing its status to `ARCHIVED`.
* Creating and managing orders required strict transactional checks, including rate limiting order creation.
* I had to prevent multiple active orders per user.
* I had to reserve stock and check product availability.
* I had to apply complex promo code logic like percentage vs. fixed discounts, maximum uses, and expiration dates.
* I had to implement strict state machine transitions for orders.
* Every API request had to be logged in JSON format.
* The logs had to capture details like `request_id`, execution duration, and method, while masking sensitive data like passwords.
* I had to implement JWT authorization with short-lived access tokens and long-lived refresh tokens.
* The endpoints needed to be protected by a Role-Based Access Control matrix for `USER`, `SELLER`, and `ADMIN` roles.

## Implementation Details
To tackle these requirements, I chose a modern, asynchronous Python stack:

* **Framework:** **FastAPI**. It natively supports OpenAPI, async operations, and dependency injection, making it perfect for building this service.
* **Database & ORM:** **PostgreSQL** coupled with asynchronous **SQLAlchemy** (`asyncpg`). I wrote pure SQL migrations and used **Flyway** via Docker to apply them cleanly.
* **OpenAPI Code Generation:** I defined the API contract in `openapi.yaml` and used `datamodel-code-generator` in a custom bash script (`generate.sh`) to automatically build Pydantic V2 models.
* **Business Logic:** The order logic is built using SQLAlchemy sessions to ensure ACID compliance. If a user tries to order an item without sufficient stock, or if a promo code is invalid, the transaction is rolled back, and a specific contract-compliant error (e.g., `INSUFFICIENT_STOCK`) is returned.
* **Middleware:** I wrote a custom FastAPI HTTP middleware to intercept all requests, measure `duration_ms`, generate a UUID for `request_id`, and output a structured JSON log.
* **Security:** Passwords are hashed using `bcrypt` (via `passlib`). I implemented a custom `RoleChecker` dependency in FastAPI to enforce the strict RBAC matrix on specific routes.

## How to Run the Project

### Prerequisites
Make sure you have **Python 3.9+** and **Docker** (with Docker Compose) installed on your machine.

### Step 1: Clone and Setup Environment
Navigate to the project directory, create a virtual environment, and install the dependencies:
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
pip install -r requirements.txt
```
### Step 2: Start the Database and Run Migrations
Use Docker Compose to spin up the PostgreSQL database in the background:
```bash
docker-compose up -d db
```
Once the database is running, apply the Flyway migrations to create the tables, enums, and triggers:
```bash
docker-compose up flyway
```
### Step 3: Generate Models
Generate the Pydantic schemas from the OpenAPI specification. (Ensure the script is executable):
```bash
chmod +x generate.sh
./generate.sh
```
### Step 4: Run the API Server
Start the FastAPI server using Uvicorn:
```bash
uvicorn src.main:app --reload
```
### Step 5: Explore and Test
Open your browser and navigate to the automatically generated Swagger UI:
http://127.0.0.1:8000/docs

Here you can test the entire E2E flow:

Register a new user ```(POST /auth/register)``` and log in ```(POST /auth/login)``` to get a JWT token.

Click the "Authorize" button at the top of the Swagger UI and paste your access_token.

Create products `````(POST /products)````` and manage your orders ```(POST /orders)```.