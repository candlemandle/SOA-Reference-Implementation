# Marketplace Architecture

## 1. Context
I designed a scalable architecture for a Marketplace system where sellers list items and users make purchases. The primary goals were high availability, fault isolation, and independent scalability of components.

## 2. C4 Diagram (Container Level)
I used the **Microservices pattern** to decompose the system.

![C4 Diagram](C4_Diagram.png)
*(The diagram is available in the file C4_Diagram.png)*

## 3. Domain Decomposition & Responsibilities
To ensure loose coupling, I divided the system into distinct business domains and mapped them to specific services.

| Domain | Service | Responsibilities |
|--------|---------|------------------|
| **User Domain** | `Auth Service` | User registration, authentication, JWT issuance, profile management. |
| **Catalog Domain** | `Catalog Service` | Managing product listings, categories, and search functionality. |
| **Order Domain** | `Order Service` | Cart management, order placement, order lifecycle tracking. |
| **Financial Domain** | `Payment Service` | Processing transactions, integration with payment gateways, billing. |
| **Engagement Domain** | `Recommendation Service` | Generating personalized product feeds using ML models. |
| **Notification Domain** | `Notification Service` | Sending transactional emails and push notifications. |

### Data Boundaries
I strictly applied the **Database-per-Service** pattern to avoid tight coupling:
* **Auth DB:** Stores sensitive user credentials (hashed). Owned strictly by `Auth Service`.
* **Catalog DB:** Stores product data (NoSQL structure for flexibility). Owned by `Catalog Service`.
* **Order DB:** Stores transactional data (ACID compliant). Owned by `Order Service`.

**Communication Strategy:**
* **Synchronous (REST/gRPC):** For direct user requests (e.g., "Get Item Details").
* **Asynchronous (Kafka/RabbitMQ):** For decoupling services (e.g., "Order Created" event -> triggers `Notification Service`).

## 4. Architecture Alternatives & Trade-offs

I considered two architectural styles for this system before making a decision.

### Option A: Modular Monolith
A single deployable unit where domains are separated by code modules.

* **Pros:** Easier to deploy initially, simpler debugging, zero network latency between calls.
* **Cons:** The entire system fails if one module leaks memory; difficult to scale only the "Catalog" module during sales events; technology lock-in (single language).

### Option B: Microservices (The one I went with)
Independent services running in separate containers.

* **Pros:**
    * **Independent Scaling:** I can scale `Catalog Service` x10 during Black Friday without touching `Auth Service`.
    * **Fault Isolation:** If `Notification Service` fails, users can still complete purchases.
    * **Tech Freedom:** I can use Python for ML (Recommendations) and Go/Java for high-load components.
* **Cons:** Complexity in deployment (Docker/K8s required), eventual consistency challenges.

### 5. Final Decision
I chose **Option B (Microservices)** because the requirements imply:
1.  **Personalized Feed:** This requires heavy ML computation, which I isolated in a separate service (`Recommendation Service`) so it doesn't degrade the performance of the main API.
2.  **High Load on Catalog:** Marketplace traffic is predominantly read-heavy. Microservices allow me to cache and scale the Catalog independently from the Transactional logic.

---

## 6. Service Implementation 
For this assignment, I implemented the **Notification Service** using Python (FastAPI) and Docker.

### How to Run
1. Navigate to the task directory:
   ```bash
   cd "Task 1"
   ```
2. Start the container:
   ```
   docker compose up --build
   ```
3. Check Health Endpoint:
   Open http://localhost:8000/health.
   Expected Output:
   ```
   {"status": "OK", "service": "Notification Service"}
   ```