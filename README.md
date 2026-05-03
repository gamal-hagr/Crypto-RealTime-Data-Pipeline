🚀 Crypto Real-Time Data Pipeline: Ingestion Phase
This repository contains the Data Ingestion Layer for a real-time cryptocurrency data engineering project. The system streams live aggregate trade data from the Binance Exchange directly into Microsoft Azure Event Hubs.

📌 Project Overview
The goal of this phase is to establish a high-frequency data producer that captures market movements and ensures a steady flow of "Bronze Layer" (raw) data into the cloud for downstream processing.

Key Features:
Real-Time Streaming: Utilizing WebSockets for low-latency data capture.

Resilient Connectivity: Implements exponential backoff and auto-reconnection logic.

Optimized Throughput: Uses Event Hubs batching to minimize API calls and cloud costs.

Comprehensive Logging: Full tracking of system health and transmission status.

🏗️ Architecture
Source: Binance WebSocket API (wss://stream.binance.com).

Producer: Python script running websocket-client and azure-eventhub SDK.

Ingestion Point: Azure Event Hubs (Standard Tier) acting as the message broker.

🛠️ Technical Stack
Language: Python 3.x

Cloud: Microsoft Azure

Libraries:

websocket-client: For persistent socket connections.

azure-eventhub: For high-scale telemetry ingestion.

logging: For operational monitoring.

⚙️ Configuration & Setup
Prerequisites
Install the required dependencies:

Bash
pip install websocket-client azure-eventhub
Environment Variables
Update the following constants in producer.py with your Azure credentials:

CONNECTION_STR: Your Event Hub Namespace connection string.

EVENT_HUB_NAME: The specific Hub/Topic name (e.g., crypto-raw-stream).

📝 Execution
To start the ingestion pipeline, run:

Bash
python producer.py
Upon execution, the console will display live trade summaries:
Captured: BTCUSDT | Price: 62450.50 | Quantity: 0.0012

⚠️ Challenges Overcome
Permission Management: Resolved Azure RBAC conflicts by correctly configuring "Contributor" roles.

Connection Stability: Implemented a while True loop with a 5-second retry delay to handle network fluctuations.

Data Consistency: Ensured JSON payloads are properly formatted for the Spark "Silver Layer" processing.

👥 Contributors
Gamal Hagr: Team Leader & Lead Data Engineer (Producer Logic).

Magda: Cloud Infrastructure & Security (Azure Provisioning).
