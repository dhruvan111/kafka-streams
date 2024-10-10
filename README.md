# Kafka Streams Data Aggregation Project

This project is built using Kafka Streams' **Processor API** to process and aggregate streaming data. The system accumulates incoming data until a specified threshold is met and then sends the data to an output topic for further consumption. This Kafka-based solution is designed to be flexible, scalable, and ready for integration with other services or users.

## Features
- **Processor API**: Built using Kafka Streams' Processor API, which allows fine-grained control over the processing logic and topology.
- **Threshold-Based Data Aggregation**: Incoming data is aggregated until a predefined threshold (set by the user) is reached.
- **Output to Kafka Topic**: Once the threshold is met, the accumulated data is sent to a designated Kafka topic (`OUTPUT_TOPIC`).
- **Real-Time Processing**: The system processes data in real-time, allowing it to be consumed by external systems or users directly.

## Table of Contents
- [Architecture](#architecture)
- [Setup and Installation](#setup-and-installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Architecture
The project leverages Kafka Streams' **Processor API** to manually define the data processing topology. The key logic includes:
1. **Data Ingestion**: Streaming data is ingested from a Kafka `INPUT_TOPIC`.
2. **Data Aggregation**: The incoming records are accumulated until the set threshold is met.
3. **Threshold Evaluation**: Once the threshold is reached, the aggregated data is emitted to the `OUTPUT_TOPIC`.
4. **Output for Consumption**: The data in the `OUTPUT_TOPIC` is ready to be consumed by external users or third-party services.

The entire flow is managed through a custom topology defined using the Processor API.

## Setup and Installation

### Prerequisites
- **Kafka**: Make sure you have Kafka installed and running.
- **Java 8+**: This project is built with Java, so ensure you have a compatible version installed.
- **Gradle**: The project uses Gradle for dependency management.

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/dhruvan111/kafka-streams.git
   cd kafka-streams
