# Project Zero Mongo DB Sync

This project is designed to synchronize data between different MongoDB instances. The synchronization process ensures that data is consistent across all instances.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Docker Setup](#docker-setup)
- [Configuration](#configuration)
- [Contributing](#contributing)
- [License](#license)

## Prerequisites

Before you begin, ensure you have met the following requirements:

- You have installed [Docker](https://www.docker.com/get-started)
- You have a MongoDB instance running

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/project_zero_mongo_db_sync.git
    cd project_zero_mongo_db_sync
    ```

2. Install the dependencies:
    ```sh
    npm install
    ```

## Usage

To start the synchronization process, run the following command:
```sh
npm start
```

## Docker Setup

To set up and run the project using Docker, follow these steps:

1. Build the Docker image:
    ```sh
    docker build -t project_zero_mongo_db_sync .
    ```

2. Run the Docker container:
    ```sh
    docker run -d -p 3000:3000 --name mongo_sync project_zero_mongo_db_sync
    ```

3. Verify that the container is running:
    ```sh
    docker ps
    ```

## Configuration

You can configure the synchronization settings by editing the `config.json` file. The configuration file includes settings for source and destination MongoDB instances, synchronization intervals, and other parameters.

## Contributing

Contributions are always welcome! Please read the [contributing guidelines](CONTRIBUTING.md) first.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.