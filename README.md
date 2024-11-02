# grid-scale

Highly scalable time-series database in a package!!

Time-series data is a sequence of data points collected or recorded at specific time intervals. It is used to track changes over time and is commonly used in various fields such as finance, healthcare, environmental monitoring, Industrial IOT and more. 

In the context of grid-scale, time-series data is managed with high efficiency and scalability. The database is designed to handle large volumes of time-series data by distributing the query load and supporting high-speed data ingestion. The custom schema allows for optimized storage and retrieval, ensuring that the system remains cost-effective while maintaining high performance.

## Features

- **High Scalability**: Efficiently handles large volumes of time-series data with custom schema.
- **Query Load Distribution**: Distributes query load to maintain high performance.
- **High Speed Ingestion**: Supports rapid data ingestion with high concurrency.
- **Cost-Effective**: Designed to be budget-friendly while maintaining performance.

### Concept
Let's start with an example of ledger record which can also be treated as time-series data.

User `U1` bought `company1` shares of `1000` qty at $`21.5`on `01-Nov-2024`

Now it does depend on how you model your data to store this information, but typically Time and User are 2 important dimensions here.
Herein the User `U1` is mapped to `Tag` dimension and `Time` is mapped to `Time` dimension and the rest of the bits are called value in context of this data-base

Key aspects of time-series data in grid-scale:
- **Temporal Ordering**: Data points are stored in the order they are collected, making it easy to analyze trends over time.
- **Efficient Storage**: Data is broken into small chunks and stored in a hierarchical directory structure, allowing for efficient space utilization and easy access.
- **Scalability**: The system can scale horizontally by adding more nodes to handle increased data volume and query load.
- **High Availability**: Multiple replicas of data ensure that the system remains available even in case of node failures.

Overall, grid-scale provides a robust solution for managing time-series data, ensuring that it can handle the demands of modern applications with ease.

## Getting Started

### Prerequisites

- Node.js v20 and above.
- Redis
- SQLite

### Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/grid-scale.git
    ```
2. Navigate to the project directory:
    ```sh
    cd grid-scale
    ```
3. Install dependencies:
    ```sh
    npm install
    ```
4. Run :
    ```sh
    <!-- For reader rest service -->
    npm run build && npm run start-rr 
    <!-- For writer rest service -->
    npm run build && npm run start-wr

    <!-- For reader cli service -->
    npm run build && npm run start-rc
    <!-- For writer cli service -->
    npm run build && npm run start-wc
    ```
### Usage

1. Use examples/cli folder to examples in cli
2. Use examples/rest folder for a microservice kind of approach.

## Built with

1. Authors :heart: for Open Source.

## Contributions

1. New ideas/techniques are welcomed.
2. Raise a Pull Request.

## License

This project is contribution to public domain under license specified, view [LICENSE.md](/LICENSE) file for details.


## F.A.Q

### What dimensions does this scale on?

1. **Space**: High resolution time-series data is every growing and needs cheap scale-able space, The idea is to save data on files and move them to a cheap storage when not used.
2. **Latency**: Idea is to be H-scalable design when more and more clients connects in cost effective way, this happens by having N read replica's of the data.
3. **Concurrency**: This again is possible via H-scaling and data immutability
Point 2 & 3 would need a auto scale system like k8s to leverage best out of the system

### Is there a Hard dependency on SQLite?

No, Grid scale is a partitioning algorithm thats the core concept, SQLite was chosen cause it has some good qualities which makes h-scaling easy. most and every part of this library can be customized according to one's need eg if you want to change it to postgres you will need plugin extends `ChunkBase` abstract class.

### Why do we have one table per tag design?

Data is normally read in ranges for given tags eg Tag1 from 2010 -> 2024, if a table has mixed tags then a page read from disk will have filter out lot of records ie: efficiency per disk IO drops, to improve this and gain more efficiency of sequential page reads this model is adopted.(its similar to how we will try to place data in cpu Lx cache to increase compute).

### What is the maximum size of tag name and encoding?

Maximum size of tag name is 255 UTF-8 characters.

### How is data layed-out on disk?

Data is broken up into small chunks according to the grid-scale algo and pushed into individual files and directories.
All files(*.db) are sqlite files.
Following is the directory hierarchy/structure
1. **data** : [Folder] Logical folder to gather everything under one folder, helps with mounting and readers.
2. **scaled-set(n)** : [Folder] Logical folder, when a writer scales beyond capacity of attached disks under this folder.

    - For scenario's where IOPS and Latency both needs to be scaled.
    - To keep adding space to your cluster.
    - To introduce cheap storage for data which is rarely being used.

3. **disk(n)** : [Folder] Attached multiple disks, helps to spread IOPS, structure cannot be changed once config is active for entire deployment lifecycle.
4. **D(|*|*|)** : [Folder] Logical units aka cells of the grid system which helps in storing data in chunks.
5. **(*).db** : [File] The actual sqlite file individual writer will makes its own file for H-Scaling.


### Things than can be further improved?

1. **Hash Algo**: A function which takes in variable length string input capped at 255 characters and return an integer/big-integer output such that 
    - Every string generates a unique integer output
    - Should be repeatable for same input same output
    - Should be sequential i.e: spread factor of 1.
    - Should be stateless, cannot build a LUT or similar table memory.
    - Should be fast.
    - Collision's are fine.

2. **Data Fragmentation**: H-Scaling a data structure means to spread data across multiple chunks, but it brings data fragmentation problem which means to read data the reader may have to iterate through multiple chunks, thus adding time and complexity at the readers end and increasing read-latency.
