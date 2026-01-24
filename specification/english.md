# Project Technical Specification

## Big Data Systems Architecture

## Goal

The goal of this course project is to design and implement a big data processing system architecture, as well as to demonstrate the use of such a system through various examples of data transformation, analysis, and presentation.

## Project Description

### Datasets

- At least two datasets from different sources must be provided.
- One of these two datasets is considered primary and should contain historical data on a given/chosen topic.
- The primary dataset should be larger than 300 MB and can be downloaded from public data repositories (data.gov, data.worldbank.org, data.gov.rs, kaggle.com, datasetsearch.research.google.com) or can be collected using techniques such as web scraping.
- The second dataset should have the characteristics of a data stream that is logically connected to the topic of the primary dataset in some way.
- The second dataset (stream) can be created using public APIs (e.g., via WebSocket), by periodically fetching data, or by generating a data stream from an existing (historical) dataset that has a temporal dimension.
- It is important that these two datasets originate from different sources; it is not allowed to use the same initial dataset to create both the primary dataset and the data stream.

### Data Lake

- A data lake with a minimum of 3 zones (layers) needs to be designed and implemented:
  - raw zone,
  - transformation zone,
  - curated zone.
- The loading of the selected dataset into the data lake needs to be automated.

### Data Processing and Presentation of Processing Results

- A meaningful data analysis needs to be designed, which should yield useful insights from the selected dataset.
  - For this purpose, design one or two personas representing stakeholders in the data analysis process; view the relevance and meaningfulness of the defined data analysis through the lens of these personas.
- The processing results must be visualized for the end user.

---

## Tasks

### Project Specification (KT1) - Dataset and Desired Processing Presentation

- describe the domain, motivation, goals, and
- specify concrete questions that the data analysis should answer
  - at least 10 questions for batch data processing and
  - at least 5 questions for real-time data processing.

### Initial Architecture Setup (KT2) - Diagram and Containerized Modules

- data lake definition and
- specification of modules that will be used for the desired data processing
  - provide a diagrammatic representation of the entire system architecture and
  - prepare components for use in containerized form
    - all architecture components need to be containerized,
- use the prepared system to implement the answer to at least one question posed for batch data processing

### Data Processing

#### Batch Processing

- at least 10 different complex queries/transformations on data from the data lake need to be implemented
  - for this purpose, use one of the tools that enable parallel processing of large amounts of data for data preparation,
- analytical window functions need to be used and
- at least 3 query/transformation results need to be presented using a visualization technology of choice.

#### Real-time Processing / Stream Data Processing

- up to 5 complex stream data transformations (stream processors) need to be implemented,
- stream joining or joining a stream with batch-type data needs to be used, as well as aggregation using Windowing and
- the stream processing results should be stored in a storage/database of choice (e.g., Kudu, Druid, ElasticSearch, Citus).

#### Data Processing Orchestration

- mechanisms for automated triggering of data processing processes need to be provided.

#### Public Git Repository with README

- send the repository link to the responsible teaching assistant BEFORE the project defense.
