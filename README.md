# Production Data Pipeline Automation

## Overview

This repository demonstrates a production-style automated data pipeline built with Python, designed around safety, observability, and controlled data promotion.

The pipeline follows a staging-first approach commonly used in enterprise and regulated environments, where incoming data is fully validated before being promoted to a live table. The design prioritises reliability, recoverability, and clarity of failure over raw throughput.

This project is intended as a reference implementation for Automation Engineer, DevOps Engineer, and Platform Engineer roles.

## Key Design Principles

- Staging-first architecture  
  All data is loaded into a staging table and validated before promotion to live.

- Defensive automation  
  The pipeline aborts safely on validation failures, duplicate detection, or reconciliation mismatches.

- Operational transparency  
  Structured logging and explicit error propagation make failures easy to diagnose.

- Deterministic recovery  
  Staging data is retained to support manual re-promotion without reprocessing the external source.

## High-Level Flow

External Source (CSV)
↓  
Download and validation  
↓  
Data cleansing  
↓  
Fiscal context resolution  
↓  
Duplicate detection (live)  
↓  
Load to staging  
↓  
Staging to source reconciliation  
↓  
Promote to live  
↓  
Success or failure notification

## Why This Approach

In many enterprise environments:

- Distributed transactions are unavailable or restricted  
- Database journaling or rollback is not enabled  
- External data sources are non-idempotent or unreliable  

This pipeline is designed to embrace those constraints by using structural safeguards rather than assuming perfect transactional guarantees.

## Features

- External data ingestion via HTTP  
- CSV validation and cleansing  
- Fiscal period resolution via database lookup  
- Duplicate load prevention per fiscal period  
- Row-count and data reconciliation checks  
- Database-native staging-to-live promotion  
- Structured logging and notification hooks  

## Technologies Used

- Python for batch orchestration  
- JDBC and JayDeBeAPI for database connectivity  
- Pandas for dataset reconciliation  
- Relational database (vendor-agnostic design)  
- SMTP integration for success and failure notifications  

All identifiers, schemas, and endpoints are anonymised for public reference.

## Project Structure

.
├── etl_pipeline.py
├── SMTP_helper.py
├── README.md
└── logs/

## Configuration

All sensitive values such as credentials, URLs, and filesystem paths are supplied via environment variables or documented placeholders.

No secrets or proprietary identifiers are included in this repository.


## Disclaimer

This repository contains anonymised example code intended to demonstrate architectural and automation patterns. It does not represent a complete production system.

## Author Notes

This project reflects real-world experience designing automation under non-ideal constraints, where safety, clarity, and recoverability matter more than theoretical elegance.
