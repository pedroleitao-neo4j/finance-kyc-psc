# UK Corporate Governance & UBO Analysis with Neo4j

This project implements a high-throughput **KYC (Know Your Customer)** and **AML (Anti-Money Laundering)** knowledge graph pipeline using **UK Companies House** data.

By transitioning from flat, tabular registry data to a connected graph structure, this solution enables organizations to instantly traverse ownership structures, identify **Ultimate Beneficial Owners (UBOs)**, detect high-risk "Russian Doll" corporate structures, and visualize the geospatial footprint of the UK economy.

<p align="center">
  <img src="renderings/voting_rights_expanded_graph.png" alt="Voting Rights Expanded Graph"/>
  <br>
  <sub>The voting rights of an expanded network of control.</sub>
</p>

## The Business Background

Identifying the Ultimate Beneficial Owner (UBO) is a cornerstone of modern compliance frameworks across the financial, insurance, and risk management sectors. It is no longer sufficient to simply know the immediate legal entity acting as a customer; organizations must pierce the corporate veil to understand the individuals who truly profit from and control these entities. This requirement is driven by increasingly stringent global regulations aimed at curbing money laundering, terrorist financing, and tax evasion. Failing to accurately identify UBOs exposes institutions to severe reputational damage and regulatory fines, making robust due diligence not just a legal obligation but a critical component of operational risk management.

In this context, traditional relational databases often struggle to capture the complex, multi-layered ownership structures used to obscure true control. These systems typically rely on rigid table structures that make traversing deep hierarchies of shell companies and trusts computationally expensive and difficult to visualize. A graph-based approach, such as that offered by Neo4j, fundamentally changes this dynamic by treating companies, people, and their relationships as first-class citizens. By modeling data as a network of connected nodes, Neo4j allow analysts to instantly traverse intricate ownership chains and uncover hidden patterns of influence, such as circular ownership or "Russian Doll" structures, that would otherwise remain invisible in tabular data. This capability empowers compliance teams to conduct more effective investigations, reduce false positives, and proactively manage risk with greater speed and precision.

## The Stack

* **Neo4j Graph Database:** The core engine for storing and querying connected governance data.
* **Apache Spark (PySpark):** Used for high-performance ETL, capable of processing millions of company and officer records.
* **Neo4j Connector for Apache Spark:** Enables high-throughput data transfer between Spark DataFrames and Neo4j.
* **PyDeck:** Utilized for high-fidelity 3D geospatial visualizations (Heatmaps, ArcLayers, ColumnLayers).
* **pgeocode:** Used within Spark UDFs to batch-geocode UK postcodes into latitude/longitude coordinates.

## The Dataset

The pipeline consumes public data from **Companies House (UK)**. Specifically:

1. **Basic Company Data:** Foundational details (Name, Status, Incorporation Date, SIC Codes) for over 5 million companies.
2. **Persons with Significant Control (PSC):** A snapshot detailing the beneficial ownership structures, linking individuals and organizations to the companies they control.

## The Graph Model

The ETL pipeline transforms raw CSV and JSON data into a rich property graph. The schema allows for complex traversals regarding control, location, and industry classification.

<p align="center">
  <img src="renderings/schema_graph.png" alt="Schema Graph"/>
  <br>
  <sub>The schema of the property graph representing UK corporate governance.</sub>
</p>

### Nodes

* **`Company`**: Registered business entities.
* **`Person`**: Individual officers or beneficial owners (enriched with deterministic IDs based on name and DOB).
* **`Organization`**: Corporate entities that control other companies (e.g., holding companies).
* **`Address`**: Physical locations, enriched with Geolocation (Lat/Lon).
* **`Country`**: Jurisdictions of residence or origin.
* **`SICCode`**: Standard Industrial Classification codes representing economic activity.
* **`CompanyStatus`**: E.g., "Active", "Liquidation", "Dissolved".
* **`PreviousName`**: Historical names of companies, linked to track identity changes over time.

### Relationships

* `(:Person|:Organization)-[:CONTROLS]->(:Company)`: Represents beneficial ownership. Enriched with parsed voting rights and share percentages (e.g., `voting_rights_min: 25`, `voting_rights_max: 50`).
* `(:Company)-[:REGISTERED_AT]->(:Address)`: Physical footprint of the business.
* `(:Person)-[:LIVES_AT]->(:Address)`: Residence of the controller.
* `(:Company)-[:HAS_SIC]->(:SICCode)`: Classification of business activity.
* `(:Company)-[:HAS_PREVIOUS_NAME]->(:PreviousName)`: Tracks corporate history.

<p align="center">
  <img src="renderings/unified_outflows.png" alt="Unified Control Outflows"/>
  <br>
  <sub>Visualization of control outflows of UK companies.</sub>
</p>

## Notebooks

The project is divided into three primary notebooks, handling data ingestion, graph analysis, and geospatial intelligence.

### 1. Data Loader & ETL ([`loader.ipynb`](loader.ipynb))

A robust PySpark pipeline designed to ingest and clean UK corporate registry data.

* **Batch Geocoding:** Implements Pandas UDFs to convert UK postcodes to coordinates efficiently.
* **Control Parsing:** Converts unstructured strings (e.g., *"voting-rights-25-to-50-percent"*) into queryable numerical ranges.
* **Entity Resolution:** Generates deterministic unique IDs (MD5 hashes) for Persons and Addresses to prevent duplication.
* **Schema Enforcement:** Applies Neo4j constraints to ensure data integrity before loading.

### 2. UBO & Control Analysis ([`ubo.ipynb`](ubo.ipynb))

Demonstrates how to query the graph to identify beneficial owners and complex control structures.

* **Majority Indirect Control:** Identifies active control relationships where voting rights exceed specific thresholds.
* **Visualizing Control Strength:** Uses graph visualization to color-code and resize edges based on the percentage of shares or voting rights.
* **Indirect Control Inference:** Detects "Bridge" structures where a person controls a target company via an intermediary organization.
* **"Russian Doll" Structures:** Recursive queries that uncover deep, multi-layered ownership chains often used to obscure true ownership.

### 3. Geospatial Intelligence ([`geo.ipynb`](geo.ipynb))

Leverages the spatial data enriched during the loading phase to visualize economic patterns.

* **Company Density:** 3D Column maps visualizing the concentration of registered businesses across the UK.
* **Liquidation Heatmaps:** Identifies geographic clusters of business failures and potential "phoenixing" hotspots.
* **Industry Clustering:** Maps dominant industries (via SIC codes) to specific locations (e.g., Tech in Shoreditch vs. Finance in Canary Wharf).
* **Cross-Border Outflows:** Visualizes ownership flows from UK assets to foreign jurisdictions, highlighting connections to tax havens via 3D ArcLayers.

<p align="center">
  <img src="renderings/finance_choropleth_dynamic.png" alt="Finance Choropleth"/>
  <br>
  <sub>Dynamic visualization of the density of financial industry businesses across London.</sub>
</p>

## Setup & Configuration

### Prerequisites

* Python 3.10+
* Neo4j Database (AuraDB or Desktop)
* Apache Spark (local or cluster)

### Environment Variables

Create a `.env` file in the root directory with the following credentials:

```bash
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=neo4j
DATA_PATH=./data
COMPANIES_URL=http://download.companieshouse.gov.uk/BasicCompanyDataAsOneFile-202X-XX-XX.zip
PSC_URL=http://download.companieshouse.gov.uk/psc-snapshot-202X-XX-XX.zip

```

### Installation

Install the required dependencies:

```bash
pip install pyspark neo4j pgeocode pydeck pandas python-dotenv
```

Or use the provided `environment.yml` for a Conda environment:

```bash
conda env create -f environment.yml
```

### Running the Pipeline

1. Run [`loader.ipynb`](loader.ipynb) to download data, process it with Spark, and populate the Neo4j graph.
2. Run [`ubo.ipynb`](ubo.ipynb) to perform graph traversals and identify beneficial owners.
3. Run [`geo.ipynb`](geo.ipynb) to generate geospatial visualizations.