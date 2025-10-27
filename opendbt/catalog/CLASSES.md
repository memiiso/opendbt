# Documentation for the `opendbt.opendbt.catalog` module

This document summarizes the classes defined in `__init__.py`, explaining their responsibilities, main attributes, and how they relate within the column lineage catalog generation flow.

## Overview of relationships

```
OpenDbtCatalog
└── OpenDbtNode*
    ├── OpenDbtTableRef
    └── OpenDbtColumn*
        ├── transformations
        └── depends_on → OpenDbtColumn (from other OpenDbtNode)
```

* `OpenDbtCatalog` serves as the entry point: it loads the dbt manifest and catalog, instantiates `OpenDbtNode` for each relevant resource, and coordinates column-lineage computation.
* Each `OpenDbtNode` represents a dbt resource (model, seed, snapshot, or source) and aggregates:

  * An `OpenDbtTableRef`, used to build fully qualified names (FQN).
  * A set of `OpenDbtColumn` objects, which store metadata, types, transformations, and column-to-column dependencies.
* `Dialect` is an auxiliary enumerator used by both `OpenDbtTableRef` and `OpenDbtNode` to adjust behavior depending on the SQL engine.

## `Dialect`

Enum listing supported SQL dialects (`postgres`, `snowflake`, `bigquery`, etc.).

* **Responsibility:** provide a canonical set of values to identify the adapter in use.
* **Usage:** `OpenDbtTableRef` checks if the dialect is `starrocks` to decide whether to include the database name in the FQN. `OpenDbtNode` derives the dialect directly from the manifest and passes it to SQLGlot utilities.

## `OpenDbtTableRef`

Immutable dataclass encapsulating a table’s identity.

* **Attributes:** `database`, `schema`, `table`, `dialect`.
* **Main methods:**

  * `table_fqn()`: returns the fully qualified name, respecting dialect-specific nuances.
  * `_generate_db_structure()`: builds the base dictionary (database/schema) used in the FQN.
* **Relationships:**

  * Instantiated by `OpenDbtNode` when loading each resource.
  * Used by `OpenDbtColumn` to generate `column_fqn` and `table_fqn`.
  * Referenced in dependency objects (`depends_on`) to trace a column’s origin.

## `OpenDbtColumn`

Simple class representing a catalog column with extended metadata.

* **Initialization:** receives an `OpenDbtTableRef` and a raw dictionary from the manifest/catalog. During construction:

  * Normalizes `name` and `type`.
  * Computes `column_fqn`, `table_fqn`, and `table_relative_fqn`.
  * Initializes `transformations` and `depends_on` lists.
* **Exposed properties:** `name`, `type`, `transformations`, `depends_on`.
* **List usage:**

  * `transformations`: SQL fragments (strings) describing how the column was derived.
  * `depends_on`: list of `OpenDbtColumn` instances that serve as data sources (each may include a `model_id` when mapped to another node).
* **Relationships:** created and managed exclusively by `OpenDbtNode`; exported to the final JSON by `OpenDbtCatalog`.

## `OpenDbtNode`

Subclass of `OpenDbtLogger` that encapsulates a dbt resource enriched with catalog data.

* **Construction:** receives `manifest_node`, `catalog_node`, and `dialect`.

  * Merges metadata and stats.
  * Creates an `OpenDbtTableRef` and a dictionary of `OpenDbtColumn` objects.
  * Stores `parent_nodes` to facilitate later compositions.
* **Main methods/properties:**

  * `columns`: mapping `name → OpenDbtColumn`.
  * `depends_on`: list of parent IDs (from the manifest).
  * `populate_lineage(tables2nodes)`: uses SQLGlot to compute column-to-column lineage, filling in `transformations` and `depends_on`.
  * `db_schema_dict()` / `parent_db_schema_dict()`: builds SQLGlot-compatible structures to resolve names in complex queries.
  * `compiled_code`: retrieves compiled SQL (ignores Python models).
* **Relationships:**

  * Created by `OpenDbtCatalog` for each relevant node/source.
  * Bridges manifest dependencies (`depends_on`) and actual instances (`parent_nodes`), stored in `OpenDbtCatalog.nodes`.
  * Converts table IDs (`tables2nodes`) into real references during lineage population.

## `OpenDbtCatalog`

Orchestrator responsible for merging the manifest and catalog and producing the enriched JSON (`catalogl.json`).

* **Initialization:** loads JSON files, stores `catalog_file` (output), and keeps the default dialect.
* **Main methods:**

  * `nodes`: builds (once) all `OpenDbtNode` and source instances and caches them.

    * Updates each node’s `parent_nodes` based on `depends_on`.
  * `tables2nodes`: mapping `table_fqn → node_id` used during lineage computation.
  * `table(fqn)` / `node(id)`: convenient accessors to loaded structures.
  * `export()`: iterates over all `OpenDbtNode` instances, calls `populate_lineage`, filters relevant fields, and writes the final JSON.
* **Relationships:**

  * The only class that instantiates `OpenDbtNode`.
  * Provides SQLGlot schema context via node methods.
  * Uses `tables2nodes` to link derived columns across tables/models.

## Summary flow

1. `OpenDbtCatalog` reads the input files and builds `OpenDbtNode` objects (models and sources).
2. For each node, `OpenDbtNode`:

   * Constructs an `OpenDbtTableRef`.
   * Prepares `OpenDbtColumn` objects for all known columns.
   * Expands `parent_nodes` based on `depends_on`.
3. `OpenDbtCatalog.export()` calls `populate_lineage` on each node:

   * SQLGlot parses the SQL (`compiled_code`) and returns a dependency tree.
   * Each `OpenDbtColumn` receives its transformations and references to source columns (new instances linked via `tables2nodes`).
4. The final catalog (`catalogl.json`) aggregates metadata, stats, columns, and computed lineage.
