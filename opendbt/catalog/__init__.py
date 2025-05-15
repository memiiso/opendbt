import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import sqlglot
import tqdm
from sqlglot import Expression
from sqlglot.lineage import lineage, SqlglotError, exp

from opendbt.logger import OpenDbtLogger
from opendbt.utils import Utils


@dataclass(frozen=True)
class OpenDbtTableRef:
    database: str
    schema: str
    table: str

    def __str__(self) -> str:
        return self.table_fqn()

    def table_fqn(self) -> str:
        return ".".join([self.database, self.schema, self.table])


class OpenDbtColumn:
    """Represents a column within a dbt node, including lineage info."""

    def __init__(self, table_ref: OpenDbtTableRef, data: dict):
        self.table_ref: OpenDbtTableRef = table_ref
        self.data: dict = data or {}
        self.data["name"] = self.data["name"] if "name" in self.data else "unknown-column"
        self.data["type"] = self.data["type"] if "type" in self.data else "unknown"
        self.data["column_fqn"] = f"{self.table_ref.table_fqn()}.{self.name}".lower()
        self.data["table_fqn"] = self.table_ref.table_fqn().lower()
        self.data["table_relative_fqn"] = f"{self.table_ref.schema}.{self.table_ref.table}"
        self.data["transformations"] = []
        self.data["depends_on"] = []

    @property
    def transformations(self):
        return self.data["transformations"]

    @property
    def depends_on(self):
        return self.data["depends_on"]

    # @property
    # def column_fqn(self):
    #     return self.data["column_fqn"]

    @property
    def type(self):
        return self.data["type"]

    @property
    def name(self):
        return self.data["name"]

    def to_dict(self):
        return self.data


class OpenDbtNode(OpenDbtLogger):
    def __init__(self, manifest_node: dict, catalog_node: dict, dialect: str):
        self.node: dict = manifest_node
        # Enrich node with catalog information
        self.node["metadata"] = Utils.merge_dicts(dict1=self.node.get("metadata", {}),
                                                  dict2=catalog_node.get("metadata", {}))
        self.node["stats"] = Utils.merge_dicts(dict1=self.node.get("stats", {}),
                                               dict2=catalog_node.get("stats", {}))
        self.table_ref = OpenDbtTableRef(database=self.node.get("database", "$database"),
                                         schema=self.node.get("schema", "$schema"),
                                         table=self.node.get("name", "$name"))
        self.node["columns"]: Dict[str, OpenDbtColumn] = self.__columns(
            catalog_cols=catalog_node.get("columns", {})
        )
        self.dialect = dialect
        self.parent_nodes: Dict[str, OpenDbtNode] = {}

    def __columns(self, catalog_cols: dict) -> Dict[str, OpenDbtColumn]:
        combined = Utils.merge_dicts(dict1=self.node.get("columns", {}),
                                     dict2=catalog_cols.get("columns", {}))
        cols = {}
        for col_name, col_data in combined.items():
            col_name: str
            cols[col_name.strip().lower()] = OpenDbtColumn(table_ref=self.table_ref, data=col_data)
        return cols

    def to_dict(self):
        return self.node

    @property
    def columns(self) -> Dict[str, OpenDbtColumn]:
        return self.node.get("columns", {})

    @property
    def depends_on(self) -> list:
        return self.node.get("depends_on", {}).get("nodes", [])

    def column_types(self):
        return {name: data.type for name, data in self.columns.items()}

    @property
    def unique_id(self):
        return self.node['unique_id']

    @property
    def resource_type(self):
        return self.node['resource_type']

    @property
    def table_fqn(self):
        return self.table_ref.table_fqn()

    @property
    def is_python_model(self) -> bool:
        return self.node.get("language") == "python"

    @property
    def compiled_code(self) -> Optional[str]:
        """
        Returns the compiled SQL code for this node, if available.
        Returns None if the node is not a model, is a Python model,
        or has no compiled code defined in the manifest.
        """
        if self.resource_type != "model" or self.is_python_model:
            return None

        return self.node.get("compiled_code", None)

    @property
    def column_names(self) -> list[str]:
        return [item.name.lower() for item in self.columns.values()]

    def _sqlglot_column_ref(self, node):
        if node.source.key != "table":
            raise ValueError(f"Node source is not a table, but {node.source.key}")
        column_name = node.name.split(".")[-1].lower()
        table_ref = OpenDbtTableRef(database=node.source.catalog, schema=node.source.db, table=node.source.name)

        return OpenDbtColumn(table_ref=table_ref, data={"name": column_name})

    def populate_lineage(self, tables2nodes: dict):
        """
        Calculates the column-level lineage for the node.

        Returns:
            dict: A dictionary where keys are lowercase column names of the current node,
                  and values are lists of dictionaries. Each inner dictionary represents
                  a source column and contains:
                  - 'parent_column': The name of the source column (lowercase).
                  - 'parent_table': An OpenDbtTableRef object for the source table.
                  - 'transformation': A string containing the SQL expression that
                                      transforms the source(s) into the target column.
        """
        sqlglot_column_lineage_map = self.sqlglot_column_lineage_map()
        # pylint: disable=too-many-nested-blocks
        for column_name, node in sqlglot_column_lineage_map.items():
            column_name = column_name.strip().lower()
            if column_name not in self.columns:
                self.columns[column_name] = OpenDbtColumn(table_ref=self.table_ref, data={"name": column_name})

            # Handle cases where lineage couldn't be determined or returned an unexpected format.
            # sqlglot.lineage might return an empty list or other non-Node types on failure/no lineage.
            if not node or not isinstance(node, sqlglot.lineage.Node):
                self.log.debug(
                    f"No lineage node or invalid format for column '{column_name}' in model '{self.unique_id}'.")
                continue  # Skip to the next column

            try:
                for n in node.walk():
                    try:
                        transf_sql = n.expression.sql(dialect=self.dialect)
                        if transf_sql:
                            self.columns[column_name].transformations.append(transf_sql)
                    except:
                        pass
                    if n.source and isinstance(n.source, exp.Table):  # More specific check
                        try:
                            parent_column = self._sqlglot_column_ref(n)
                            parent_model_id = tables2nodes.get(parent_column.table_ref.table_fqn().strip().lower(),
                                                               None)
                            if parent_model_id:
                                parent_column.data["model_id"] = parent_model_id
                            self.columns[column_name].depends_on.append(parent_column)
                        except Exception as e:
                            self.log.error(
                                f"Unexpected error processing lineage source node for column "
                                f"'{column_name}' in model '{self.unique_id}': {e}"
                            )
            except Exception as e:
                self.log.warning(
                    f"Error walking lineage tree for column '{column_name}' in model '{self.unique_id}': {e}")

            if not self.columns[column_name].depends_on:
                self.log.debug(
                    f"No source table/column found during lineage walk for column '{column_name}' "
                    f"in model '{self.unique_id}'."
                )
            if self.columns[column_name].transformations:
                self.columns[column_name].transformations.reverse()
        return self.columns

    def sqlglot_column_lineage_map(self):
        if not self.compiled_code:
            self.log.warning(f"Compiled code not found for model {self.unique_id}")
            return {}

        selected_columns = self.column_names
        lineage_data = {}
        if not selected_columns:
            try:
                sql: Expression = sqlglot.parse_one(sql=self.compiled_code, dialect=self.dialect)
                selected_columns = []
                for column in sql.expressions:
                    if isinstance(column, (exp.Column, exp.Alias)):
                        selected_columns.append(column.alias_or_name.lower())

            except Exception as e:
                self.log.warning(f"Error parsing SQL for model {self.unique_id}: {str(e)}")
                return {}

        for column_name in selected_columns:
            lineage_data[column_name] = []
            try:
                sqlglot_lineage = lineage(column=column_name,
                                          sql=self.compiled_code,
                                          schema=self.db_schema_dict(),
                                          dialect=self.dialect)
                lineage_data[column_name] = sqlglot_lineage
            except SqlglotError as e:
                self.log.warning(f"Error processing model {self.unique_id}, column {column_name}: {e}")
            except Exception as e:
                self.log.debug(f"Unexpected error processing model {self.unique_id}, column {column_name}: {e}")

        return lineage_data

    def parent_db_schema_dict(self):
        db_structure = {}
        for parent_key, parent_node in self.parent_nodes.items():
            parent_db_schema_dict = parent_node.db_schema_dict(include_parents=False)
            db_structure = Utils.merge_dicts(db_structure, parent_db_schema_dict)

        return db_structure

    def db_schema_dict(self, include_parents=True) -> dict:
        db_structure = {}
        db, schema, table = self.table_ref.database, self.table_ref.schema, self.table_ref.table
        db_structure[db] = {}
        db_structure[db][schema] = {}
        db_structure[db][schema][table] = {}
        for col_name, col in self.columns.items():
            db_structure[db][schema][table][col_name] = col.type

        if include_parents is False:
            return db_structure

        parent_db_schema_dict = self.parent_db_schema_dict()
        db_structure = Utils.merge_dicts(db_structure, parent_db_schema_dict)

        return db_structure


class OpenDbtCatalog(OpenDbtLogger):
    def __init__(self, manifest_path: Path, catalog_path: Path):
        self.manifest: dict = json.loads(manifest_path.read_text())
        self.catalog_file = manifest_path.parent.joinpath("catalogl.json")
        if catalog_path.exists():
            self.catalog: dict = json.loads(catalog_path.read_text())
        else:
            self.catalog: dict = {}

        self._nodes: Optional[Dict[str, OpenDbtNode]] = None
        self._tables2nodes = None
        self.dialect = self.manifest["metadata"]["adapter_type"]

    def table(self, table_fqn: str) -> OpenDbtNode:
        if table_fqn in self.tables2nodes:
            node_id = self.tables2nodes[table_fqn]
            try:
                return self.node(node_id=node_id)
            except:
                raise Exception(f"Given table {table_fqn}, node: {node_id} not found in catalog")

        raise Exception(f"Given table {table_fqn} not found in catalog")

    def node(self, node_id: str) -> OpenDbtNode:
        if node_id in self.nodes:
            return self.nodes.get(node_id)

        raise Exception(f"Given node {node_id} not found in catalog")

    @property
    def tables2nodes(self) -> dict:
        if not self._tables2nodes:
            self._tables2nodes = {}
            self._tables2nodes = {node.table_fqn.strip().lower(): key for key, node in self.nodes.items()}

        return self._tables2nodes

    def export(self):
        self.log.info("Generating catalogl.json data with column level lineage.")
        catalog = self.catalog
        catalog["nodes"] = {}
        catalog["sources"] = {}
        keys_to_export = {"metadata", "stats", "columns"}
        for model_id, model in tqdm.tqdm(self.nodes.items()):
            model.populate_lineage(self.tables2nodes)
            node_dict = {key: model.node[key] for key in keys_to_export if key in model.node}
            catalog["nodes"][model_id] = node_dict

        self.catalog_file.unlink(missing_ok=True)
        self.catalog_file.write_text(json.dumps(obj=catalog, default=lambda obj: obj.to_dict()))

    @property
    def nodes(self):
        # pylint: disable=(too-many-locals)
        if not self._nodes:
            self._nodes = {}
            manifest_nodes = self.manifest.get("nodes", {})
            catalog_nodes = self.catalog.get("nodes", {})

            for node_id, manifest_node_data in manifest_nodes.items():
                if manifest_node_data.get("resource_type") in ["model", "seed", "snapshot"]:
                    # Find corresponding catalog data, default to empty dict if not found
                    catalog_node_data = catalog_nodes.get(node_id, {})
                    try:
                        merged_node = OpenDbtNode(manifest_node=manifest_node_data,
                                                  catalog_node=catalog_node_data,
                                                  dialect=self.dialect)
                        self._nodes[node_id] = merged_node
                    except Exception as e:
                        self.log.warning(f"Could not create MergedDBTNode for node '{node_id}' {str(e)}")

            manifest_sources = self.manifest.get("sources", {})
            catalog_sources = self.catalog.get("sources", {})
            for source_id, manifest_source_data in manifest_sources.items():
                catalog_source_data = catalog_sources.get(source_id, {})
                try:
                    merged_source = OpenDbtNode(manifest_node=manifest_source_data,
                                                catalog_node=catalog_source_data,
                                                dialect=self.dialect)
                    self._nodes[source_id] = merged_source
                except Exception as e:
                    self.log.warning(f"Could not create MergedDBTNode for source '{source_id}': {e}",
                                     exc_info=True)  # Add traceback

            self.log.info(f"Loaded {len(self._nodes)} merged nodes and sources.")

            # update parent nodes for each node
            for node_id, node in self._nodes.items():
                for parent_node_id in node.depends_on:
                    if parent_node_id in self.nodes:
                        parent_node_obj = self.nodes[parent_node_id]
                        node.parent_nodes[parent_node_id] = parent_node_obj
                    else:
                        self.log.warning(f"Parent model {parent_node_id} not found in catalog")

        return self._nodes
