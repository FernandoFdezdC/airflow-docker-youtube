import warnings
import json

class JsonToDataframes:
    # Constructor method (__init__) which initializes the class attributes
    def __init__(self):
        pass

    def __merge_templates(self, t1, t2, path="root"):
        """
        Merge two template fragments t1 and t2.
        If there is any inconsistency in types or in dict keys, issue a warning.
        """
        # If they are exactly equal, nothing to do.
        if t1 == t2:
            return t1

        # If one is None (a scalar) and the other is not, warn.
        if t1 is None and t2 is not None:
            warnings.warn(
                f"Inconsistent structure at {path}: expected a scalar but got {type(t2).__name__}."
            )
            return t2
        if t2 is None and t1 is not None:
            warnings.warn(
                f"Inconsistent structure at {path}: expected {type(t1).__name__} but got scalar."
            )
            return t1

        # If both are dicts, merge keys.
        if isinstance(t1, dict) and isinstance(t2, dict):
            keys = set(t1.keys()) | set(t2.keys())
            merged = {}
            for key in keys:
                subpath = f"{path}.{key}"
                merged[key] = self.__merge_templates(t1.get(key), t2.get(key), subpath)
            return merged

        # If both are lists, merge their inner templates.
        if isinstance(t1, list) and isinstance(t2, list):
            # Both lists are assumed to be in “template form”
            # (either [] for a list of scalars or a list with one merged element for structured items)
            if not t1 and not t2:
                return []  # both are empty: fine
            if not t1 or not t2:
                warnings.warn(
                    f"Inconsistent structure at {path}: one list is empty while the other is not."
                )
                return t1 if t1 else t2
            # Both non-empty: we expect a single template element in each.
            merged_inner = self.__merge_templates(t1[0], t2[0], path + "[0]")
            return [merged_inner]

        # Otherwise, types are different.
        warnings.warn(
            f"Inconsistent structure at {path}: type mismatch between {type(t1).__name__} and {type(t2).__name__}."
        )
        # Prefer a structured type (dict or list) if available.
        return t1 if isinstance(t1, (dict, list)) else t2

    def __get_structure_template(self, data, path="root"):
        """
        Analyzes the JSON structure and returns a template containing all possible keys.
        
        - Dictionaries are recursed into.
        - Lists are processed element‐by‐element: if they contain structured items (dicts or non‐empty lists)
        the merged structure is wrapped into a list; if they contain only scalars the template is [].
        - Scalars are represented as None.
        
        If inconsistencies are found (different types, mismatching keys, etc.) a warning is issued.
        The `path` parameter (which defaults to "root") is used to indicate the location in the JSON.
        """
        if isinstance(data, dict):
            template = {}
            for key, value in data.items():
                template[key] = self.__get_structure_template(value, f"{path}.{key}")
            return template

        elif isinstance(data, list):
            # Process each item in the list.
            templates = [self.__get_structure_template(item, f"{path}[{i}]") for i, item in enumerate(data)]
            
            # If all items are scalars (represented as None), then we return an empty list.
            if all(t is None for t in templates):
                return []
            
            # Otherwise, merge the templates from all items.
            merged = templates[0]
            for t in templates[1:]:
                merged = self.__merge_templates(merged, t, path)
            
            # If the merged result is a structured type (a dict or a non-empty list), wrap it in a list.
            if isinstance(merged, dict) or (isinstance(merged, list) and merged != []):
                return [merged]
            else:
                return merged

        else:
            # Scalars (int, str, etc.) are represented as None.
            return None
    

    def __get_valid_choice(self, prompt, valid_choices):
        """
        Prompt the user until they enter one of the valid choices.
        """
        while True:
            choice = input(prompt).strip()
            if choice in valid_choices:
                return choice
            print("Invalid option, please try again.")


    def __get_separation_action(self, full_key):
        """
        Ask the user how to separate a nested structure and return the corresponding action.
        """
        inner_prompt = (
            f"\nFor column '{full_key}', choose how to separate the nested structure:\n"
            " - (1): Repeat the outer table by adding a column for each secondary item.\n"
            " - (2): Create a secondary table where each item references the original table's ID.\n"
            "Your selection (1/2): "
        )
        inner_choice = self.__get_valid_choice(inner_prompt, {"1", "2"})
        return "separateOuter" if inner_choice == "1" else "separateInner"


    def __prompt_for_selection(self, key, value, parent=""):
        """
        Recursively prompt the user for what to do with the given key/value.
        """
        full_key = f"{parent}.{key}" if parent else key

        # Handle nested structures (dict or list)
        if isinstance(value, dict) or isinstance(value, list):
            type_label = "list" if isinstance(value, list) else "nested structure (dict)"
            prompt = (
                f"\nThe column '{full_key}' is a {type_label}. "
                "Do you want to (1) keep it in the main table (flatten it), "
                "(2) separate it into its own table, or (3) skip it? Enter 1, 2, or 3: "
            )
            choice = self.__get_valid_choice(prompt, {"1", "2", "3"})
            if choice == "3":
                return full_key, "skip"

            # Choose action based on user's choice
            action = "keep" if choice == "1" else self.__get_separation_action(full_key)

            # If the value is a dictionary, recursively process its keys.
            if isinstance(value, dict):
                nested_selections = {}
                for subkey, subvalue in value.items():
                    sub_full_key, sub_selection = self.__prompt_for_selection(subkey, subvalue, full_key)
                    nested_selections[sub_full_key] = sub_selection
                return full_key, (action, nested_selections)

            # If the value is a list, and it contains dictionaries, build the union of keys.
            if isinstance(value, list):
                if any(isinstance(item, dict) for item in value):
                    union_keys = {}
                    for item in value:
                        if isinstance(item, dict):
                            for subkey, subvalue in item.items():
                                union_keys.setdefault(subkey, subvalue)
                    if union_keys:
                        print(f"\nFor the list '{full_key}', found the following possible keys: {list(union_keys.keys())}")
                        nested_selections = {}
                        for subkey, subvalue in union_keys.items():
                            sub_full_key, sub_selection = self.__prompt_for_selection(subkey, subvalue, full_key)
                            nested_selections[sub_full_key] = sub_selection
                        return full_key, (action, nested_selections)
                # For lists with no dictionaries, simply return the action.
                return full_key, action

        # For scalar values, simply ask if the column should be kept.
        prompt = f"\nDo you want to keep column '{full_key}'? (y/n): "
        choice = self.__get_valid_choice(prompt, {"y", "n"})
        return full_key, ("keep" if choice == "y" else "skip")


    def __get_user_selections(self, template, parent=""):
        """
        Walk through the JSON template and collect user selections.
        """
        # If the top-level template is a list, expect a single element.
        if isinstance(template, list):
            if len(template) != 1:
                print("Top-level list must contain exactly one element.")
                return {}
            template = template[0]

        if not isinstance(template, dict):
            print("JSON format not valid. It must be either a dictionary or a list.")
            return {}

        selections = {}
        for key, value in template.items():
            full_key, selection = self.__prompt_for_selection(key, value, parent)
            selections[full_key] = selection

        return selections

    def fit(self, data):
        print("Scanning the JSON file structure...")
        template = self.__get_structure_template(data)
        print("Merged structure template:")
        print(json.dumps(template, indent=2))
        self.selections = self.__get_user_selections(template)


import json
import pandas as pd
from typing import Any, Dict, List, Union

def json_to_dataframes(data, selections):
    # -------------------------------
    class DataProcessor:
        """
        Encapsulates the state and functions for processing JSON records
        according to a selections mapping.
        """
        def __init__(self, selections: Dict[str, Any]):
            self.selections = selections
            self.pk_counters = {}    # e.g. { 'comments': 3, ... }
            self.separate_data = {}  # e.g. { 'comments': [ {...}, ... ], ... }

        def get_new_pk(self, table_name: str) -> int:
            """Return a new primary key for the given table name."""
            if table_name not in self.pk_counters:
                self.pk_counters[table_name] = 0
            self.pk_counters[table_name] += 1
            return self.pk_counters[table_name]

        def process_level(
            self,
            record: Dict[str, Any],
            selections: Dict[str, Any],
            common_prefix: str = "",
            preserve_full_keys: bool = False,
            current_table: str = "main"
        ) -> Dict[str, Any]:
            """
            Recursively process a JSON record according to the selections.
            
            Parameters:
                record            : dict, the JSON object at the current level.
                selections        : dict mapping selection keys to:
                                    - "keep" or "skip"
                                    - (directive, sub_selections) tuples.
                common_prefix     : str; common prefix for the current level.
                preserve_full_keys: bool; if True, use full selection keys (dots replaced by underscores)
                                    in the output; otherwise, use the effective (stripped) key.
                current_table     : str; the name of the table that is being processed.
                                For the top-level record, defaults to "main". For nested records,
                                this is set to a value based on the full_key.
            
            Returns:
                A dict representing the flattened row for the current table.
            
            Side Effects:
                For every field with a separate directive, a new table row is created and stored in
                self.separate_data.
            """
            output = {}
            prefix = common_prefix + "." if common_prefix else ""
            
            for full_key, sel in selections.items():
                # When a common_prefix is provided, only process keys that start with it.
                if common_prefix:
                    if not full_key.startswith(prefix):
                        continue
                    effective_key = full_key[len(prefix):]
                else:
                    effective_key = full_key

                # --- Leaf fields ---
                if isinstance(sel, str):
                    if sel == "keep" and effective_key in record:
                        out_key = full_key.replace(".", "_") if preserve_full_keys else effective_key
                        output[out_key] = record[effective_key]
                    elif sel == "separateOuter" and effective_key in record:
                        # For separateOuter on a leaf value, create a separate table row.
                        value = record[effective_key]
                        table_name = full_key.replace(".", "_")
                        if isinstance(value, list):
                            fk_list = []
                            for item in value:
                                pk = self.get_new_pk(table_name)
                                fk_list.append(pk)
                                self.separate_data.setdefault(table_name, []).append({
                                    "id": pk,
                                    table_name: item
                                })
                            fk_col = table_name + "_id"
                            output[fk_col] = fk_list
                        else:
                            pk = self.get_new_pk(table_name)
                            fk_col = table_name + "_id"
                            output[fk_col] = pk
                            self.separate_data.setdefault(table_name, []).append({
                                "id": pk,
                                table_name: value
                            })
                    elif sel == "separateInner" and effective_key in record:
                        # Ensure the parent row has a PK.
                        if "id" not in output:
                            output["id"] = self.get_new_pk(current_table)
                        parent_id = output["id"]
                        table_name = full_key.replace(".", "_")
                        value = record[effective_key]
                        if isinstance(value, list):
                            for item in value:
                                new_row = {
                                    "id": self.get_new_pk(table_name),
                                    table_name: item,
                                    "parent_id": parent_id
                                }
                                self.separate_data.setdefault(table_name, []).append(new_row)
                        else:
                            new_row = {
                                "id": self.get_new_pk(table_name),
                                table_name: value,
                                "parent_id": parent_id
                            }
                            self.separate_data.setdefault(table_name, []).append(new_row)
                
                # --- Nested fields (using tuple directive) ---
                elif isinstance(sel, tuple):
                    directive, sub_sel = sel
                    if effective_key not in record:
                        continue
                    value = record[effective_key]
                    
                    # --- When the field's value is a list ---
                    if isinstance(value, list):
                        if directive == "keep":
                            nested_results = []
                            for item in value:
                                result = self.process_level(
                                    item,
                                    sub_sel,
                                    common_prefix=full_key,
                                    preserve_full_keys=False,
                                    current_table=current_table
                                )
                                nested_results.append(result)
                            output[effective_key] = nested_results
                            
                        elif directive == "separateOuter":
                            table_name = full_key.replace(".", "_")
                            fk_list = []
                            for item in value:
                                result = self.process_level(
                                    item,
                                    sub_sel,
                                    common_prefix=full_key,
                                    preserve_full_keys=True,
                                    current_table=table_name
                                )
                                pk = self.get_new_pk(table_name)
                                result["id"] = pk
                                fk_list.append(pk)
                                self.separate_data.setdefault(table_name, []).append(result)
                            fk_col = table_name + "_id"
                            output[fk_col] = fk_list
                            
                        elif directive == "separateInner":
                            # For separateInner, the parent's row gets its own PK if not already set.
                            if "id" not in output:
                                output["id"] = self.get_new_pk(current_table)
                            parent_id = output["id"]
                            table_name = full_key.replace(".", "_")
                            for item in value:
                                result = self.process_level(
                                    item,
                                    sub_sel,
                                    common_prefix=full_key,
                                    preserve_full_keys=True,
                                    current_table=table_name
                                )
                                result["parent_id"] = parent_id
                                self.separate_data.setdefault(table_name, []).append(result)
                            # Note: with separateInner the parent row is not modified with a foreign key column.
                        
                        else:
                            # Unknown directive; optionally raise an error or skip.
                            pass

                    # --- When the field's value is a single (dict) object ---
                    else:
                        if directive == "keep":
                            nested = self.process_level(
                                value,
                                sub_sel,
                                common_prefix=full_key,
                                preserve_full_keys=False,
                                current_table=current_table
                            )
                            for k, v in nested.items():
                                output[f"{effective_key}_{k}"] = v
                        elif directive == "separateOuter":
                            table_name = full_key.replace(".", "_")
                            pk = self.get_new_pk(table_name)
                            fk_col = table_name + "_id"
                            output[fk_col] = pk
                            nested = self.process_level(
                                value,
                                sub_sel,
                                common_prefix=full_key,
                                preserve_full_keys=True,
                                current_table=table_name
                            )
                            nested["id"] = pk
                            self.separate_data.setdefault(table_name, []).append(nested)
                        elif directive == "separateInner":
                            if "id" not in output:
                                output["id"] = self.get_new_pk(current_table)
                            parent_id = output["id"]
                            table_name = full_key.replace(".", "_")
                            nested = self.process_level(
                                value,
                                sub_sel,
                                common_prefix=full_key,
                                preserve_full_keys=True,
                                current_table=table_name
                            )
                            nested["parent_id"] = parent_id
                            self.separate_data.setdefault(table_name, []).append(nested)
                        else:
                            # Unknown directive.
                            pass
            return output

        def process_records(
            self,
            records: Union[Dict[str, Any], List[Any]]
        ) -> (List[Dict[str, Any]], Dict[str, pd.DataFrame]):
            """
            Process a JSON record or list of records.
            
            Returns:
                main_rows: a list of flattened dictionaries (one per record).
                separate_dfs: a dict mapping table names to their corresponding DataFrames.
            """
            if not isinstance(records, list):
                records = [records]
            # For the top-level, current_table defaults to "main".
            main_rows = [self.process_level(rec, self.selections, current_table="main") for rec in records]
            separate_dfs = {}
            for table, rows in self.separate_data.items():
                df = pd.DataFrame(rows)
                # Convert 'id' and 'parent_id' columns to integers if they exist
                # for col in ['id', 'parent_id']:
                #     if col in df.columns:
                #         df[col] = df[col].astype('Int64')  # Nullable integer type
                separate_dfs[table] = df
            return main_rows, separate_dfs

    # -------------------------------
    def flatten_df(df: pd.DataFrame) -> pd.DataFrame:
        """
        Recursively flatten a DataFrame by:
        1. Exploding columns whose values are lists.
        2. Expanding columns whose values are dictionaries (using pd.json_normalize).
        
        Continues until no further lists or dictionaries are found.
        """
        df = df.copy()
        changed = True
        while changed:
            changed = False
            
            # Explode any columns containing lists.
            list_cols = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, list)).any()]
            if list_cols:
                for col in list_cols:
                    df = df.explode(col).reset_index(drop=True)
                changed = True
            
            # Expand any columns containing dictionaries.
            for col in list(df.columns):  # Use list() so that we can modify columns during iteration.
                sample = df[col].dropna()
                if not sample.empty and all(isinstance(x, dict) for x in sample):
                    expanded = pd.json_normalize(df[col])
                    # Prefix new column names with the original column name.
                    expanded.columns = [f"{col}_{sub}" for sub in expanded.columns]
                    df = df.drop(columns=[col]).reset_index(drop=True).join(expanded.reset_index(drop=True))
                    changed = True
                    break  # Restart loop because df has changed.
        return df

    # Create a DataProcessor instance and process the data.
    processor = DataProcessor(selections)
    main_rows, separate_dfs = processor.process_records(data)

    # Create and flatten the main DataFrame.
    main_df = pd.DataFrame(main_rows)
    main_df = flatten_df(main_df)

    # Also flatten each separate DataFrame.
    for table, df in separate_dfs.items():
        separate_dfs[table] = flatten_df(df)

    # # --- Output the final DataFrames.
    # print("Flattened Main DataFrame:")
    # print(main_df)

    # for table, df in separate_dfs.items():
    #     print(f"\nFlattened DataFrame for table: {table}")
    #     print(df)

    return main_df, separate_dfs

# Load the JSON file (adjust the file name if needed)
def read_json_file(file_path: str) -> Union[Dict[str, Any], List[Any]]:
    """Load JSON data from a file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)