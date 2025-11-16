"""
Tabular module
"""

import os
from typing import Any, List, Optional, Union

# Conditional import
try:
    import pandas as pd

    PANDAS = True
except ImportError:
    PANDAS = False

try:
    from ..base import Pipeline
except ImportError:
    # Fallback for direct testing
    import sys
    sys.path.append('/home/logi/txtai/src/python')
    from txtai.pipeline.base import Pipeline


# Original Tabular class - commented out as requested
# class Tabular(Pipeline):
#     """
#     Splits tabular data into rows and columns.
#     """
#
#     def __init__(self, idcolumn=None, textcolumns=None, content=False):
#         """
#         Creates a new Tabular pipeline.
#
#         Args:
#             idcolumn: column name to use for row id
#             textcolumns: list of columns to combine as a text field
#             content: if True, a dict per row is generated with all fields. If content is a list, a subset of fields
#                      is included in the generated rows.
#         """
#
#         if not PANDAS:
#             raise ImportError('Tabular pipeline is not available - install "pipeline" extra to enable')
#         self.idcolumn = idcolumn
#         self.textcolumns = textcolumns
#         self.content = content
#
#     def __call__(self, data):
#         """
#         Splits data into rows and columns.
#
#         Args:
#             data: input data
#
#         Returns:
#             list of (id, text, tag)
#         """
#
#         items = [data] if not isinstance(data, list) else data
#
#         # Combine all rows into single return element
#         results = []
#         dicts = []
#
#         for item in items:
#             # File path
#             if isinstance(item, str):
#                 _, extension = os.path.splitext(item)
#                 extension = extension.replace(".", "").lower()
#
#                 if extension == "csv":
#                     df = pd.read_csv(item)
#
#                 results.append(self.process(df))
#
#             # Dict
#             if isinstance(item, dict):
#                 dicts.append(item)
#
#             # List of dicts
#             elif isinstance(item, list):
#                 df = pd.DataFrame(item)
#                 results.append(self.process(df))
#
#         if dicts:
#             df = pd.DataFrame(dicts)
#             results.extend(self.process(df))
#
#         return results
#
#     def process(self, df):
#         """
#         Process a DataFrame into rows.
#
#         Args:
#             df: input DataFrame
#
#         Returns:
#             list of (id, text, tag)
#         """
#
#         results = []
#
#         for _, row in df.iterrows():
#             # Convert row to dictionary
#             row_dict = row.to_dict()
#
#             # Generate id
#             uid = str(row[self.idcolumn]) if self.idcolumn and self.idcolumn in row_dict else str(row.name)
#
#             # Combine text columns
#             text = " ".join([str(row[col]) for col in self.textcolumns if col in row_dict]) if self.textcolumns else ""
#
#             # Generate content
#             if self.content:
#                 if isinstance(self.content, list):
#                     content = {col: row_dict[col] for col in self.content if col in row_dict}
#                 else:
#                     content = row_dict
#             else:
#                 content = None
#
#             # Build result
#             result = (uid, text, content) if content else (uid, text, None)
#             results.append(result)
#
#         return results
#
#     def column(self, value):
#         """
#         Applies column standardization logic:
#             - Replace NaN values with None
#
#         Args:
#             value: input value
#
#         Returns:
#             formatted value
#         """
#
#         # Check for null - treat lists as not null
#         return None if not isinstance(value, list) and pd.isnull(value) else value


# Versatile Tabular class moved from pipeline.py
class Tabular(Pipeline):
    """
    Splits tabular data into rows and columns.
    """

    def __init__(
        self,
        idcolumn: Optional[str] = None,
        textcolumns: Optional[List[str]] = None,
        content: Union[bool, List[str]] = False,
        **kwargs
    ):
        """
        Creates a new Tabular pipeline.

        Args:
            idcolumn: column name to use for row id
            textcolumns: list of columns to combine as a text field
            content: if True, a dict per row is generated with all fields. If content is a list, a subset of fields
                     is included in the generated rows.
        """

        if not PANDAS:
            raise ImportError('Tabular pipeline is not available - install "pipeline" extra to enable')
        super().__init__(**kwargs)
        self.idcolumn = idcolumn
        self.textcolumns = textcolumns
        self.content = content

    def __call__(self, data: Any) -> List[Any]:
        """
        Splits data into rows and columns.

        Args:
            data: input data

        Returns:
            list of (id, text, tag)
        """

        items = [data] if not isinstance(data, list) else data

        # Combine all rows into single return element
        results = []
        dicts = []

        for item in items:
            # File path
            if isinstance(item, str):
                _, extension = os.path.splitext(item)
                extension = extension.replace(".", "").lower()

                if extension == "csv":
                    df = pd.read_csv(item)
                elif extension in ["xlsx", "xls"]:
                    df = pd.read_excel(item)
                elif extension == "json":
                    df = pd.read_json(item)
                elif extension == "parquet":
                    df = pd.read_parquet(item)
                else:
                    raise ValueError(f"Unsupported file format: {extension}")

                results.append(self.process(df))

            # DataFrame
            elif isinstance(item, pd.DataFrame):
                results.append(self.process(item))

            # Dictionary
            elif isinstance(item, dict):
                df = pd.DataFrame([item])
                results.append(self.process(df))

            # List of dictionaries
            elif isinstance(item, list) and item and isinstance(item[0], dict):
                df = pd.DataFrame(item)
                results.append(self.process(df))

        # Flatten results
        return [result for sublist in results for result in sublist]

    def process(self, df: pd.DataFrame) -> List[Any]:
        """
        Process a DataFrame into rows.

        Args:
            df: input DataFrame

        Returns:
            list of (id, text, tag)
        """

        results = []

        for _, row in df.iterrows():
            # Convert row to dictionary
            row_dict = row.to_dict()

            # Generate id
            uid = str(row[self.idcolumn]) if self.idcolumn and self.idcolumn in row_dict else str(row.name)

            # Combine text columns
            text = " ".join([str(row[col]) for col in self.textcolumns if col in row_dict]) if self.textcolumns else ""

            # Generate content
            if self.content:
                if isinstance(self.content, list):
                    content = {col: row_dict[col] for col in self.content if col in row_dict}
                else:
                    content = row_dict
            else:
                content = None

            # Build result
            result = (uid, text, content) if content else (uid, text, None)
            results.append(result)

        return results

    def concat(self, row, columns):
        """
        Builds a text field from row using columns.

        Args:
            row: input row
            columns: list of columns to join together

        Returns:
            text
        """

        parts = []
        for column in columns:
            column = self.column(row[column])
            if column:
                parts.append(str(column))

        return ". ".join(parts) if parts else None

    def column(self, value):
        """
        Applies column standardization logic:
            - Replace NaN values with None

        Args:
            value: input value

        Returns:
            formatted value
        """

        # Check for null - treat lists as not null
        return None if not isinstance(value, list) and pd.isnull(value) else value

    def process_file(self, file_path: str) -> List[Any]:
        """
        Process a file into rows.

        Args:
            file_path: path to file

        Returns:
            list of (id, text, tag)
        """

        _, extension = os.path.splitext(file_path)
        extension = extension.replace(".", "").lower()

        if extension == "csv":
            df = pd.read_csv(file_path)
        elif extension in ["xlsx", "xls"]:
            df = pd.read_excel(file_path)
        elif extension == "json":
            df = pd.read_json(file_path)
        elif extension == "parquet":
            df = pd.read_parquet(file_path)
        else:
            raise ValueError(f"Unsupported file format: {extension}")

        return self.process(df)

