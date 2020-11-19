import pandas as pd
import sys
from enum import Enum, auto, unique


def date_range(start, end):
    timestamps = pd.date_range(start, end).tolist()
    return [timestamp.date().isoformat() for timestamp in timestamps]


def file_extension():
    return ".json_" if sys.platform.startswith("win") else ".json?"


@unique
class Entity(Enum):
    Table = auto()
    Column = auto()
    Row = auto()
    Field = auto()

    @classmethod
    def string_representations(cls):
        return [entity.name.lower() for entity in list(Entity)]

    def to_str(self):
        return self.name.lower()


class Field:
    table: str
    column: str
    row: str  # allows row number or primary key

    def __init__(self, table, column, row):
        self.table = table
        self.column = column
        self.row = row

    def __eq__(self, other):
        return self.table == other.table and self.column == other.column and self.row == other.row

    def __hash__(self):
        return hash((self.table, self.column, self.row))

    @classmethod
    def get_with_level(cls, level: Entity, table, column, row):
        if level == Entity.Table:
            return Entity(table, None, None, None)
        if level == Entity.Column:
            return Entity(table, column, None)
        if level == Entity.Row:
            return Entity(table, None, row)
        return Entity(table, column, row)

    @classmethod
    def get_csv_header(cls, level: Entity) -> str:
        if level == Entity.Table:
            return "table"
        if level == Entity.Column:
            return "table;column"
        if level == Entity.Row:
            return "table;row"
        if level == Entity.Field:
            return "table;column;row"
        raise ValueError("Unsupported entity.")

    def get_csv(self, level: str) -> str:
        if level == Entity.Table:
            return self.table
        if level == Entity.Column:
            return ";".join([self.table, self.column])
        if level == Entity.Row:
            return ";".join([self.table, self.row])
        if level == Entity.Field:
            return ";".join([self.table, self.column, self.row])
        raise ValueError("Unsupported entity.")
