import pandas as pd
import seaborn as sns
import sys
from collections import defaultdict
from enum import Enum, auto, unique
from matplotlib.ticker import FuncFormatter


def date_range(start, end):
    timestamps = pd.date_range(start, end).tolist()
    return [timestamp.date().isoformat() for timestamp in timestamps]


def file_extension():
    return ".json?"


def read_rule(line):
    parts = line.strip().split(";")
    antecedent = parts[0]
    consequent = parts[1]
    support = int(parts[2])
    confidence = float(parts[3])
    lift = float(parts[4])
    hist_str = parts[5][2:-2]
    hist_parts = hist_str.split(",")
    hist = [int(x) for x in hist_parts]
    result = [support, confidence, lift, hist]
    if len(parts) > 6:
        result += parts[6:]
    return antecedent, consequent, result


def read_rules(file_name):
    result = defaultdict(dict)
    with open(file_name) as f:
        for line in f:
            antecedent, consequent, hist = read_rule(line)
            result[antecedent][consequent] = hist
    return result


def format_number(value, decimals=0, sep="\u2009"):
    return f"{value:,.{decimals}f}".replace(",", sep)


def number_formatter(decimals=0, sep="\u2009"):
    return FuncFormatter(lambda x, p: format_number(x, decimals, sep))


def markers():
    return ["^", "X", "s", "D", ".", "o"]


def colors():
    return sns.color_palette("tab10")


@unique
class Entity(Enum):
    Table = auto()
    Column = auto()
    Row = auto()
    Field = auto()

    @classmethod
    def string_representations(cls):
        return [entity.to_str() for entity in cls]

    def to_str(self):
        return super().name.lower()


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
    def get_with_level(cls, level, table, column, row):
        if level == Entity.Table:
            return cls(table, "", "")
        if level == Entity.Column:
            return cls(table, column, "")
        if level == Entity.Row:
            return cls(table, "", row)
        return cls(table, column, row)

    @classmethod
    def get_csv_header(cls, level: Entity) -> str:
        entities_needed = [Entity.Table]
        if level in [Entity.Column, Entity.Field]:
            entities_needed.append(Entity.Column)
        if level in [Entity.Row, Entity.Field]:
            entities_needed.append(Entity.Row)
        return ";".join([e.to_str() for e in entities_needed])

    def get_csv(self, level: Entity) -> str:
        return self._join_by_level(level, ";")

    def get_id(self, level: Entity) -> str:
        return self._join_by_level(level, "_")

    def _join_by_level(self, level: Entity, sep: str) -> str:
        representations = {
            Entity.Table: self.table,
            Entity.Column: sep.join([self.table, self.column]),
            Entity.Row: sep.join([self.table, self.row]),
            Entity.Field: sep.join([self.table, self.column, self.row])
        }
        if not level in representations:
            raise ValueError("Unsupported entity.")
        return representations[level]

