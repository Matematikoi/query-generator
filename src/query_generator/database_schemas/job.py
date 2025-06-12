from typing import Any

from query_generator.utils.exceptions import (
  InvalidForeignKeyError,
  TableNotFoundError,
)


def get_job_table_info() -> tuple[dict[str, dict[str, Any]], list[str]]:
  tables: dict[str, dict[str, Any]] = {
    "aka_name": {
      "alias": "an",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "person_id": {"min": "placeholder", "max": "placeholder"},
        "name": {"min": "placeholder", "max": "placeholder"},
        "imdb_index": {"min": "placeholder", "max": "placeholder"},
        "name_pcode_cf": {"min": "placeholder", "max": "placeholder"},
        "name_pcode_nf": {"min": "placeholder", "max": "placeholder"},
        "surname_pcode": {"min": "placeholder", "max": "placeholder"},
        "md5sum": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [
        {
          "column": "person_id",
          "ref_table": "name",
          "ref_column": "id",
        },
      ],
    },
    "aka_title": {
      "alias": "at",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "title": {"min": "placeholder", "max": "placeholder"},
        "imdb_index": {"min": "placeholder", "max": "placeholder"},
        "kind_id": {"min": "placeholder", "max": "placeholder"},
        "production_year": {"min": "placeholder", "max": "placeholder"},
        "phonetic_code": {"min": "placeholder", "max": "placeholder"},
        "episode_of_id": {"min": "placeholder", "max": "placeholder"},
        "season_nr": {"min": "placeholder", "max": "placeholder"},
        "episode_nr": {"min": "placeholder", "max": "placeholder"},
        "note": {"min": "placeholder", "max": "placeholder"},
        "md5sum": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [],
    },
    "cast_info": {
      "alias": "ci",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "person_id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "person_role_id": {"min": "placeholder", "max": "placeholder"},
        "note": {"min": "placeholder", "max": "placeholder"},
        "nr_order": {"min": "placeholder", "max": "placeholder"},
        "role_id": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [
        {
          "column": "person_id",
          "ref_table": "aka_name",
          "ref_column": "person_id",
        },
        {
          "column": "person_role_id",
          "ref_table": "char_name",
          "ref_column": "id",
        },
        {
          "column": "role_id",
          "ref_table": "role_type",
          "ref_column": "id",
        },
      ],
    },
    "char_name": {
      "alias": "chn",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "name": {"min": "placeholder", "max": "placeholder"},
        "imdb_index": {"min": "placeholder", "max": "placeholder"},
        "imdb_id": {"min": "placeholder", "max": "placeholder"},
        "name_pcode_nf": {"min": "placeholder", "max": "placeholder"},
        "surname_pcode": {"min": "placeholder", "max": "placeholder"},
        "md5sum": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [],
    },
    "comp_cast_type": {
      "alias": "cct",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "kind": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [],
    },
    "company_name": {
      "alias": "cn",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "name": {"min": "placeholder", "max": "placeholder"},
        "country_code": {"min": "placeholder", "max": "placeholder"},
        "imdb_id": {"min": "placeholder", "max": "placeholder"},
        "name_pcode_nf": {"min": "placeholder", "max": "placeholder"},
        "name_pcode_sf": {"min": "placeholder", "max": "placeholder"},
        "md5sum": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [],
    },
    "company_type": {
      "alias": "ct",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "kind": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [],
    },
    "complete_cast": {
      "alias": "cc",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "subject_id": {"min": "placeholder", "max": "placeholder"},
        "status_id": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [
        {
          "column": "subject_id",
          "ref_table": "comp_cast_type",
          "ref_column": "id",
        },
        {
          "column": "status_id",
          "ref_table": "comp_cast_type",
          "ref_column": "id",
        },
      ],
    },
    "info_type": {
      "alias": "it",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "info": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [],
    },
    "keyword": {
      "alias": "k",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "keyword": {"min": "placeholder", "max": "placeholder"},
        "phonetic_code": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [],
    },
    "kind_type": {
      "alias": "kt",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "kind": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [],
    },
    "link_type": {
      "alias": "lt",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "link": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [],
    },
    "movie_companies": {
      "alias": "mc",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "company_id": {"min": "placeholder", "max": "placeholder"},
        "company_type_id": {"min": "placeholder", "max": "placeholder"},
        "note": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [
        {
          "column": "company_id",
          "ref_table": "company_name",
          "ref_column": "id",
        },
        {
          "column": "company_type_id",
          "ref_table": "company_type",
          "ref_column": "id",
        },
      ],
    },
    "movie_info": {
      "alias": "mi",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "info_type_id": {"min": "placeholder", "max": "placeholder"},
        "info": {"min": "placeholder", "max": "placeholder"},
        "note": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [
        {
          "column": "info_type_id",
          "ref_table": "info_type",
          "ref_column": "id",
        },
      ],
    },
    "movie_info_idx": {
      "alias": "mi_idx",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "info_type_id": {"min": "placeholder", "max": "placeholder"},
        "info": {"min": "placeholder", "max": "placeholder"},
        "note": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [
        {
          "column": "info_type_id",
          "ref_table": "info_type",
          "ref_column": "id",
        },
      ],
    },
    "movie_keyword": {
      "alias": "mk",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "keyword_id": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [
        {
          "column": "keyword_id",
          "ref_table": "keyword",
          "ref_column": "id",
        },
      ],
    },
    "movie_link": {
      "alias": "ml",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "linked_movie_id": {"min": "placeholder", "max": "placeholder"},
        "link_type_id": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [
        {
          "column": "link_type_id",
          "ref_table": "link_type",
          "ref_column": "id",
        },
      ],
    },
    "name": {
      "alias": "n",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "name": {"min": "placeholder", "max": "placeholder"},
        "imdb_index": {"min": "placeholder", "max": "placeholder"},
        "imdb_id": {"min": "placeholder", "max": "placeholder"},
        "gender": {"min": "placeholder", "max": "placeholder"},
        "name_pcode_cf": {"min": "placeholder", "max": "placeholder"},
        "name_pcode_nf": {"min": "placeholder", "max": "placeholder"},
        "surname_pcode": {"min": "placeholder", "max": "placeholder"},
        "md5sum": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [],
    },
    "person_info": {
      "alias": "pi",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "person_id": {"min": "placeholder", "max": "placeholder"},
        "info_type_id": {"min": "placeholder", "max": "placeholder"},
        "info": {"min": "placeholder", "max": "placeholder"},
        "note": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [],
    },
    "role_type": {
      "alias": "rt",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "role": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [],
    },
    "title": {
      "alias": "t",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "title": {"min": "placeholder", "max": "placeholder"},
        "imdb_index": {"min": "placeholder", "max": "placeholder"},
        "kind_id": {"min": "placeholder", "max": "placeholder"},
        "production_year": {"min": "placeholder", "max": "placeholder"},
        "imdb_id": {"min": "placeholder", "max": "placeholder"},
        "phonetic_code": {"min": "placeholder", "max": "placeholder"},
        "episode_of_id": {"min": "placeholder", "max": "placeholder"},
        "season_nr": {"min": "placeholder", "max": "placeholder"},
        "episode_nr": {"min": "placeholder", "max": "placeholder"},
        "series_years": {"min": "placeholder", "max": "placeholder"},
        "md5sum": {"min": "placeholder", "max": "placeholder"},
      },
      "foreign_keys": [
        {
          "column": "id",
          "ref_table": "complete_cast",
          "ref_column": "movie_id",
        },
        {
          "column": "id",
          "ref_table": "aka_title",
          "ref_column": "movie_id",
        },
        {
          "column": "id",
          "ref_table": "movie_link",
          "ref_column": "movie_id",
        },
        {
          "column": "id",
          "ref_table": "movie_link",
          "ref_column": "linked_movie_id",
        },
        {
          "column": "id",
          "ref_table": "cast_info",
          "ref_column": "movie_id",
        },
        {
          "column": "kind_id",
          "ref_table": "kind_type",
          "ref_column": "id",
        },
        {
          "column": "id",
          "ref_table": "movie_info",
          "ref_column": "movie_id",
        },
        {
          "column": "id",
          "ref_table": "movie_info_idx",
          "ref_column": "movie_id",
        },
        {
          "column": "id",
          "ref_table": "movie_keyword",
          "ref_column": "movie_id",
        },
        {
          "column": "id",
          "ref_table": "movie_companies",
          "ref_column": "movie_id",
        },
      ],
    },
  }
  for table_name, table_info in tables.items():
    for fk in table_info["foreign_keys"]:
      ref_table = fk["ref_table"]
      if fk["column"] not in tables[table_name]["columns"]:
        raise InvalidForeignKeyError(table_name, fk["column"])
      if ref_table not in tables:
        raise TableNotFoundError(fk["ref_table"])
      if fk["ref_column"] not in tables[ref_table]["columns"]:
        raise InvalidForeignKeyError(
          ref_table,
          fk["ref_column"],
        )

  fact_tables = ["title"]
  return tables, fact_tables
