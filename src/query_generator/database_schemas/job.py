from typing import Any


def get_tpcds_table_info() -> Any:
  tables = {
    "aka_name": {
      "alias" : "an",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "person_id": {"min": "placeholder", "max": "placeholder"},
        "name": {"min": "placeholder", "max": "placeholder"},
        "imdb_index": {"min": "placeholder", "max": "placeholder"},
        "name_pcode_cf": {"min": "placeholder", "max": "placeholder"},
        "name_pcode_nf": {"min": "placeholder", "max": "placeholder"},
        "surname_pcode": {"min": "placeholder", "max": "placeholder"},
        "md5sum": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "aka_title": {
      "alias" : "at",
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
      }
    },
    "cast_info": {
      "alias" : "ci",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "person_id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "person_role_id": {"min": "placeholder", "max": "placeholder"},
        "note": {"min": "placeholder", "max": "placeholder"},
        "nr_order": {"min": "placeholder", "max": "placeholder"},
        "role_id": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "char_name": {
      "alias" : "chn",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "name": {"min": "placeholder", "max": "placeholder"},
        "imdb_index": {"min": "placeholder", "max": "placeholder"},
        "imdb_id": {"min": "placeholder", "max": "placeholder"},
        "name_pcode_nf": {"min": "placeholder", "max": "placeholder"},
        "surname_pcode": {"min": "placeholder", "max": "placeholder"},
        "md5sum": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "comp_cast_type": {
      "alias" : "cct",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "kind": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "company_name": {
      "alias" : "cn",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "name": {"min": "placeholder", "max": "placeholder"},
        "country_code": {"min": "placeholder", "max": "placeholder"},
        "imdb_id": {"min": "placeholder", "max": "placeholder"},
        "name_pcode_nf": {"min": "placeholder", "max": "placeholder"},
        "name_pcode_sf": {"min": "placeholder", "max": "placeholder"},
        "md5sum": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "company_type": {
      "alias" : "ct",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "kind": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "complete_cast": {
      "alias" : "cc",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "subject_id": {"min": "placeholder", "max": "placeholder"},
        "status_id": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "info_type": {
      "alias" : "it",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "info": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "keyword": {
      "alias" : "k",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "keyword": {"min": "placeholder", "max": "placeholder"},
        "phonetic_code": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "kind_type": {
      "alias" : "kt",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "kind": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "link_type": {
      "alias" : "lt",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "link": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "movie_companies": {
      "alias" : "mc",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "company_id": {"min": "placeholder", "max": "placeholder"},
        "company_type_id": {"min": "placeholder", "max": "placeholder"},
        "note": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "movie_info": {
      "alias" : "mi",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "info_type_id": {"min": "placeholder", "max": "placeholder"},
        "info": {"min": "placeholder", "max": "placeholder"},
        "note": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "movie_info_idx": {
      "alias" : "mi_idx",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "info_type_id": {"min": "placeholder", "max": "placeholder"},
        "info": {"min": "placeholder", "max": "placeholder"},
        "note": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "movie_keyword": {
      "alias" : "mk",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "keyword_id": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "movie_link": {
      "alias" : "ml",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "movie_id": {"min": "placeholder", "max": "placeholder"},
        "linked_movie_id": {"min": "placeholder", "max": "placeholder"},
        "link_type_id": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "name": {
      "alias" : "n",
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
      }
    },
    "person_info": {
      "alias" : "pi",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "person_id": {"min": "placeholder", "max": "placeholder"},
        "info_type_id": {"min": "placeholder", "max": "placeholder"},
        "info": {"min": "placeholder", "max": "placeholder"},
        "note": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "role_type": {
      "alias" : "rt",
      "columns": {
        "id": {"min": "placeholder", "max": "placeholder"},
        "role": {"min": "placeholder", "max": "placeholder"},
      }
    },
    "title": {
      "alias" : "t",
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
      }
    },
  }
  return tables, False
