from dotenv import load_dotenv
import OpenSearchIndexing

# Load environment variables from .env file
load_dotenv()

# FILE/INDEX Names
PARQUET_FILE_PATH = 'authors_clean_data.parquet'
INDEX_NAME = 'university_authors_second'


def define_authors_mapping():
    """Defines the OpenSearch index mapping."""

    return {
        "properties": {
            "university_key": {
                "type": "keyword"
            },
            "id": {
                "type": "keyword"
            },
            "orcid": {
                "type": "keyword"
            },
            "display_name": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "display_name_alternatives": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "works_count": {
                "type": "integer"
            },
            "cited_by_count": {
                "type": "integer"
            },
            "ids": {
                "properties": {
                    "openalex": {"type": "keyword"},
                    "orcid": {"type": "keyword"},
                    "scopus": {"type": "keyword"},
                    "twitter": {"type": "keyword"}
                }
            },
            "last_known_institutions": {
                "type": "nested",
                "properties": {
                    "id": {"type": "keyword"},
                    "ror": {"type": "keyword"},
                    "display_name": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "country_code": {"type": "keyword"},
                    "type": {"type": "keyword"}
                }
            },
            "affiliations": {
                "type": "nested",
                "properties": {
                    "years": {"type": "integer"},
                    "institution": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "ror": {"type": "keyword"},
                            "display_name": {
                                "type": "text",
                                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                            },
                            "country_code": {"type": "keyword"},
                            "type": {"type": "keyword"}
                        }
                    }
                }
            },
            "summary_stats": {
                "properties": {
                    "2yr_mean_citedness": {"type": "float"},
                    "h_index": {"type": "integer"},
                    "i10_index": {"type": "integer"}
                }
            },
            "counts_by_year": {
                "type": "nested",
                "properties": {
                    "year": {"type": "integer"},
                    "works_count": {"type": "integer"},
                    "cited_by_count": {"type": "integer"}
                }
            },
            "x_concepts": {
                "type": "nested",
                "properties": {
                    "id": {"type": "keyword"},
                    "wikidata": {"type": "keyword"},
                    "display_name": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "level": {"type": "integer"},
                    "score": {"type": "float"}
                }
            },
            "created_date": {
                "type": "date",
                "format": "yyyy-MM-dd"
            },
            "updated_date": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis"
            }
        }
    }


if __name__ == "__main__":
    authors_mapping = define_authors_mapping()
    OpenSearchIndexing.main(
        PARQUET_FILE_PATH, INDEX_NAME, authors_mapping, 'id')
