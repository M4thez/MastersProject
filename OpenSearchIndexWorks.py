from dotenv import load_dotenv
import OpenSearchIndexing

# Load environment variables from .env file
load_dotenv()

# FILE/INDEX Names
PARQUET_FILE_PATH = 'papers_clean2.parquet'
INDEX_NAME = 'university_works_test'
# INDEX_NAME = 'university_papers_second'


def define_works_mapping():
    """Defines the OpenSearch index mapping."""

    return {
        "properties": {
            "university_key": {
                "type": "keyword",
            },
            "openalex_id": {
                "type": "keyword",
            },
            "doi": {
                "type": "keyword",
            },
            "language": {
                "type": "keyword",
            },
            "type": {
                "type": "keyword",
            },
            "title": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 512
                    }
                }
            },
            "publication_date": {
                "type": "date",
                "format": "yyyy-MM-dd",
            },
            "publication_year": {
                "type": "integer",
            },
            "open_access": {
                "properties": {
                    "any_repository_has_fulltext": {
                        "type": "boolean"
                    },
                    "is_oa": {
                        "type": "boolean"
                    },
                    "oa_status": {
                        "type": "keyword"
                    },
                    "oa_url": {
                        "type": "keyword",
                        "index": False
                    }
                },
            },
            "institutions": {
                "type": "keyword",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                },
            },
            "authors": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                },
            },
            "cited_by_count": {
                "type": "integer",
            },
            "fwci": {
                "type": "float",
            },
            "citation_normalized_percentile": {
                "properties": {
                    "is_in_top_10_percent": {"type": "boolean"},
                    "is_in_top_1_percent": {"type": "boolean"},
                    "value": {"type": "float"}
                },
            },
            "abstract": {
                "type": "text",
                "analyzer": "standard",
            },
            "primary_topic": {
                "properties": {
                    "display_name": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "id": {"type": "keyword"},
                    "score": {"type": "float"},
                    "field": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "display_name": {"type": "keyword"}
                        }
                    },
                    "subfield": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "display_name": {"type": "keyword"}
                        }
                    },
                    "domain": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "display_name": {"type": "keyword"}
                        }
                    }
                },
            },
            "topics": {
                "type": "nested",
                "properties": {
                    "display_name": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}
                    },
                    "id": {"type": "keyword"},
                    "score": {"type": "float"},
                    "field": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "display_name": {"type": "keyword"}
                        }
                    },
                    "subfield": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "display_name": {"type": "keyword"}
                        }
                    },
                    "domain": {
                        "properties": {
                            "id": {"type": "keyword"},
                            "display_name": {"type": "keyword"}
                        }
                    }
                }
            },
            "keywords": {
                "type": "nested",
                "properties": {
                    "display_name": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                    },
                    "id": {
                        "type": "keyword",
                    },
                    "score": {
                        "type": "float",
                    }
                }
            },


            "cited_by_api_url": {
                "type": "keyword",
                "index": False,
            },
            "updated_date": {
                "type": "date",
                "format": "strict_date_optional_time",
            },
            "created_date": {
                "type": "date",
                "format": "yyyy-MM-dd",
            }
        }
    }


if __name__ == "__main__":
    works_mapping = define_works_mapping()
    OpenSearchIndexing.main(PARQUET_FILE_PATH, INDEX_NAME,
                            works_mapping, 'openalex_id')
