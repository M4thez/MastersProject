from dotenv import load_dotenv
import OpenSearchIndexing

# Load environment variables from .env file
load_dotenv()

# FILE/INDEX Names
PARQUET_FILE_PATH = 'projects_clean.parquet'
INDEX_NAME = 'university_projects'


def define_projects_mapping():
    """Defines the OpenSearch index mapping."""
    return {
        "properties": {
            "university_key": {
                "type": "keyword",
            },
            "id": {
                "type": "keyword",
            },
            "code": {
                "type": "keyword",
            },
            "acronym": {
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
                },
            },
            "startDate": {
                "type": "date",
                "format": "yyyy-MM-dd",
            },
            "endDate": {
                "type": "date",
                "format": "yyyy-MM-dd",
            },
            "callIdentifier": {
                "type": "keyword",
            },
            "openAccessMandateForPublications": {
                "type": "boolean",
            },
            "openAccessMandateForDataset": {
                "type": "boolean",
            },
            "subjects": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                },
            },
            "fundings": {
                "type": "nested",
                "properties": {
                    "fundingStream": {
                        "type": "keyword",
                        "null_value": "NULL"
                    },
                    "jurisdiction": {"type": "keyword"},
                    "name": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {
                            "keyword": {"type": "keyword", "ignore_above": 256}
                        }
                    },
                    "shortName": {"type": "keyword"}
                },
            },
            "summary": {
                "type": "text",
                "analyzer": "standard",
            },
            "granted": {
                "properties": {
                    "currency": {"type": "keyword"},
                    "fundedAmount": {"type": "double"},
                    "totalCost": {"type": "double"}
                },
            }
        }
    }


if __name__ == "__main__":
    mapping = define_projects_mapping()
    OpenSearchIndexing.main(PARQUET_FILE_PATH, INDEX_NAME, mapping, 'id')
