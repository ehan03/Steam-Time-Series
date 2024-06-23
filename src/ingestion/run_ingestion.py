# standard library imports

# third party imports

# local imports
from .ingestion_pipeline import IngestionPipeline


def main() -> None:
    """
    Main function to run ingestion pipeline
    """

    ingestion_pipeline = IngestionPipeline()
    ingestion_pipeline.run()


if __name__ == "__main__":
    main()
