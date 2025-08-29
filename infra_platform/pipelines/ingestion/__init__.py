from pipelines.loading.openaq_duckdb_loader import OpenAQLoader


def load_openaq_current_hour():
    """
    Simple interface for OpenAQ
    """
    loader = OpenAQLoader()
    rows = loader.load_current_hour()
    loader.get_air_quality_summary()  # Bonus: résumé automatique
    return rows


def load_openaq_specific(date_str, hour):
    """
    Interface for Backfill OpenAQ
    """
    loader = OpenAQLoader()
    return loader.load_specific_hour(date_str, hour)
