"""Base scraper class that all country scrapers inherit from."""

from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime
from pathlib import Path


class BaseScraper(ABC):
    """Base class for medicine shortage scrapers."""

    def __init__(self, country_code: str, country_name: str, source_name: str, base_url: str):
        self.country_code = country_code
        self.country_name = country_name
        self.source_name = source_name
        self.base_url = base_url

    @abstractmethod
    def scrape(self) -> pd.DataFrame:
        """Scrape the website and return a DataFrame with shortage data.

        Expected columns:
            - country_code: ISO 2-letter country code
            - country_name: Full country name
            - source: Name of the source agency
            - medicine_name: Name of the medicine
            - active_substance: International non-proprietary name
            - strength: Dosage strength
            - package_size: Package description
            - shortage_start: Date when shortage started
            - estimated_end: Estimated date of availability (None if unknown)
            - status: Current status (e.g. "shortage", "resolved")
            - scraped_at: Timestamp of scraping
        """
        pass

    def save_csv(self, df: pd.DataFrame, output_dir: str = "output") -> Path:
        """Save DataFrame to CSV."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        date_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"{self.country_code}_{self.source_name}_shortage_{date_str}.csv"
        filepath = output_path / filename

        df.to_csv(filepath, index=False, encoding="utf-8-sig")
        print(f"Saved {len(df)} records to {filepath}")
        return filepath
