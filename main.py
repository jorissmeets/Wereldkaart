"""Main entry point for the medicine shortage scraper."""

import scrapers as scr
import inspect


def main():
    # Auto-discover all scraper classes from the scrapers module
    all_scrapers = []
    for name, cls in inspect.getmembers(scr, inspect.isclass):
        if name != "BaseScraper" and hasattr(cls, "scrape") and name.endswith("Scraper"):
            all_scrapers.append(cls())

    # Sort by country code for predictable order
    all_scrapers.sort(key=lambda s: s.country_code)

    print(f"Running {len(all_scrapers)} scrapers...")
    for scraper in all_scrapers:
        try:
            df = scraper.scrape()
            scraper.save_csv(df)
        except Exception as e:
            print(f"ERROR scraping {scraper.country_name} ({scraper.source_name}): {e}")


if __name__ == "__main__":
    main()
