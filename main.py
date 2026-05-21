import pandas as pd

from settings.config import MAX_LINKS
from crawler import map_sources_from_yaml, scrape_links_to_table
from raw_process import process_raw_article

if __name__ == "__main__":
    # Demo pipeline: yaml -> map_sources_from_yaml -> scrape_links_to_table.
    # Note: each source fires up to `limit` /scrape calls -- mind the Firecrawl quota.
    map_results = map_sources_from_yaml(limit=MAX_LINKS)

    dfs = [scrape_links_to_table(r) for r in map_results]
    df = (
        pd.concat(dfs, ignore_index=True)
        if dfs
        else pd.DataFrame(columns=[
            "source_name",
            "source_url",
            "default_tag",
            "article_url",
            "markdown",
        ])
    )

    # P1.9: drop rows whose scrape failed or returned an empty body so the
    # LLM stage never sees a None/"" markdown.
    df = df.dropna(subset=["markdown"])
    df = df[df["markdown"].astype(str).str.len() > 0].reset_index(drop=True)

    print(f"\nfinal table shape: {df.shape}")
    print(df.head())

    for _, row in df.iterrows():
        output = process_raw_article(
            article_url=row["article_url"],
            raw_article=row["markdown"],
            source_name=row["source_name"],
            default_tag=row["default_tag"],
        )
        print("================================================")
        print(output.model_dump_json(indent=2))
        print("================================================")
