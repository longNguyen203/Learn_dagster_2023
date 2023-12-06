import json
import requests
import pandas as pd
from dagster import asset, MetadataValue
from dagster import AssetExecutionContext


@asset
def hackernews_top_story_ids() -> None:
    """
        Get top 10 stories from the HackerNews top stories endpoint.
        API Docs: https://github.com/HackerNews/API#new-top-and-best-stories.
    """
    top_story_ids = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json").json()
    print("This is a basic dagster")

    with open("hackernews_top_story_ids.json", "w") as file:
        json.dump(top_story_ids[:10], file)

    
# asset dependencies can be inferred from parameter names
# Nghia la asset hackernews_top_stories phu thuoc vao asset hackernews_top_story_ids
@asset(deps=[hackernews_top_story_ids])
def hackernews_top_stories(context: AssetExecutionContext) -> None:
    """Get items based on story ids from the HackerNews items endpoint."""

    with open("hackernews_top_story_ids.json", "r") as file:
        hackernews_top_story_ids = json.load(file)

    results = []
    for item_id in hackernews_top_story_ids:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)

    df = pd.DataFrame(results)
    df.to_csv("hackernews_top_stories.csv")

    # recorded metadata can be customized
    metadata = {
        "num_records": len(df),
        "preview": MetadataValue.md(df[["title", "by", "url"]].to_markdown()),
    }
    context.add_output_metadata(metadata=metadata)