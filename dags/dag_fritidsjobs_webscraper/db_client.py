from typing import Any

from dag_fritidsjobs_webscraper.utils.model import Job
from sqlalchemy.orm import Session


def insert_jobs(session: Session, job_data: list[dict[str, Any]]):
    """
    Inserts multiple jobs into the database.

    :param session: Database session
    :param job_data: List of scraped site/list/item structures
    """
    try:
        for site, title, url in _iter_job_rows(job_data):
            new_job = Job(site=site, title=title, url=url)
            session.add(new_job)
        session.commit()
    except Exception:
        session.rollback()
        raise


def filter_existing_jobs(session: Session, job_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Filters out jobs that already exist in the database.

    :param job_data: List of scraped site/list/item structures
    :return: Nested structure containing only new jobs
    """
    try:
        existing_jobs = session.query(Job.title, Job.url).all()
        existing_jobs_set = {(job.title, job.url) for job in existing_jobs}
        seen_new_jobs: set[tuple[str, str]] = set()
        filtered_sites: list[dict[str, Any]] = []

        for site in job_data:
            filtered_lists: list[dict[str, Any]] = []
            for job_list in site.get("lists", []):
                filtered_items: list[dict[str, str]] = []
                for item in job_list.get("items", []):
                    title = item.get("title")
                    url = item.get("link") or item.get("url")
                    if not title or not url:
                        continue

                    job_key = (title, url)
                    if job_key in existing_jobs_set or job_key in seen_new_jobs:
                        continue

                    seen_new_jobs.add(job_key)
                    filtered_items.append({"title": title, "link": url})

                if filtered_items:
                    filtered_lists.append(
                        {
                            "list_name": job_list.get("list_name"),
                            "items": filtered_items,
                        }
                    )

            if filtered_lists:
                filtered_sites.append(
                    {
                        "site_name": site.get("site_name"),
                        "site_url": site.get("site_url"),
                        "lists": filtered_lists,
                    }
                )

        return filtered_sites
    except Exception:
        raise


def _iter_job_rows(job_data: list[dict[str, Any]]) -> list[tuple[str, str, str]]:
    """
    Flatten site/list/item payload into site/title/url tuples.

    :param job_data: List of scraped site/list/item structures
    :return: Flat site/title/url rows
    """
    job_rows: list[tuple[str, str, str]] = []

    for site in job_data:
        site_name = site.get("site_name") or site.get("site_url") or "unknown"
        for job_list in site.get("lists", []):
            for item in job_list.get("items", []):
                title = item.get("title")
                url = item.get("link") or item.get("url")
                if title and url:
                    job_rows.append((site_name, title, url))

    return job_rows
