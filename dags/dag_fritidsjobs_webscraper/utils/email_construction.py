

from html import escape


def construct_email(job_data: list[dict]) -> tuple[str, str]:
    """
    Constructs the email body for the job listings.

    :param job_data: A list of site dictionaries with nested job lists.
    :return: A tuple containing a subject and a constructed HTML email body.
    """

    # Count only sites that actually have one or more jobs.
    sites_with_jobs = [
        site
        for site in job_data
        if any(job_list.get("items") for job_list in site.get("lists", []))
    ]
    site_count = len(sites_with_jobs)
    job_count = sum(
        len(job_list.get("items", []))
        for site in sites_with_jobs
        for job_list in site.get("lists", [])
    )

    body_parts = [
        "<html><body>",
        (
            f"<p>Der blev fundet <strong>{job_count}</strong> nye jobs i dag fra "
            f"<strong>{site_count}</strong> virksomheder.</p>"
        ),
    ]

    for site in sites_with_jobs:
        site_name = escape(site.get("site_name", "Ukendt virksomhed"))
        body_parts.append(f"<h2>{site_name}</h2>")

        for job_list in site.get("lists", []):
            items = job_list.get("items", [])
            if not items:
                continue

            list_name = escape(job_list.get("list_name", "Ukendt afdeling"))
            body_parts.append(f"<h3>{list_name}</h3>")
            body_parts.append("<ul>")

            for item in items:
                title = escape(item.get("title", "N/A"))
                link = escape(item.get("link", "#"), quote=True)
                body_parts.append(f'<li><a href="{link}">{title}</a></li>')

            body_parts.append("</ul>")

    body_parts.append("</body></html>")
    body = "\n".join(body_parts)

    subject = f"{job_count} nye jobs fundet i dag"

    return subject, body
