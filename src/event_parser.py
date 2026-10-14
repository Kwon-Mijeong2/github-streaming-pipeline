from typing import Any


def parse_github_event(
    raw_event: dict[str, Any],
) -> dict[str, Any]:

    actor = raw_event.get("actor") or {}
    repo = raw_event.get("repo") or {}

    return {
        "event_id": raw_event.get("id"),
        "event_type": raw_event.get("type"),
        "actor_id": actor.get("id"),
        "actor_login": actor.get("login"),
        "repo_id": repo.get("id"),
        "repo_name": repo.get("name"),
        "is_public": raw_event.get("public"),
        "created_at": raw_event.get("created_at"),
    }