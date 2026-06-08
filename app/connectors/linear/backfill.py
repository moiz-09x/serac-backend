import asyncio
import uuid
from datetime import datetime, timedelta, timezone

from app.connectors.linear.client import LinearClient
from app.connectors.linear.normalizer import (
    comment_to_event,
    issue_to_event,
    state_change_to_event,
)
from app.extraction import pipeline

_TEAMS_QUERY = """
query {
  teams {
    nodes { id name key private }
  }
}
"""

_ISSUES_QUERY = """
query($teamId: ID!, $after: String, $createdAfter: DateTime) {
  issues(
    filter: {
      team: { id: { eq: $teamId } }
      createdAt: { gt: $createdAfter }
    }
    first: 50
    after: $after
    orderBy: createdAt
  ) {
    nodes {
      id title description createdAt
      state { name }
      creator { id name email }
      team { id name private }
      comments(first: 100) {
        nodes { id body createdAt user { id name email } }
      }
      history(first: 50) {
        nodes {
          id createdAt
          fromState { name }
          toState { name }
          actor { id name email }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


async def run(tenant_id: uuid.UUID, api_key: str) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=365)).isoformat()
    async with LinearClient(api_key) as client:
        data = await client.query(_TEAMS_QUERY)
        for team in data["teams"]["nodes"]:
            await _backfill_team(client, team["id"], tenant_id, cutoff)


async def _backfill_team(
    client: LinearClient, team_id: str, tenant_id: uuid.UUID, cutoff: str
) -> None:
    after = None
    while True:
        data = await client.query(
            _ISSUES_QUERY,
            {"teamId": team_id, "after": after, "createdAfter": cutoff},
        )
        page = data["issues"]
        for issue in page["nodes"]:
            await _process_issue(issue, tenant_id)
            await asyncio.sleep(0.1)  # Linear rate limit: ~1500 req/hour
        if not page["pageInfo"]["hasNextPage"]:
            break
        after = page["pageInfo"]["endCursor"]


async def _process_issue(issue: dict, tenant_id: uuid.UUID) -> None:
    await pipeline.run(issue_to_event(issue, tenant_id))
    for comment in issue["comments"]["nodes"]:
        await pipeline.run(comment_to_event(comment, issue, tenant_id))
    for entry in issue["history"]["nodes"]:
        event = state_change_to_event(entry, issue, tenant_id)
        if event:
            await pipeline.run(event)
