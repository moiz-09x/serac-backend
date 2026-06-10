import logging

from fastapi import APIRouter
from pydantic import BaseModel

from app.core.config import settings
from app.db import get_driver

log = logging.getLogger(__name__)
router = APIRouter(prefix="/graph", tags=["graph"])


class GraphNode(BaseModel):
    id: str
    type: str
    label: str
    platform: str | None = None


class GraphLink(BaseModel):
    source: str
    target: str
    type: str


class GraphData(BaseModel):
    nodes: list[GraphNode]
    links: list[GraphLink]


@router.get("", response_model=GraphData)
async def get_graph():
    nodes: list[GraphNode] = []
    links: list[GraphLink] = []
    node_ids: set[str] = set()

    async with get_driver().session(database=settings.neo4j_database) as session:
        node_result = await session.run(
            """
            MATCH (n)
            WHERE n.tenant_id = $tenant_id
              AND (n:Actor OR n:Thread OR n:Event)
            WITH n, labels(n)[0] AS lbl
            RETURN
              n.id              AS id,
              lbl               AS type,
              n.name            AS name,
              n.title           AS title,
              n.event_type      AS event_type,
              n.source_platform AS platform
            ORDER BY n.created_at DESC
            LIMIT 300
            """,
            tenant_id=settings.tenant_id,
        )
        async for row in node_result:
            nid = row["id"]
            if not nid:
                continue
            ntype = row["type"] or "Event"
            label = row.get("name") or row.get("title") or row.get("event_type") or str(nid)[:8]
            nodes.append(GraphNode(id=str(nid), type=ntype, label=label, platform=row["platform"]))
            node_ids.add(str(nid))

        if node_ids:
            edge_result = await session.run(
                """
                MATCH (a)-[r]->(b)
                WHERE a.id IN $node_ids AND b.id IN $node_ids
                RETURN a.id AS source, b.id AS target, type(r) AS rel_type
                LIMIT 800
                """,
                node_ids=list(node_ids),
            )
            async for row in edge_result:
                src, tgt = row.get("source"), row.get("target")
                if src and tgt:
                    links.append(
                        GraphLink(
                            source=str(src), target=str(tgt), type=row["rel_type"] or "RELATED"
                        )
                    )

    return GraphData(nodes=nodes, links=links)
