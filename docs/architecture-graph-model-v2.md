# Graph Model v2 — Architecture Specification

> **Status:** Under review — not yet implemented  
> **Purpose:** Reference document for the architectural leap from semantic thread grouping to platform-native threads with cross-thread linking. Covers the rationale, full spec, schema changes, pipeline changes, and retrieval implications.

---

## Table of Contents

1. [Background & What We Are Building](#1-background--what-we-are-building)
2. [Current Architecture](#2-current-architecture)
3. [Problems with the Current Model](#3-problems-with-the-current-model)
4. [New Architecture — Platform-Native Threads](#4-new-architecture--platform-native-threads)
5. [Cross-Thread Linking](#5-cross-thread-linking)
6. [Outcomes as Decision Records](#6-outcomes-as-decision-records)
7. [Google Meet Integration (Planned)](#7-google-meet-integration-planned)
8. [Complete Schema Changes](#8-complete-schema-changes)
9. [Pipeline Changes](#9-pipeline-changes)
10. [Connector / Normalizer Changes](#10-connector--normalizer-changes)
11. [Retrieval Implications](#11-retrieval-implications)
12. [Migration Considerations](#12-migration-considerations)
13. [Why This Is Better](#13-why-this-is-better)

---

## 1. Background & What We Are Building

Serac is a decision intelligence backend that ingests activity from multiple communication and work platforms (Slack, Linear, Notion, Google Meet, etc.), organises it into a graph, and surfaces it for retrieval, pattern analysis, and outcome tracking.

The core idea: every action taken by every person across every tool leaves a trace. Serac captures those traces, stitches related ones together, and lets you query across all of them as if they lived in one place — answering questions like "what led to this outcome?", "who drove this decision?", and "what is happening with this customer/feature/deal right now?"

---

## 2. Current Architecture

### 2.1 Graph Structure (v1)

```
Actor -[:EXECUTED {timestamp}]-> Event -[:PART_OF {timestamp, confidence}]-> Thread
                                      |
                                      -[:VISIBLE_TO]-> Group (SlackChannel/LinearTeam/...)

Thread -[:RESULTED_IN {timestamp}]-> Outcome
```

### 2.2 Node Definitions (v1)

| Node | Key Fields |
|---|---|
| `Actor` | id, tenant_id, resolved_identities (dict), status |
| `Event` | id, tenant_id, source_platform, native_id, timestamp, event_type, delta_fields, embedding |
| `Thread` | id, tenant_id, status (ACTIVE/STALLED/CONCLUDED), resolved_at |
| `Group` | id, tenant_id, source_platform, native_id, kind (CHANNEL/TEAM/PAGE), name |
| `Outcome` | id, tenant_id, type (COMPLETED/CANCELLED/DEAL_WON/DEAL_LOST), duration_ms |

### 2.3 Ingestion Pipeline (v1)

```
Platform event
      │
      ▼
Normalizer → CanonicalEvent
      │
      ▼
Pipeline Stage 1: Deduplication (Redis, SHA256, 24h TTL)
      │
      ▼
Pipeline Stage 2: Identity Resolution (Neo4j — platform UID → actor)
      │
      ▼
Pipeline Stage 3: Thread Attachment
      │   ├── Scenario A: Explicit parent pointer (e.g. Slack reply → parent thread ts)
      │   └── Scenario B: Cosine similarity search (threshold 0.65, top-5 nearest events)
      │
      ▼
Pipeline Stage 4: Graph Write (Event node + EXECUTED + PART_OF + VISIBLE_TO edges)
      │
      ▼
Pipeline Stage 5 (conditional): Outcome creation if event.outcome_signal is set
```

### 2.4 Connector Structure (v1)

```
app/connectors/
  slack/
    client.py       Slack Web API client
    socket.py       Socket Mode real-time listener
    normalizer.py   Slack message → CanonicalEvent
    backfill.py     Historical sync via REST pagination
  linear/
    client.py       GraphQL client
    normalizer.py   Issue / Comment / StateChange → CanonicalEvent
    backfill.py     Historical sync via GraphQL pagination
```

### 2.5 How Outcomes Work Today

Outcomes are triggered exclusively by Linear state changes. When a Linear issue moves to a state matching hardcoded strings (`done`, `completed`, `resolved`, `cancelled`, etc.), the normalizer sets `outcome_signal` on the `CanonicalEvent`. The pipeline then calls `_create_outcome`, which:

1. Looks up `Thread.created_at`
2. Calculates `duration_ms` = `now - thread.created_at`
3. Creates `Outcome {type, duration_ms}` and the edge `Thread -[:RESULTED_IN]-> Outcome`
4. Sets `Thread.status = CONCLUDED`

```python
# Current Outcome model — the entirety of what it stores
class Outcome(NodeBase):
    type: OutcomeType        # COMPLETED / CANCELLED / DEAL_WON / DEAL_LOST
    duration_ms: int | None  # time from thread creation to closure — nothing else
```

---

## 3. Problems with the Current Model

### 3.1 The Central Flaw: One Question Answered Two Ways

The pipeline conflates two fundamentally different questions using the same mechanism (cosine similarity):

1. **"Does this event belong to this conversation?"**  
   This is a *structural* question. The platform already knows the answer. A Slack reply belongs to its parent thread. A Linear comment belongs to its issue. There is no ambiguity.

2. **"Is this conversation related to that conversation?"**  
   This is a *semantic* question. A Slack thread about a customer might relate to a Linear issue about a bug they reported. This requires semantic reasoning.

The current model uses cosine similarity for both. This means the first question — which should be answered deterministically — gets a probabilistic answer that can be wrong.

### 3.2 Multi-Topic Threads

A single Linear issue may generate a Slack discussion, be mentioned in a meeting, and have a Notion doc written about it. In the current model, all of those events get forced into a single thread via similarity matching. The thread becomes a mixed bag with no clear identity.

### 3.3 Outcomes Are Thin State Flags

The current Outcome node is barely more useful than a boolean. It carries no semantic content, no narrative, no cross-platform signal. It answers "did this close?" but not "what was decided?", "why did it close?", or "what was the journey?". It is linked to one thread only, which means it misses everything that happened on other platforms.

### 3.4 Thread Identity Is Implicit

A "thread" in v1 is an emergent cluster — the pipeline decided to group these events together based on similarity. There is no direct correspondence to a real artifact that a human would recognise as a thing. This makes it hard to reason about ("which thread is the Acme deal thread?") and hard to maintain (similarity thresholds drift).

---

## 4. New Architecture — Platform-Native Threads

### 4.1 Core Principle

> **Every natural conversation unit on a platform becomes its own Thread.**

The mapping is explicit and deterministic:

| Platform Artifact | Thread |
|---|---|
| Slack thread (parent message + replies) | 1 Thread |
| Linear issue (+ comments, state changes) | 1 Thread |
| Notion page (+ comments, edits) | 1 Thread |
| Google Meet meeting (+ transcript segments) | 1 Thread |

Events within a thread are determined by the platform's own structure — no similarity involved. A Slack reply always belongs to its parent thread. A Linear comment always belongs to its issue. This is now deterministic.

### 4.2 Thread Identity

Each Thread gets a `platform_native_id` and `source_platform` field that uniquely identifies the real-world artifact it represents. Thread creation becomes idempotent: `MERGE (t:Thread {tenant_id, source_platform, platform_native_id})`.

```python
class Thread(NodeBase):
    # existing
    status: ThreadStatus
    resolved_at: datetime | None

    # new
    source_platform: SourcePlatform      # SLACK, LINEAR, NOTION, GOOGLE_MEET
    platform_native_id: str              # Slack thread_ts, Linear issue id, etc.
    title: str | None                    # human-readable label (issue title, meeting name, etc.)
    anchor_event_type: str | None        # "Message" / "IssueCreated" / "Meeting" etc.
```

### 4.3 New Graph Structure

```
Actor -[:EXECUTED {timestamp}]-> Event -[:PART_OF {timestamp}]-> Thread
                                      |
                                      -[:VISIBLE_TO]-> Group

Thread -[:RELATES_TO {confidence, source, linked_at}]-> Thread   ← NEW

Thread -[:RESULTED_IN {role}]-> Outcome                           ← EXTENDED
```

---

## 5. Cross-Thread Linking

### 5.1 The `RELATES_TO` Edge

This edge connects Thread nodes that are about the same subject across platforms (or even within a platform, for different conversations about the same topic).

```
Thread -[:RELATES_TO {
    confidence: float,       # cosine similarity score or 1.0 for explicit
    source: str,             # "semantic" | "explicit"
    linked_at: datetime
}]-> Thread
```

### 5.2 When Links Are Created

**Automatic (semantic):** After a Thread is created or a new Event is added to it, a post-ingestion job:

1. Embeds the Thread as a whole (aggregate of its Event embeddings, or a dedicated thread-level embedding)
2. Runs cosine similarity against other active Threads (threshold configurable, e.g. 0.72)
3. Creates `RELATES_TO` edges for matches above the threshold

**Explicit (future):** A user or an admin API call directly links two threads. `source = "explicit"`, `confidence = 1.0`.

### 5.3 Multi-Hop Traversal

`RELATES_TO` edges are traversable. A retrieval query can follow chains of related threads to surface the full context of a topic across platforms. For example:

```
"Show me everything about the Acme deal"
  → find Thread (Linear issue: "Acme contract")
  → RELATES_TO → Thread (Slack: "acme next steps")
  → RELATES_TO → Thread (Meet: "Acme call 2024-11-15")
  → RELATES_TO → Thread (Notion: "Acme proposal v2")
```

**Traversal depth:** Multi-hop is supported and encouraged. A decay factor on confidence (e.g. multiply by 0.9 per hop) keeps distant connections from dominating retrieval scores.

### 5.4 Separation of Concerns

| Concern | Mechanism |
|---|---|
| Event belongs to Thread | Deterministic — platform-defined parent pointer |
| Thread relates to Thread | Probabilistic — cosine similarity at thread level |

Cosine similarity moves *up one level* — from event-to-thread to thread-to-thread. Thread-level embeddings are richer, more stable, and more semantically coherent than individual event embeddings, making similarity matching significantly more reliable.

---

## 6. Outcomes as Decision Records

### 6.1 What an Outcome Should Be

An Outcome is the answer to: **"What was decided, achieved, or concluded — and what was the full journey that led there?"**

It is not a state flag on one thread. It is a record that aggregates the entire thread cluster that converged on a conclusion.

### 6.2 New Outcome Schema

```python
class Outcome(NodeBase):
    # existing
    type: OutcomeType                    # COMPLETED / CANCELLED / DEAL_WON / DEAL_LOST

    # extended
    summary: str | None                  # what was decided/achieved (rule-based now, LLM later)
    duration_ms: int | None              # from FIRST event in entire cluster to outcome
    contributing_thread_count: int       # how many threads fed into this outcome
    trigger_platform: SourcePlatform     # which platform event triggered closure
    trigger_native_id: str | None        # the native ID of the triggering event
```

### 6.3 New Outcome Edges

The `RESULTED_IN` edge gains a `role` property and can now connect multiple threads to one Outcome:

```cypher
// The thread whose event triggered the outcome
Thread -[:RESULTED_IN {role: "trigger", timestamp}]-> Outcome

// All threads in the RELATES_TO cluster at time of outcome
Thread -[:RESULTED_IN {role: "contributing", timestamp}]-> Outcome
```

### 6.4 How an Outcome Is Created (New Flow)

1. A platform event fires with `outcome_signal` set (e.g. Linear issue → Done)
2. Find the trigger Thread for this event
3. Traverse all `RELATES_TO` edges from the trigger Thread — collect the full cluster
4. Find the earliest `created_at` across all Events in the entire cluster → `first_event_at`
5. `duration_ms = now - first_event_at` (measures the true lifespan of the work)
6. Create the Outcome node
7. Link trigger Thread with `role: "trigger"`
8. Link all other cluster threads with `role: "contributing"`
9. Set all cluster threads to `status: CONCLUDED`
10. Generate `summary` (see below)

### 6.5 Summary Generation

**Phase 1 (rule-based):**
```
"{trigger_platform} artifact '{thread_title}' concluded as {outcome_type} after 
{contributing_thread_count} related threads and {total_event_count} events 
over {duration_days} days."
```

**Phase 2 (LLM-enriched, future):** Pass the key events from all contributing threads to an LLM to generate a semantic narrative of what happened and why.

### 6.6 What This Unlocks

| Query | How it works |
|---|---|
| "Why did we win the Acme deal?" | Find DEAL_WON Outcome for Acme → traverse contributing threads → timeline of key events |
| "Why do deals get cancelled?" | Aggregate CANCELLED Outcomes → find patterns in contributing thread clusters |
| "How long does it take us to ship a feature?" | Aggregate `duration_ms` across COMPLETED Outcomes |
| "What were the key decisions last month?" | Query Outcomes by date range → each surfaces its full cluster |
| "Who drives successful deals?" | Traverse Actor → Event → Thread → Outcome(DEAL_WON) |

---

## 7. Google Meet Integration (Planned)

### 7.1 Why Not Google's Own API

Google Meet's native transcription and the Meet REST API (`conferenceRecords`) both require **Google Workspace (paid)**. The majority of Google Meet users are on the free tier and have no programmatic access to their transcripts via Google's own APIs. This makes a direct Google integration non-viable for most users.

### 7.2 Recommended Approach: Recall.ai

[Recall.ai](https://recall.ai) is a developer infrastructure service that joins video meetings as a bot participant — similar to a human notetaker being added to a meeting. It works without requiring Workspace or any special admin permissions from the meeting host.

**Platform coverage:** Zoom, Google Meet (free & paid), Microsoft Teams, Webex, Slack Huddles.

**How it works:**
1. User connects their Google Calendar via OAuth (free, no Workspace needed)
2. Our system detects upcoming Meet links in calendar events
3. We instruct Recall.ai to send a bot to the meeting
4. Post-meeting: Recall.ai fires a webhook with the structured transcript (speaker, text, timestamps)
5. Our normalizer processes the transcript → Events → MeetingThread

**Why this is the right level:** Most consumer-facing transcription tools (Fireflies, Fathom, Grain) use Recall.ai as their own backend infrastructure. Building on Recall.ai directly gives more control with less overhead.

### 7.3 Meeting → Graph Mapping

| Meeting Concept | Graph Node |
|---|---|
| Meeting | Thread (`source_platform: GOOGLE_MEET`, `platform_native_id: recall_bot_id`) |
| Speaker turn (one person speaking continuously) | Event (`event_type: "SpeakerTurn"`) |
| Meeting participant | Actor (resolved via email from calendar invite) |
| Meeting end | Outcome trigger (type: COMPLETED) |

### 7.4 How Meetings Link to Other Threads

A meeting rarely exists in isolation. After a meeting is fully ingested:

1. The pipeline embeds the Meeting Thread as a whole
2. Runs `RELATES_TO` similarity against active threads
3. The meeting thread links to any related Linear issues, Slack conversations, or Notion pages

This means "find everything related to the Acme deal" surfaces the meeting where it was discussed without any manual tagging.

### 7.5 Connector Structure (Planned)

```
app/connectors/
  google_meet/
    client.py       Recall.ai API client (schedule bot, query transcript)
    normalizer.py   Recall.ai transcript → CanonicalEvent (per speaker turn)
    backfill.py     Historical meetings sync (if Recall.ai has stored recordings)
  
app/api/routes/webhooks.py
  POST /webhooks/recall    Recall.ai post-meeting transcript webhook
```

### 7.6 Required Additions to Enums

```python
class SourcePlatform(str, Enum):
    SLACK = "Slack"
    LINEAR = "Linear"
    NOTION = "Notion"
    GOOGLE_MEET = "GoogleMeet"    # new
    HUBSPOT = "HubSpot"
    SALESFORCE = "Salesforce"

class PrincipalType(str, Enum):
    SLACK_CHANNEL = "SlackChannel"
    LINEAR_TEAM = "LinearTeam"
    NOTION_PAGE = "NotionPage"
    GOOGLE_MEET_SPACE = "GoogleMeetSpace"    # new
    HUBSPOT_PIPELINE = "HubSpotPipeline"
    SALESFORCE_ROLE = "SalesforceRole"
```

---

## 8. Complete Schema Changes

### 8.1 `Thread` — Extended

```python
class Thread(NodeBase):
    # existing — no changes
    status: ThreadStatus                 # ACTIVE / STALLED / CONCLUDED
    resolved_at: datetime | None

    # new fields
    source_platform: SourcePlatform      # which platform this thread lives on
    platform_native_id: str              # the platform's own ID for this artifact
    title: str | None                    # human-readable (issue title, meeting name, channel+ts)
    anchor_event_type: str | None        # the event_type of the first/defining event
```

### 8.2 `Outcome` — Extended

```python
class Outcome(NodeBase):
    # existing
    type: OutcomeType

    # extended (duration_ms meaning changes — now measures full cluster lifespan)
    duration_ms: int | None

    # new fields
    summary: str | None
    contributing_thread_count: int
    trigger_platform: SourcePlatform
    trigger_native_id: str | None
```

### 8.3 New Edge Type: `RELATES_TO`

```cypher
(t1:Thread)-[:RELATES_TO {
    confidence: float,       // 0.0–1.0
    source: string,          // "semantic" | "explicit"
    linked_at: datetime
}]->(t2:Thread)
```

### 8.4 Extended Edge Type: `RESULTED_IN`

```cypher
// before
(t:Thread)-[:RESULTED_IN {timestamp}]->(o:Outcome)

// after
(t:Thread)-[:RESULTED_IN {
    role: string,            // "trigger" | "contributing"
    timestamp: datetime
}]->(o:Outcome)
```

### 8.5 New Neo4j Indexes Required

```cypher
// For fast thread lookup by platform artifact
CREATE INDEX thread_platform_native IF NOT EXISTS
FOR (t:Thread) ON (t.tenant_id, t.source_platform, t.platform_native_id);

// For RELATES_TO traversal filtering by confidence
CREATE INDEX relates_to_confidence IF NOT EXISTS
FOR ()-[r:RELATES_TO]-() ON (r.confidence);
```

---

## 9. Pipeline Changes

### 9.1 `_ensure_thread` — Rewritten

**Current behaviour:** Cosine similarity search to find the best-matching thread; create new if no match above threshold.

**New behaviour:** Deterministic lookup by `(tenant_id, source_platform, platform_native_id)`.

```python
async def _ensure_thread(event: CanonicalEvent, tenant_id: str) -> uuid.UUID:
    # Every CanonicalEvent now carries source_platform and platform_thread_id
    # (set by the normalizer — see Section 10)
    
    platform = event.metadata.source_platform.value
    native_thread_id = event.metadata.platform_thread_id  # new field on EventMetadata
    
    async with get_driver().session(...) as session:
        result = await session.run("""
            MERGE (t:Thread {
                tenant_id: $tid,
                source_platform: $platform,
                platform_native_id: $native_id
            })
            ON CREATE SET
                t.id = $new_id,
                t.status = $active,
                t.created_at = $now,
                t.title = $title
            RETURN t.id AS thread_id
        """, ...)
        record = await result.single()
        return uuid.UUID(record["thread_id"])
```

No cosine similarity. No fallback. The platform tells us which thread this event belongs to.

### 9.2 New Pipeline Stage: Thread Linking (Post-Ingestion)

After a thread receives its first event (or periodically as events accumulate), a background job runs:

```python
async def _link_related_threads(
    thread_id: uuid.UUID,
    tenant_id: str,
    threshold: float = 0.72,
) -> None:
    # 1. Compute thread-level embedding (mean of all event embeddings in thread)
    # 2. Vector similarity search against all other active threads for this tenant
    # 3. For matches above threshold, create RELATES_TO edges
    # 4. Skip if edge already exists with equal or higher confidence
```

This job is enqueued as a background Arq task, decoupled from the hot ingestion path.

### 9.3 `_create_outcome` — Rewritten

```python
async def _create_outcome(
    trigger_thread_id: uuid.UUID,
    tenant_id: str,
    outcome_type: OutcomeType,
    trigger_platform: SourcePlatform,
    trigger_native_id: str,
) -> None:
    # 1. Find all threads in the RELATES_TO cluster (BFS from trigger thread)
    # 2. Find earliest event.created_at across all threads in cluster
    # 3. Calculate duration_ms from earliest event to now
    # 4. Create Outcome node with summary, contributing_thread_count, etc.
    # 5. Link trigger thread with role="trigger"
    # 6. Link all other cluster threads with role="contributing"
    # 7. Set all cluster threads to CONCLUDED
```

### 9.4 Updated `EventMetadata`

```python
class EventMetadata(BaseModel):
    # existing
    tenant_id: uuid.UUID
    source_platform: SourcePlatform
    native_event_id: str
    timestamp: datetime
    event_type: str

    # changed: parent_native_id is now split into two clearer fields
    platform_thread_id: str          # the native ID of the thread this event belongs to
    parent_event_id: str | None      # for intra-thread parent (e.g. a reply within a thread)
```

---

## 10. Connector / Normalizer Changes

Each normalizer must now emit `platform_thread_id` on every event. The thread ID is always known deterministically from the platform's own data structure.

### 10.1 Slack

```python
# platform_thread_id = thread_ts (parent message timestamp)
# For a parent message: platform_thread_id = message["ts"]
# For a reply: platform_thread_id = message["thread_ts"]
# parent_event_id = message["thread_ts"] if this is a reply, else None
```

Thread `title` = `f"#{channel_name}"` or `f"#{channel_id}"` if name unavailable.

### 10.2 Linear

```python
# platform_thread_id = issue["id"]  (always — comments, state changes all belong to the issue)
# parent_event_id = None  (Linear issues don't have intra-thread parents in our model)
```

Thread `title` = `issue["title"]`

### 10.3 Notion

```python
# platform_thread_id = page["id"]  (all page events belong to the page thread)
# parent_event_id = None
```

Thread `title` = `page["title"]` or `page["properties"]["Name"]`

### 10.4 Google Meet (New)

```python
# platform_thread_id = recall_bot_id or conference_id
# Each speaker turn is one Event
# parent_event_id = None (turns don't nest)
```

Thread `title` = meeting title from calendar event.

---

## 11. Retrieval Implications

### 11.1 Thread as the Primary Retrieval Unit

In v1, retrieval starts from Events. In v2, the primary retrieval unit shifts to **Threads**. A Thread is a real artifact (a conversation, an issue, a meeting) with identity, history, and relationships. Retrieval finds the relevant Threads first, then surfaces Events within them.

### 11.2 Multi-Hop Thread Traversal

```cypher
// "Find everything related to the Acme deal, up to 3 hops"
MATCH path = (seed:Thread {platform_native_id: $acme_issue_id})
             -[:RELATES_TO*1..3]->
             (related:Thread)
WHERE ALL(r IN relationships(path) WHERE r.confidence > 0.65)
RETURN related, 
       REDUCE(score = 1.0, r IN relationships(path) | score * r.confidence) AS path_score
ORDER BY path_score DESC
```

Confidence decays multiplicatively across hops — distant connections score lower automatically.

### 11.3 Outcome-Anchored Retrieval

```cypher
// "What led to this outcome?"
MATCH (o:Outcome {id: $outcome_id})<-[:RESULTED_IN]-(t:Thread)
MATCH (t)<-[:PART_OF]-(e:Event)<-[:EXECUTED]-(a:Actor)
RETURN t.title, t.source_platform, collect(e) AS events, collect(a) AS actors
ORDER BY t.source_platform
```

### 11.4 Cross-Platform Topic Aggregation

```cypher
// "What is everything happening around feature X right now?"
MATCH (seed:Thread)
WHERE seed.title CONTAINS $feature_name OR 
      EXISTS { MATCH (seed)<-[:PART_OF]-(e:Event) WHERE e.text CONTAINS $feature_name }
MATCH (seed)-[:RELATES_TO*1..2]-(related:Thread)
WHERE related.status = 'Active'
RETURN seed, related
```

---

## 12. Migration Considerations

### 12.1 Existing Data

Threads ingested under v1 do not have `source_platform` or `platform_native_id`. Options:

1. **Backfill:** Re-run normalizers against stored raw events to reconstruct platform thread IDs. Feasible if raw events are stored.
2. **Mark as legacy:** Add a `schema_version: "v1"` flag to old threads and exclude them from `RELATES_TO` linking until manually reviewed.
3. **Clean slate:** If data volume is low enough, drop and re-ingest. Recommended for early-stage.

### 12.2 Rollout Order

1. Add `platform_native_id` and `source_platform` to Thread schema (backward compatible — nullable initially)
2. Update normalizers to emit `platform_thread_id` on all events
3. Rewrite `_ensure_thread` to use deterministic lookup
4. Add `RELATES_TO` edge and thread-linking background job
5. Extend Outcome schema and rewrite `_create_outcome`
6. Add new Neo4j indexes
7. Update retrieval layer to use thread-first traversal

---

## 13. Why This Is Better

| Dimension | v1 | v2 |
|---|---|---|
| Thread identity | Emergent cluster, no real-world correspondence | Explicit platform artifact — every thread IS something |
| Thread assignment | Probabilistic cosine similarity (can be wrong) | Deterministic platform-defined (always correct) |
| Cross-platform linking | Not supported | First-class `RELATES_TO` edges with confidence |
| Multi-hop traversal | Not possible | Supported, confidence-decayed |
| Outcome richness | State flag on one thread | Decision record across full thread cluster |
| Outcome duration | Thread creation → close (one platform) | First signal anywhere in cluster → close (all platforms) |
| Retrieval unit | Events | Threads (with events accessible within) |
| Mental model | "Semantic clusters we computed" | "Real conversations that happened, and how they relate" |
| Debuggability | "Why is this event in this thread?" — hard to answer | "This event is in this thread because the platform says so" |
| Future extensibility | Adding platforms requires tuning similarity | Adding platforms requires only a normalizer |
