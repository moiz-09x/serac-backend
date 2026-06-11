from enum import StrEnum


class SourcePlatform(StrEnum):
    SLACK = "Slack"
    HUBSPOT = "HubSpot"
    SALESFORCE = "Salesforce"
    LINEAR = "Linear"
    NOTION = "Notion"


class ThreadStatus(StrEnum):
    ACTIVE = "Active"
    CONCLUDED = "Concluded"
    STALLED = "Stalled"


class GoverningDocType(StrEnum):
    SOW = "SOW"
    INTERNAL_POLICY = "InternalPolicy"
    EXTERNAL_REGULATION = "ExternalRegulation"
    TECHNICAL_SPEC = "TechnicalSpec"
    ACTIVE_RULE = "ActiveRule"


class GoverningDocStatus(StrEnum):
    PENDING = "Pending"
    APPROVED = "Approved"
    REJECTED = "Rejected"
    EXPIRED = "Expired"
    RETIRED = "Retired"


class PrincipalType(StrEnum):
    SLACK_CHANNEL = "SlackChannel"
    LINEAR_TEAM = "LinearTeam"
    HUBSPOT_PIPELINE = "HubSpotPipeline"
    NOTION_PAGE = "NotionPage"
    SALESFORCE_ROLE = "SalesforceRole"


class Visibility(StrEnum):
    PUBLIC = "public"
    PRIVATE = "private"
    RESTRICTED = "restricted"


class ActorStatus(StrEnum):
    VERIFIED = "Verified"
    UNVERIFIED = "Unverified"


class GroupKind(StrEnum):
    CHANNEL = "channel"
    TEAM = "team"
    ROLE = "role"


class SemanticEnrichment(StrEnum):
    COMPLETE = "Complete"
    FAILED = "Failed"
    PENDING = "Pending"
