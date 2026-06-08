from enum import Enum


class SourcePlatform(str, Enum):
    SLACK = "Slack"
    HUBSPOT = "HubSpot"
    SALESFORCE = "Salesforce"
    LINEAR = "Linear"
    NOTION = "Notion"


class ThreadStatus(str, Enum):
    ACTIVE = "Active"
    CONCLUDED = "Concluded"
    STALLED = "Stalled"


class GoverningDocType(str, Enum):
    SOW = "SOW"
    INTERNAL_POLICY = "InternalPolicy"
    EXTERNAL_REGULATION = "ExternalRegulation"
    TECHNICAL_SPEC = "TechnicalSpec"
    ACTIVE_RULE = "ActiveRule"


class GoverningDocStatus(str, Enum):
    PENDING = "Pending"
    APPROVED = "Approved"
    REJECTED = "Rejected"
    EXPIRED = "Expired"
    RETIRED = "Retired"


class OutcomeType(str, Enum):
    COMPLETED = "Completed"
    CANCELLED = "Cancelled"
    DEAL_WON = "DealWon"
    DEAL_LOST = "DealLost"


class PrincipalType(str, Enum):
    SLACK_CHANNEL = "SlackChannel"
    LINEAR_TEAM = "LinearTeam"
    HUBSPOT_PIPELINE = "HubSpotPipeline"
    NOTION_PAGE = "NotionPage"
    SALESFORCE_ROLE = "SalesforceRole"


class Visibility(str, Enum):
    PUBLIC = "public"
    PRIVATE = "private"
    RESTRICTED = "restricted"


class ActorStatus(str, Enum):
    VERIFIED = "Verified"
    UNVERIFIED = "Unverified"


class GroupKind(str, Enum):
    CHANNEL = "channel"
    TEAM = "team"
    ROLE = "role"


class SemanticEnrichment(str, Enum):
    COMPLETE = "Complete"
    FAILED = "Failed"
    PENDING = "Pending"
