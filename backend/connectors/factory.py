from typing import Any
from typing import Type

from backend.configs.constants import DocumentSource
from backend.connectors.bookstack.connector import BookstackConnector
from backend.connectors.confluence.connector import ConfluenceConnector
from backend.connectors.danswer_jira.connector import JiraConnector
from backend.connectors.document360.connector import Document360Connector
from backend.connectors.file.connector import LocalFileConnector
from backend.connectors.github.connector import GithubConnector
from backend.connectors.gitlab.connector import GitlabConnector
from backend.connectors.gmail.connector import GmailConnector
from backend.connectors.gong.connector import GongConnector
from backend.connectors.google_drive.connector import GoogleDriveConnector
from backend.connectors.google_site.connector import GoogleSitesConnector
from backend.connectors.guru.connector import GuruConnector
from backend.connectors.hubspot.connector import HubSpotConnector
from backend.connectors.interfaces import BaseConnector
from backend.connectors.interfaces import EventConnector
from backend.connectors.interfaces import LoadConnector
from backend.connectors.interfaces import PollConnector
from backend.connectors.linear.connector import LinearConnector
from backend.connectors.loopio.connector import LoopioConnector
from backend.connectors.models import InputType
from backend.connectors.notion.connector import NotionConnector
from backend.connectors.productboard.connector import ProductboardConnector
from backend.connectors.requesttracker.connector import RequestTrackerConnector
from backend.connectors.sharepoint.connector import SharepointConnector
from backend.connectors.slab.connector import SlabConnector
from backend.connectors.slack.connector import SlackLoadConnector
from backend.connectors.slack.connector import SlackPollConnector
from backend.connectors.web.connector import WebConnector
from backend.connectors.zendesk.connector import ZendeskConnector
from backend.connectors.zulip.connector import ZulipConnector


class ConnectorMissingException(Exception):
    pass


def identify_connector_class(
    source: DocumentSource,
    input_type: InputType | None = None,
) -> Type[BaseConnector]:
    connector_map = {
        DocumentSource.WEB: WebConnector,
        DocumentSource.FILE: LocalFileConnector,
        DocumentSource.SLACK: {
            InputType.LOAD_STATE: SlackLoadConnector,
            InputType.POLL: SlackPollConnector,
        },
        DocumentSource.GITHUB: GithubConnector,
        DocumentSource.GMAIL: GmailConnector,
        DocumentSource.GITLAB: GitlabConnector,
        DocumentSource.GOOGLE_DRIVE: GoogleDriveConnector,
        DocumentSource.BOOKSTACK: BookstackConnector,
        DocumentSource.CONFLUENCE: ConfluenceConnector,
        DocumentSource.JIRA: JiraConnector,
        DocumentSource.PRODUCTBOARD: ProductboardConnector,
        DocumentSource.SLAB: SlabConnector,
        DocumentSource.NOTION: NotionConnector,
        DocumentSource.ZULIP: ZulipConnector,
        DocumentSource.REQUESTTRACKER: RequestTrackerConnector,
        DocumentSource.GURU: GuruConnector,
        DocumentSource.LINEAR: LinearConnector,
        DocumentSource.HUBSPOT: HubSpotConnector,
        DocumentSource.DOCUMENT360: Document360Connector,
        DocumentSource.GONG: GongConnector,
        DocumentSource.GOOGLE_SITES: GoogleSitesConnector,
        DocumentSource.ZENDESK: ZendeskConnector,
        DocumentSource.LOOPIO: LoopioConnector,
        DocumentSource.SHAREPOINT: SharepointConnector,
    }
    connector_by_source = connector_map.get(source, {})

    if isinstance(connector_by_source, dict):
        if input_type is None:
            # If not specified, default to most exhaustive update
            connector = connector_by_source.get(InputType.LOAD_STATE)
        else:
            connector = connector_by_source.get(input_type)
    else:
        connector = connector_by_source
    if connector is None:
        raise ConnectorMissingException(f"Connector not found for source={source}")

    if any(
        [
            input_type == InputType.LOAD_STATE
            and not issubclass(connector, LoadConnector),
            input_type == InputType.POLL and not issubclass(connector, PollConnector),
            input_type == InputType.EVENT and not issubclass(connector, EventConnector),
        ]
    ):
        raise ConnectorMissingException(
            f"Connector for source={source} does not accept input_type={input_type}"
        )

    return connector


def instantiate_connector(
    source: DocumentSource,
    input_type: InputType,
    connector_specific_config: dict[str, Any],
    credentials: dict[str, Any],
) -> tuple[BaseConnector, dict[str, Any] | None]:
    connector_class = identify_connector_class(source, input_type)
    connector = connector_class(**connector_specific_config)
    new_credentials = connector.load_credentials(credentials)

    return connector, new_credentials
