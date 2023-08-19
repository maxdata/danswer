import datetime
import io
import tempfile
from collections.abc import Generator
from collections.abc import Sequence
from itertools import chain
from typing import Any

import docx2txt  # type:ignore
from google.oauth2.credentials import Credentials  # type: ignore
from googleapiclient import discovery  # type: ignore
from PyPDF2 import PdfReader

from danswer.configs.app_configs import GOOGLE_DRIVE_FOLLOW_SHORTCUTS
from danswer.configs.app_configs import GOOGLE_DRIVE_INCLUDE_SHARED
from danswer.configs.app_configs import INDEX_BATCH_SIZE
from danswer.configs.constants import DocumentSource
from danswer.connectors.google_drive.connector_auth import DB_CREDENTIALS_DICT_KEY
from danswer.connectors.google_drive.connector_auth import get_drive_tokens
from danswer.connectors.interfaces import GenerateDocumentsOutput
from danswer.connectors.interfaces import LoadConnector
from danswer.connectors.interfaces import PollConnector
from danswer.connectors.interfaces import SecondsSinceUnixEpoch
from danswer.connectors.models import Document
from danswer.connectors.models import Section
from danswer.utils.batching import batch_generator
from danswer.utils.logger import setup_logger

logger = setup_logger()

# allow 10 minutes for modifiedTime to get propogated
DRIVE_START_TIME_OFFSET = 60 * 10
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]
SUPPORTED_DRIVE_DOC_TYPES = [
    "application/vnd.google-apps.document",
    "application/vnd.google-apps.spreadsheet",
    "application/pdf",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
]
DRIVE_FOLDER_TYPE = "application/vnd.google-apps.folder"
DRIVE_SHORTCUT_TYPE = "application/vnd.google-apps.shortcut"

GoogleDriveFileType = dict[str, Any]


def _run_drive_file_query(
    service: discovery.Resource,
    query: str,
    include_shared: bool = GOOGLE_DRIVE_INCLUDE_SHARED,
    follow_shortcuts: bool = GOOGLE_DRIVE_FOLLOW_SHORTCUTS,
    batch_size: int = INDEX_BATCH_SIZE,
) -> Generator[GoogleDriveFileType, None, None]:
    next_page_token = ""
    while next_page_token is not None:
        logger.debug(f"Running Google Drive fetch with query: {query}")
        results = (
            service.files()
            .list(
                pageSize=batch_size,
                supportsAllDrives=include_shared,
                includeItemsFromAllDrives=include_shared,
                fields=(
                    "nextPageToken, files(mimeType, id, name, "
                    "webViewLink, shortcutDetails)"
                ),
                pageToken=next_page_token,
                q=query,
            )
            .execute()
        )
        next_page_token = results.get("nextPageToken")
        files = results["files"]
        for file in files:
            if follow_shortcuts and "shortcutDetails" in file:
                file = service.files().get(
                    fileId=file["shortcutDetails"]["targetId"],
                    supportsAllDrives=include_shared,
                    fields="mimeType, id, name, webViewLink, shortcutDetails",
                )
                file = file.execute()
            yield file


def _get_folder_id(
    service: discovery.Resource,
    parent_id: str,
    folder_name: str,
    include_shared: bool,
    follow_shortcuts: bool,
) -> str | None:
    """
    Get the ID of a folder given its name and the ID of its parent folder.
    """
    query = f"'{parent_id}' in parents and name='{folder_name}' and "
    if follow_shortcuts:
        query += f"(mimeType='{DRIVE_FOLDER_TYPE}' or mimeType='{DRIVE_SHORTCUT_TYPE}')"
    else:
        query += f"mimeType='{DRIVE_FOLDER_TYPE}'"

    results = (
        service.files()
        .list(
            q=query,
            spaces="drive",
            fields="nextPageToken, files(id, name, shortcutDetails)",
            supportsAllDrives=include_shared,
            includeItemsFromAllDrives=include_shared,
        )
        .execute()
    )
    items = results.get("files", [])

    folder_id = None
    if items:
        if follow_shortcuts and "shortcutDetails" in items[0]:
            folder_id = items[0]["shortcutDetails"]["targetId"]
        else:
            folder_id = items[0]["id"]
    return folder_id


def _get_folders(
    service: discovery.Resource,
    folder_id: str | None = None,  # if specified, only fetches files within this folder
    include_shared: bool = GOOGLE_DRIVE_INCLUDE_SHARED,
    follow_shortcuts: bool = GOOGLE_DRIVE_FOLLOW_SHORTCUTS,
    batch_size: int = INDEX_BATCH_SIZE,
) -> Generator[GoogleDriveFileType, None, None]:
    query = f"mimeType = '{DRIVE_FOLDER_TYPE}' "
    if follow_shortcuts:
        query = "(" + query + f" or mimeType = '{DRIVE_SHORTCUT_TYPE}'" + ") "

    if folder_id:
        query += f"and '{folder_id}' in parents "
    query = query.rstrip()  # remove the trailing space(s)

    for file in _run_drive_file_query(
        service=service,
        query=query,
        include_shared=include_shared,
        follow_shortcuts=follow_shortcuts,
        batch_size=batch_size,
    ):
        # Need to check this since file may have been a target of a shortcut
        # and not necessarily a folder
        if file["mimeType"] == DRIVE_FOLDER_TYPE:
            yield file
        else:
            pass


def _get_files(
    service: discovery.Resource,
    time_range_start: SecondsSinceUnixEpoch | None = None,
    time_range_end: SecondsSinceUnixEpoch | None = None,
    folder_id: str | None = None,  # if specified, only fetches files within this folder
    include_shared: bool = GOOGLE_DRIVE_INCLUDE_SHARED,
    follow_shortcuts: bool = GOOGLE_DRIVE_FOLLOW_SHORTCUTS,
    supported_drive_doc_types: list[str] = SUPPORTED_DRIVE_DOC_TYPES,
    batch_size: int = INDEX_BATCH_SIZE,
) -> Generator[GoogleDriveFileType, None, None]:
    query = f"mimeType != '{DRIVE_FOLDER_TYPE}' "
    if time_range_start is not None:
        time_start = (
            datetime.datetime.utcfromtimestamp(time_range_start).isoformat() + "Z"
        )
        query += f"and modifiedTime >= '{time_start}' "
    if time_range_end is not None:
        time_stop = datetime.datetime.utcfromtimestamp(time_range_end).isoformat() + "Z"
        query += f"and modifiedTime <= '{time_stop}' "
    if folder_id:
        query += f"and '{folder_id}' in parents "
    query = query.rstrip()  # remove the trailing space(s)

    files = _run_drive_file_query(
        service=service,
        query=query,
        include_shared=include_shared,
        follow_shortcuts=follow_shortcuts,
        batch_size=batch_size,
    )
    for file in files:
        if file["mimeType"] in supported_drive_doc_types:
            yield file


def get_all_files_batched(
    service: discovery.Resource,
    include_shared: bool = GOOGLE_DRIVE_INCLUDE_SHARED,
    follow_shortcuts: bool = GOOGLE_DRIVE_FOLLOW_SHORTCUTS,
    batch_size: int = INDEX_BATCH_SIZE,
    time_range_start: SecondsSinceUnixEpoch | None = None,
    time_range_end: SecondsSinceUnixEpoch | None = None,
    folder_id: str | None = None,  # if specified, only fetches files within this folder
    # if True, will fetch files in sub-folders of the specified folder ID.
    # Only applies if folder_id is specified.
    traverse_subfolders: bool = True,
    folder_ids_traversed: list[str] | None = None,
) -> Generator[list[GoogleDriveFileType], None, None]:
    """Gets all files matching the criteria specified by the args from Google Drive
    in batches of size `batch_size`.
    """
    valid_files = _get_files(
        service=service,
        time_range_start=time_range_start,
        time_range_end=time_range_end,
        folder_id=folder_id,
        include_shared=include_shared,
        follow_shortcuts=follow_shortcuts,
        batch_size=batch_size,
    )
    yield from batch_generator(
        items=valid_files,
        batch_size=batch_size,
        pre_batch_yield=lambda batch_files: logger.info(
            f"Parseable Documents in batch: {[file['name'] for file in batch_files]}"
        ),
    )

    if traverse_subfolders:
        folder_ids_traversed = folder_ids_traversed or []
        subfolders = _get_folders(
            service=service,
            folder_id=folder_id,
            include_shared=include_shared,
            follow_shortcuts=follow_shortcuts,
            batch_size=batch_size,
        )
        for subfolder in subfolders:
            if subfolder["id"] not in folder_ids_traversed:
                logger.info("Fetching all files in subfolder: " + subfolder["name"])
                folder_ids_traversed.append(subfolder["id"])
                yield from get_all_files_batched(
                    service=service,
                    include_shared=include_shared,
                    follow_shortcuts=follow_shortcuts,
                    batch_size=batch_size,
                    time_range_start=time_range_start,
                    time_range_end=time_range_end,
                    folder_id=subfolder["id"],
                    traverse_subfolders=traverse_subfolders,
                    folder_ids_traversed=folder_ids_traversed,
                )
            else:
                logger.debug(
                    "Skipping subfolder since already traversed: " + subfolder["name"]
                )


def extract_text(file: dict[str, str], service: discovery.Resource) -> str:
    mime_type = file["mimeType"]
    if mime_type == "application/vnd.google-apps.document":
        return (
            service.files()
            .export(fileId=file["id"], mimeType="text/plain")
            .execute()
            .decode("utf-8")
        )
    elif mime_type == "application/vnd.google-apps.spreadsheet":
        return (
            service.files()
            .export(fileId=file["id"], mimeType="text/csv")
            .execute()
            .decode("utf-8")
        )
    elif (
        mime_type
        == "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ):
        response = service.files().get_media(fileId=file["id"]).execute()
        word_stream = io.BytesIO(response)
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            temp.write(word_stream.getvalue())
            temp_path = temp.name
        return docx2txt.process(temp_path)
    # Default download to PDF since most types can be exported as a PDF
    else:
        response = service.files().get_media(fileId=file["id"]).execute()
        pdf_stream = io.BytesIO(response)
        pdf_reader = PdfReader(pdf_stream)
        return "\n".join(page.extract_text() for page in pdf_reader.pages)


class GoogleDriveConnector(LoadConnector, PollConnector):
    def __init__(
        self,
        # optional list of folder paths e.g. "[My Folder/My Subfolder]"
        # if specified, will only index files in these folders
        folder_paths: list[str] | None = None,
        batch_size: int = INDEX_BATCH_SIZE,
        include_shared: bool = GOOGLE_DRIVE_INCLUDE_SHARED,
        follow_shortcuts: bool = GOOGLE_DRIVE_FOLLOW_SHORTCUTS,
    ) -> None:
        self.folder_paths = folder_paths or []
        self.batch_size = batch_size
        self.include_shared = include_shared
        self.follow_shortcuts = follow_shortcuts
        self.creds: Credentials | None = None

    @staticmethod
    def _process_folder_paths(
        service: discovery.Resource,
        folder_paths: list[str],
        include_shared: bool,
        follow_shortcuts: bool,
    ) -> list[str]:
        """['Folder/Sub Folder'] -> ['<FOLDER_ID>']"""
        folder_ids: list[str] = []
        for path in folder_paths:
            folder_names = path.split("/")
            parent_id = "root"
            for folder_name in folder_names:
                found_parent_id = _get_folder_id(
                    service=service,
                    parent_id=parent_id,
                    folder_name=folder_name,
                    include_shared=include_shared,
                    follow_shortcuts=follow_shortcuts,
                )
                if found_parent_id is None:
                    raise ValueError(
                        (
                            f"Folder '{folder_name}' in path '{path}' "
                            "not found in Google Drive"
                        )
                    )
                parent_id = found_parent_id
            folder_ids.append(parent_id)

        return folder_ids

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        access_token_json_str = credentials[DB_CREDENTIALS_DICT_KEY]
        creds = get_drive_tokens(token_json_str=access_token_json_str)
        if creds is None:
            raise PermissionError("Unable to access Google Drive.")
        self.creds = creds
        new_creds_json_str = creds.to_json()
        if new_creds_json_str != access_token_json_str:
            return {DB_CREDENTIALS_DICT_KEY: new_creds_json_str}
        return None

    def _fetch_docs_from_drive(
        self,
        start: SecondsSinceUnixEpoch | None = None,
        end: SecondsSinceUnixEpoch | None = None,
    ) -> GenerateDocumentsOutput:
        if self.creds is None:
            raise PermissionError("Not logged into Google Drive")

        service = discovery.build("drive", "v3", credentials=self.creds)
        folder_ids: Sequence[str | None] = self._process_folder_paths(
            service, self.folder_paths, self.include_shared, self.follow_shortcuts
        )
        if not folder_ids:
            folder_ids = [None]

        file_batches = chain(
            *[
                get_all_files_batched(
                    service=service,
                    include_shared=self.include_shared,
                    follow_shortcuts=self.follow_shortcuts,
                    batch_size=self.batch_size,
                    time_range_start=start,
                    time_range_end=end,
                    folder_id=folder_id,
                    traverse_subfolders=True,
                )
                for folder_id in folder_ids
            ]
        )
        for files_batch in file_batches:
            doc_batch = []
            for file in files_batch:
                text_contents = extract_text(file, service)
                full_context = file["name"] + " - " + text_contents

                doc_batch.append(
                    Document(
                        id=file["webViewLink"],
                        sections=[Section(link=file["webViewLink"], text=full_context)],
                        source=DocumentSource.GOOGLE_DRIVE,
                        semantic_identifier=file["name"],
                        metadata={},
                    )
                )

            yield doc_batch

    def load_from_state(self) -> GenerateDocumentsOutput:
        yield from self._fetch_docs_from_drive()

    def poll_source(
        self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch
    ) -> GenerateDocumentsOutput:
        # need to subtract 10 minutes from start time to account for modifiedTime
        # propogation if a document is modified, it takes some time for the API to
        # reflect these changes if we do not have an offset, then we may "miss" the
        # update when polling
        yield from self._fetch_docs_from_drive(
            max(start - DRIVE_START_TIME_OFFSET, 0, 0), end
        )
