from typing import cast

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response
from fastapi import UploadFile
from sqlalchemy.orm import Session

from backend.auth.users import current_admin_user
from backend.auth.users import current_user
from backend.background.celery.celery_utils import get_deletion_status
from backend.configs.constants import DocumentSource
from backend.connectors.file.utils import write_temp_files
from backend.db.connector import create_connector
from backend.db.connector import delete_connector
from backend.db.connector import fetch_connector_by_id
from backend.db.connector import fetch_connectors
from backend.db.connector import get_connector_credential_ids
from backend.db.connector import update_connector
from backend.db.connector_credential_pair import get_connector_credential_pairs
from backend.db.credentials import create_credential
from backend.db.credentials import fetch_credential_by_id
from backend.db.deletion_attempt import check_deletion_attempt_is_allowed
from backend.db.document import get_document_cnts_for_cc_pairs
from backend.db.embedding_model import get_current_db_embedding_model
from backend.db.embedding_model import get_secondary_db_embedding_model
from backend.db.engine import get_session
from backend.db.index_attempt import cancel_indexing_attempts_for_connector
from backend.db.index_attempt import create_index_attempt
from backend.db.index_attempt import get_index_attempts_for_cc_pair
from backend.db.index_attempt import get_latest_index_attempts
from backend.db.models import User
from backend.dynamic_configs.interface import ConfigNotFoundError
from backend.server.documents.models import AuthStatus
from backend.server.documents.models import AuthUrl
from backend.server.documents.models import ConnectorBase
from backend.server.documents.models import ConnectorCredentialPairIdentifier
from backend.server.documents.models import ConnectorIndexingStatus
from backend.server.documents.models import ConnectorSnapshot
from backend.server.documents.models import CredentialSnapshot
from backend.server.documents.models import FileUploadResponse
from backend.server.documents.models import IndexAttemptSnapshot
from backend.server.documents.models import ObjectCreationIdResponse
from backend.server.documents.models import RunConnectorRequest
from backend.server.models import StatusResponse

_GMAIL_CREDENTIAL_ID_COOKIE_NAME = "gmail_credential_id"
_GOOGLE_DRIVE_CREDENTIAL_ID_COOKIE_NAME = "google_drive_credential_id"


router = APIRouter(prefix="/manage")


"""Admin only API endpoints"""

@router.post("/admin/connector/file/upload")
def upload_files(
    files: list[UploadFile], _: User = Depends(current_admin_user)
) -> FileUploadResponse:
    for file in files:
        if not file.filename:
            raise HTTPException(status_code=400, detail="File name cannot be empty")
    try:
        file_paths = write_temp_files(
            [(cast(str, file.filename), file.file) for file in files]
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return FileUploadResponse(file_paths=file_paths)


@router.get("/admin/connector/indexing-status")
def get_connector_indexing_status(
    secondary_index: bool = False,
    _: User = Depends(current_admin_user),
    db_session: Session = Depends(get_session),
) -> list[ConnectorIndexingStatus]:
    indexing_statuses: list[ConnectorIndexingStatus] = []

    # TODO: make this one query
    cc_pairs = get_connector_credential_pairs(db_session)
    cc_pair_identifiers = [
        ConnectorCredentialPairIdentifier(
            connector_id=cc_pair.connector_id, credential_id=cc_pair.credential_id
        )
        for cc_pair in cc_pairs
    ]

    latest_index_attempts = get_latest_index_attempts(
        connector_credential_pair_identifiers=cc_pair_identifiers,
        secondary_index=secondary_index,
        db_session=db_session,
    )
    cc_pair_to_latest_index_attempt = {
        (index_attempt.connector_id, index_attempt.credential_id): index_attempt
        for index_attempt in latest_index_attempts
    }

    document_count_info = get_document_cnts_for_cc_pairs(
        db_session=db_session,
        cc_pair_identifiers=cc_pair_identifiers,
    )
    cc_pair_to_document_cnt = {
        (connector_id, credential_id): cnt
        for connector_id, credential_id, cnt in document_count_info
    }

    for cc_pair in cc_pairs:
        # TODO remove this to enable ingestion API
        if cc_pair.name == "DefaultCCPair":
            continue

        connector = cc_pair.connector
        credential = cc_pair.credential
        latest_index_attempt = cc_pair_to_latest_index_attempt.get(
            (connector.id, credential.id)
        )
        indexing_statuses.append(
            ConnectorIndexingStatus(
                cc_pair_id=cc_pair.id,
                name=cc_pair.name,
                connector=ConnectorSnapshot.from_connector_db_model(connector),
                credential=CredentialSnapshot.from_credential_db_model(credential),
                public_doc=cc_pair.is_public,
                owner=credential.user.email if credential.user else "",
                last_status=cc_pair.last_attempt_status,
                last_success=cc_pair.last_successful_index_time,
                docs_indexed=cc_pair_to_document_cnt.get(
                    (connector.id, credential.id), 0
                ),
                error_msg=latest_index_attempt.error_msg
                if latest_index_attempt
                else None,
                latest_index_attempt=IndexAttemptSnapshot.from_index_attempt_db_model(
                    latest_index_attempt
                )
                if latest_index_attempt
                else None,
                deletion_attempt=get_deletion_status(
                    connector_id=connector.id,
                    credential_id=credential.id,
                    db_session=db_session,
                ),
                is_deletable=check_deletion_attempt_is_allowed(
                    connector_credential_pair=cc_pair
                ),
            )
        )

    return indexing_statuses


@router.post("/admin/connector")
def create_connector_from_model(
    connector_info: ConnectorBase,
    _: User = Depends(current_admin_user),
    db_session: Session = Depends(get_session),
) -> ObjectCreationIdResponse:
    try:
        return create_connector(connector_info, db_session)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.patch("/admin/connector/{connector_id}")
def update_connector_from_model(
    connector_id: int,
    connector_data: ConnectorBase,
    _: User = Depends(current_admin_user),
    db_session: Session = Depends(get_session),
) -> ConnectorSnapshot | StatusResponse[int]:
    updated_connector = update_connector(connector_id, connector_data, db_session)
    if updated_connector is None:
        raise HTTPException(
            status_code=404, detail=f"Connector {connector_id} does not exist"
        )

    if updated_connector.disabled:
        cancel_indexing_attempts_for_connector(connector_id, db_session)

    return ConnectorSnapshot(
        id=updated_connector.id,
        name=updated_connector.name,
        source=updated_connector.source,
        input_type=updated_connector.input_type,
        connector_specific_config=updated_connector.connector_specific_config,
        refresh_freq=updated_connector.refresh_freq,
        credential_ids=[
            association.credential.id for association in updated_connector.credentials
        ],
        time_created=updated_connector.time_created,
        time_updated=updated_connector.time_updated,
        disabled=updated_connector.disabled,
    )


@router.delete("/admin/connector/{connector_id}", response_model=StatusResponse[int])
def delete_connector_by_id(
    connector_id: int,
    _: User = Depends(current_admin_user),
    db_session: Session = Depends(get_session),
) -> StatusResponse[int]:
    try:
        with db_session.begin():
            return delete_connector(db_session=db_session, connector_id=connector_id)
    except AssertionError:
        raise HTTPException(status_code=400, detail="Connector is not deletable")


@router.post("/admin/connector/run-once")
def connector_run_once(
    run_info: RunConnectorRequest,
    _: User = Depends(current_admin_user),
    db_session: Session = Depends(get_session),
) -> StatusResponse[list[int]]:
    connector_id = run_info.connector_id
    specified_credential_ids = run_info.credential_ids
    try:
        possible_credential_ids = get_connector_credential_ids(
            run_info.connector_id, db_session
        )
    except ValueError:
        raise HTTPException(
            status_code=404,
            detail=f"Connector by id {connector_id} does not exist.",
        )

    if not specified_credential_ids:
        credential_ids = possible_credential_ids
    else:
        if set(specified_credential_ids).issubset(set(possible_credential_ids)):
            credential_ids = specified_credential_ids
        else:
            raise HTTPException(
                status_code=400,
                detail="Not all specified credentials are associated with connector",
            )

    if not credential_ids:
        raise HTTPException(
            status_code=400,
            detail="Connector has no valid credentials, cannot create index attempts.",
        )

    skipped_credentials = [
        credential_id
        for credential_id in credential_ids
        if get_index_attempts_for_cc_pair(
            cc_pair_identifier=ConnectorCredentialPairIdentifier(
                connector_id=run_info.connector_id,
                credential_id=credential_id,
            ),
            disinclude_finished=True,
            db_session=db_session,
        )
    ]

    embedding_model = get_current_db_embedding_model(db_session)

    secondary_embedding_model = get_secondary_db_embedding_model(db_session)

    index_attempt_ids = [
        create_index_attempt(
            run_info.connector_id, credential_id, embedding_model.id, db_session
        )
        for credential_id in credential_ids
        if credential_id not in skipped_credentials
    ]

    if not index_attempt_ids:
        raise HTTPException(
            status_code=400,
            detail="No new indexing attempts created, indexing jobs are queued or running.",
        )

    return StatusResponse(
        success=True,
        message=f"Successfully created {len(index_attempt_ids)} index attempts",
        data=index_attempt_ids,
    )


"""Endpoints for basic users"""

@router.get("/connector")
def get_connectors(
    _: User = Depends(current_user),
    db_session: Session = Depends(get_session),
) -> list[ConnectorSnapshot]:
    connectors = fetch_connectors(db_session)
    return [
        ConnectorSnapshot.from_connector_db_model(connector)
        for connector in connectors
        # don't include INGESTION_API, as it's not a "real"
        # connector like those created by the user
        if connector.source != DocumentSource.INGESTION_API
    ]


@router.get("/connector/{connector_id}")
def get_connector_by_id(
    connector_id: int,
    _: User = Depends(current_user),
    db_session: Session = Depends(get_session),
) -> ConnectorSnapshot | StatusResponse[int]:
    connector = fetch_connector_by_id(connector_id, db_session)
    if connector is None:
        raise HTTPException(
            status_code=404, detail=f"Connector {connector_id} does not exist"
        )

    return ConnectorSnapshot(
        id=connector.id,
        name=connector.name,
        source=connector.source,
        input_type=connector.input_type,
        connector_specific_config=connector.connector_specific_config,
        refresh_freq=connector.refresh_freq,
        credential_ids=[
            association.credential.id for association in connector.credentials
        ],
        time_created=connector.time_created,
        time_updated=connector.time_updated,
        disabled=connector.disabled,
    )
