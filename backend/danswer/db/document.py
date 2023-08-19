from collections.abc import Sequence

from sqlalchemy import and_
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from danswer.datastores.interfaces import DocumentMetadata
from danswer.db.models import Document
from danswer.db.models import DocumentByConnectorCredentialPair
from danswer.db.utils import model_to_dict
from danswer.utils.logger import setup_logger

logger = setup_logger()


def get_documents_with_single_connector_credential_pair(
    db_session: Session,
    connector_id: int,
    credential_id: int,
) -> Sequence[Document]:
    initial_doc_ids_stmt = select(DocumentByConnectorCredentialPair.id).where(
        and_(
            DocumentByConnectorCredentialPair.connector_id == connector_id,
            DocumentByConnectorCredentialPair.credential_id == credential_id,
        )
    )

    # Filter it down to the documents with only a single connector/credential pair
    # Meaning if this connector/credential pair is removed, this doc should be gone
    trimmed_doc_ids_stmt = (
        select(Document.id)
        .join(
            DocumentByConnectorCredentialPair,
            DocumentByConnectorCredentialPair.id == Document.id,
        )
        .where(Document.id.in_(initial_doc_ids_stmt))
        .group_by(Document.id)
        .having(func.count(DocumentByConnectorCredentialPair.id) == 1)
    )

    stmt = select(Document).where(Document.id.in_(trimmed_doc_ids_stmt))
    return db_session.scalars(stmt).all()


def get_document_by_connector_credential_pairs_indexed_by_multiple(
    db_session: Session,
    connector_id: int,
    credential_id: int,
) -> Sequence[DocumentByConnectorCredentialPair]:
    initial_doc_ids_stmt = select(DocumentByConnectorCredentialPair.id).where(
        and_(
            DocumentByConnectorCredentialPair.connector_id == connector_id,
            DocumentByConnectorCredentialPair.credential_id == credential_id,
        )
    )

    # Filter it down to the documents with more than 1 connector/credential pair
    # Meaning if this connector/credential pair is removed, this doc is still accessible
    trimmed_doc_ids_stmt = (
        select(Document.id)
        .join(
            DocumentByConnectorCredentialPair,
            DocumentByConnectorCredentialPair.id == Document.id,
        )
        .where(Document.id.in_(initial_doc_ids_stmt))
        .group_by(Document.id)
        .having(func.count(DocumentByConnectorCredentialPair.id) > 1)
    )

    stmt = select(DocumentByConnectorCredentialPair).where(
        DocumentByConnectorCredentialPair.id.in_(trimmed_doc_ids_stmt)
    )

    return db_session.execute(stmt).scalars().all()


def upsert_documents(
    db_session: Session, document_metadata_batch: list[DocumentMetadata]
) -> None:
    """NOTE: this function is Postgres specific. Not all DBs support the ON CONFLICT clause."""
    seen_document_ids: set[str] = set()
    for document_metadata in document_metadata_batch:
        if document_metadata.document_id not in seen_document_ids:
            seen_document_ids.add(document_metadata.document_id)

    insert_stmt = insert(Document).values(
        [model_to_dict(Document(id=doc_id)) for doc_id in seen_document_ids]
    )
    # for now, there are no columns to update. If more metadata is added, then this
    # needs to change to an `on_conflict_do_update`
    on_conflict_stmt = insert_stmt.on_conflict_do_nothing()
    db_session.execute(on_conflict_stmt)
    db_session.commit()


def upsert_document_by_connector_credential_pair(
    db_session: Session, document_metadata_batch: list[DocumentMetadata]
) -> None:
    """NOTE: this function is Postgres specific. Not all DBs support the ON CONFLICT clause."""
    insert_stmt = insert(DocumentByConnectorCredentialPair).values(
        [
            model_to_dict(
                DocumentByConnectorCredentialPair(
                    id=document_metadata.document_id,
                    connector_id=document_metadata.connector_id,
                    credential_id=document_metadata.credential_id,
                )
            )
            for document_metadata in document_metadata_batch
        ]
    )
    # for now, there are no columns to update. If more metadata is added, then this
    # needs to change to an `on_conflict_do_update`
    on_conflict_stmt = insert_stmt.on_conflict_do_nothing()
    db_session.execute(on_conflict_stmt)
    db_session.commit()


def upsert_documents_complete(
    db_session: Session, document_metadata_batch: list[DocumentMetadata]
) -> None:
    upsert_documents(db_session, document_metadata_batch)
    upsert_document_by_connector_credential_pair(db_session, document_metadata_batch)
    logger.info(
        f"Upserted {len(document_metadata_batch)} document store entries into DB"
    )


def delete_document_by_connector_credential_pair(
    db_session: Session, document_ids: list[str]
) -> None:
    db_session.execute(
        delete(DocumentByConnectorCredentialPair).where(
            DocumentByConnectorCredentialPair.id.in_(document_ids)
        )
    )


def delete_documents(db_session: Session, document_ids: list[str]) -> None:
    db_session.execute(delete(Document).where(Document.id.in_(document_ids)))


def delete_documents_complete(db_session: Session, document_ids: list[str]) -> None:
    logger.info(f"Deleting {len(document_ids)} documents from the DB")
    delete_document_by_connector_credential_pair(db_session, document_ids)
    delete_documents(db_session, document_ids)
    db_session.commit()
