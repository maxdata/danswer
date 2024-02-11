from threading import Thread

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.access.models import DocumentAccess
from backend.db.document import get_acccess_info_for_documents
from backend.db.engine import get_sqlalchemy_engine
from backend.db.models import Document
from backend.document_index.document_index_utils import get_both_index_names
from backend.document_index.factory import get_default_document_index
from backend.document_index.interfaces import UpdateRequest
from backend.document_index.vespa.index import VespaIndex
from backend.dynamic_configs import get_dynamic_config_store
from backend.dynamic_configs.interface import ConfigNotFoundError
from backend.utils.logger import setup_logger

logger = setup_logger()


_COMPLETED_ACL_UPDATE_KEY = "completed_acl_update"


def set_acl_for_vespa(should_check_if_already_done: bool = False) -> None:
    """Updates the ACL for all documents based on the state of Postgres."""
    dynamic_config_store = get_dynamic_config_store()
    if should_check_if_already_done:
        try:
            # if entry is found, then we've already done this
            dynamic_config_store.load(_COMPLETED_ACL_UPDATE_KEY)
            return
        except ConfigNotFoundError:
            pass

    logger.info("Populating Access Control List fields in Vespa")
    with Session(get_sqlalchemy_engine()) as db_session:
        # for all documents, set the `access_control_list` field appropriately
        # based on the state of Postgres
        documents = db_session.scalars(select(Document)).all()
        document_access_info = get_acccess_info_for_documents(
            db_session=db_session,
            document_ids=[document.id for document in documents],
        )

        curr_ind_name, sec_ind_name = get_both_index_names(db_session)
        vespa_index = get_default_document_index(
            primary_index_name=curr_ind_name, secondary_index_name=sec_ind_name
        )
        if not isinstance(vespa_index, VespaIndex):
            raise ValueError("This script is only for Vespa indexes")

        update_requests = [
            UpdateRequest(
                document_ids=[document_id],
                access=DocumentAccess.build(user_ids, is_public),
            )
            for document_id, user_ids, is_public in document_access_info
        ]
        vespa_index.update(update_requests=update_requests)

    dynamic_config_store.store(_COMPLETED_ACL_UPDATE_KEY, True)


def set_acl_for_vespa_nonblocking(should_check_if_already_done: bool = False) -> None:
    """Kick off the ACL update in a separate thread so that other work can continue."""
    Thread(
        target=set_acl_for_vespa,
        args=[should_check_if_already_done],
    ).start()
