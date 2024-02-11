from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Query
from sqlalchemy.orm import Session

from backend.auth.users import current_user
from backend.db.embedding_model import get_current_db_embedding_model
from backend.db.engine import get_session
from backend.db.models import User
from backend.document_index.factory import get_default_document_index
from backend.llm.utils import get_default_llm_token_encode
from backend.search.access_filters import build_access_filters_for_user
from backend.search.models import IndexFilters
from backend.server.documents.models import ChunkInfo
from backend.server.documents.models import DocumentInfo


router = APIRouter(prefix="/document")


# Have to use a query parameter as FastAPI is interpreting the URL type document_ids
# as a different path
@router.get("/document-size-info")
def get_document_info(
    document_id: str = Query(...),
    user: User | None = Depends(current_user),
    db_session: Session = Depends(get_session),
) -> DocumentInfo:
    embedding_model = get_current_db_embedding_model(db_session)

    document_index = get_default_document_index(
        primary_index_name=embedding_model.index_name, secondary_index_name=None
    )

    user_acl_filters = build_access_filters_for_user(user, db_session)
    filters = IndexFilters(access_control_list=user_acl_filters)

    inference_chunks = document_index.id_based_retrieval(
        document_id=document_id,
        chunk_ind=None,
        filters=filters,
    )

    if not inference_chunks:
        raise HTTPException(status_code=404, detail="Document not found")

    contents = [chunk.content for chunk in inference_chunks]

    combined = "\n".join(contents)

    tokenizer_encode = get_default_llm_token_encode()

    return DocumentInfo(
        num_chunks=len(inference_chunks), num_tokens=len(tokenizer_encode(combined))
    )


@router.get("/chunk-info")
def get_chunk_info(
    document_id: str = Query(...),
    chunk_id: int = Query(...),
    user: User | None = Depends(current_user),
    db_session: Session = Depends(get_session),
) -> ChunkInfo:
    embedding_model = get_current_db_embedding_model(db_session)

    document_index = get_default_document_index(
        primary_index_name=embedding_model.index_name, secondary_index_name=None
    )

    user_acl_filters = build_access_filters_for_user(user, db_session)
    filters = IndexFilters(access_control_list=user_acl_filters)

    inference_chunks = document_index.id_based_retrieval(
        document_id=document_id,
        chunk_ind=chunk_id,
        filters=filters,
    )

    if not inference_chunks:
        raise HTTPException(status_code=404, detail="Chunk not found")

    chunk_content = inference_chunks[0].content

    tokenizer_encode = get_default_llm_token_encode()

    return ChunkInfo(
        content=chunk_content, num_tokens=len(tokenizer_encode(chunk_content))
    )