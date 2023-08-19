import abc
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from danswer.chunking.models import IndexChunk
from danswer.chunking.models import InferenceChunk
from danswer.configs.model_configs import SEARCH_DISTANCE_CUTOFF
from danswer.connectors.models import IndexAttemptMetadata

IndexFilter = dict[str, str | list[str] | None]


@dataclass(frozen=True)
class DocumentInsertionRecord:
    document_id: str
    already_existed: bool


@dataclass
class DocumentMetadata:
    connector_id: int
    credential_id: int
    document_id: str


@dataclass
class UpdateRequest:
    """For all document_ids, update the allowed_users and the boost to the new value
    ignore if None"""

    document_ids: list[str]
    # all other fields will be left alone
    allowed_users: list[str] | None = None
    boost: int | None = None


class Verifiable(abc.ABC):
    @abc.abstractmethod
    def __init__(self, index_name: str, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.index_name = index_name

    @abc.abstractmethod
    def ensure_indices_exist(self) -> None:
        raise NotImplementedError


class Indexable(abc.ABC):
    @abc.abstractmethod
    def index(
        self, chunks: list[IndexChunk], index_attempt_metadata: IndexAttemptMetadata
    ) -> set[DocumentInsertionRecord]:
        """Indexes document chunks into the Document Index and return the IDs of all the documents indexed"""
        raise NotImplementedError


class Deletable(abc.ABC):
    @abc.abstractmethod
    def delete(self, ids: list[str]) -> None:
        """Removes the specified documents from the Index"""
        raise NotImplementedError


class Updatable(abc.ABC):
    @abc.abstractmethod
    def update(self, update_requests: list[UpdateRequest]) -> None:
        """Updates metadata for the specified documents sets in the Index"""
        raise NotImplementedError


class KeywordCapable(abc.ABC):
    @abc.abstractmethod
    def keyword_retrieval(
        self,
        query: str,
        user_id: UUID | None,
        filters: list[IndexFilter] | None,
        num_to_retrieve: int,
    ) -> list[InferenceChunk]:
        raise NotImplementedError


class VectorCapable(abc.ABC):
    @abc.abstractmethod
    def semantic_retrieval(
        self,
        query: str,
        user_id: UUID | None,
        filters: list[IndexFilter] | None,
        num_to_retrieve: int,
        distance_cutoff: float | None = SEARCH_DISTANCE_CUTOFF,
    ) -> list[InferenceChunk]:
        raise NotImplementedError


class HybridCapable(abc.ABC):
    @abc.abstractmethod
    def semantic_retrieval(
        self,
        query: str,
        user_id: UUID | None,
        filters: list[IndexFilter] | None,
        num_to_retrieve: int,
    ) -> list[InferenceChunk]:
        raise NotImplementedError


class BaseIndex(Verifiable, Indexable, Updatable, Deletable, abc.ABC):
    """All basic functionalities excluding a specific retrieval approach
    Indices need to be able to
    - Check that the index exists with a schema definition
    - Can index documents
    - Can delete documents
    - Can update document metadata (such as access permissions and document specific boost)
    """


class KeywordIndex(KeywordCapable, BaseIndex, abc.ABC):
    pass


class VectorIndex(VectorCapable, BaseIndex, abc.ABC):
    pass


class DocumentIndex(KeywordCapable, VectorCapable, HybridCapable, BaseIndex, abc.ABC):
    pass
