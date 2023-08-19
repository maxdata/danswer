import json
from uuid import UUID

from nltk.corpus import stopwords  # type:ignore
from nltk.stem import WordNetLemmatizer  # type:ignore
from nltk.tokenize import word_tokenize  # type:ignore

from danswer.chunking.models import InferenceChunk
from danswer.configs.app_configs import NUM_RETURNED_HITS
from danswer.datastores.interfaces import IndexFilter
from danswer.datastores.interfaces import KeywordIndex
from danswer.utils.logger import setup_logger
from danswer.utils.timing import log_function_time

logger = setup_logger()


def lemmatize_text(text: str) -> list[str]:
    lemmatizer = WordNetLemmatizer()
    word_tokens = word_tokenize(text)
    return [lemmatizer.lemmatize(word) for word in word_tokens]


def remove_stop_words(text: str) -> list[str]:
    stop_words = set(stopwords.words("english"))
    word_tokens = word_tokenize(text)
    return [word for word in word_tokens if word.casefold() not in stop_words]


def query_processing(query: str) -> str:
    query = " ".join(remove_stop_words(query))
    query = " ".join(lemmatize_text(query))
    return query


@log_function_time()
def retrieve_keyword_documents(
    query: str,
    user_id: UUID | None,
    filters: list[IndexFilter] | None,
    datastore: KeywordIndex,
    num_hits: int = NUM_RETURNED_HITS,
) -> list[InferenceChunk] | None:
    edited_query = query_processing(query)
    top_chunks = datastore.keyword_retrieval(edited_query, user_id, filters, num_hits)
    if not top_chunks:
        filters_log_msg = json.dumps(filters, separators=(",", ":")).replace("\n", "")
        logger.warning(
            f"Keyword search returned no results - Filters: {filters_log_msg}\tEdited Query: {edited_query}"
        )
        return None
    return top_chunks
