from datetime import datetime

import pytz
import timeago  # type: ignore
from slack_sdk.models.blocks import ActionsBlock
from slack_sdk.models.blocks import Block
from slack_sdk.models.blocks import ButtonElement
from slack_sdk.models.blocks import DividerBlock
from slack_sdk.models.blocks import HeaderBlock
from slack_sdk.models.blocks import Option
from slack_sdk.models.blocks import RadioButtonsElement
from slack_sdk.models.blocks import SectionBlock

from backend.chat.models import DanswerQuote
from backend.configs.app_configs import DISABLE_GENERATIVE_AI
from backend.configs.constants import DocumentSource
from backend.configs.constants import SearchFeedbackType
from backend.configs.danswerbot_configs import DANSWER_BOT_NUM_DOCS_TO_DISPLAY
from backend.danswerbot.slack.constants import DISLIKE_BLOCK_ACTION_ID
from backend.danswerbot.slack.constants import FEEDBACK_DOC_BUTTON_BLOCK_ACTION_ID
from backend.danswerbot.slack.constants import FOLLOWUP_BUTTON_ACTION_ID
from backend.danswerbot.slack.constants import FOLLOWUP_BUTTON_RESOLVED_ACTION_ID
from backend.danswerbot.slack.constants import LIKE_BLOCK_ACTION_ID
from backend.danswerbot.slack.utils import build_feedback_id
from backend.danswerbot.slack.utils import remove_slack_text_interactions
from backend.danswerbot.slack.utils import translate_vespa_highlight_to_slack
from backend.search.models import SavedSearchDoc
from backend.utils.text_processing import decode_escapes
from backend.utils.text_processing import replace_whitespaces_w_space

_MAX_BLURB_LEN = 75


def build_qa_feedback_block(message_id: int) -> Block:
    return ActionsBlock(
        block_id=build_feedback_id(message_id),
        elements=[
            ButtonElement(
                action_id=LIKE_BLOCK_ACTION_ID,
                text="👍",
                style="primary",
            ),
            ButtonElement(
                action_id=DISLIKE_BLOCK_ACTION_ID,
                text="👎",
                style="danger",
            ),
        ],
    )


def get_document_feedback_blocks() -> Block:
    return SectionBlock(
        text=(
            "- 'Up-Boost' if this document is a good source of information and should be "
            "shown more often.\n"
            "- 'Down-boost' if this document is a poor source of information and should be "
            "shown less often.\n"
            "- 'Hide' if this document is deprecated and should never be shown anymore."
        ),
        accessory=RadioButtonsElement(
            options=[
                Option(
                    text=":thumbsup: Up-Boost",
                    value=SearchFeedbackType.ENDORSE.value,
                ),
                Option(
                    text=":thumbsdown: Down-Boost",
                    value=SearchFeedbackType.REJECT.value,
                ),
                Option(
                    text=":x: Hide",
                    value=SearchFeedbackType.HIDE.value,
                ),
            ]
        ),
    )


def build_doc_feedback_block(
    message_id: int,
    document_id: str,
    document_rank: int,
) -> ButtonElement:
    feedback_id = build_feedback_id(message_id, document_id, document_rank)
    return ButtonElement(
        action_id=FEEDBACK_DOC_BUTTON_BLOCK_ACTION_ID,
        value=feedback_id,
        text="Give Feedback",
    )


def get_restate_blocks(
    msg: str,
    is_bot_msg: bool,
) -> list[Block]:
    # Only the slash command needs this context because the user doesn't see their own input
    if not is_bot_msg:
        return []

    return [
        HeaderBlock(text="Responding to the Query"),
        SectionBlock(text=f"```{msg}```"),
    ]


def build_documents_blocks(
    documents: list[SavedSearchDoc],
    message_id: int | None,
    num_docs_to_display: int = DANSWER_BOT_NUM_DOCS_TO_DISPLAY,
) -> list[Block]:
    header_text = (
        "Retrieved Documents" if DISABLE_GENERATIVE_AI else "Reference Documents"
    )
    seen_docs_identifiers = set()
    section_blocks: list[Block] = [HeaderBlock(text=header_text)]
    included_docs = 0
    for rank, d in enumerate(documents):
        if d.document_id in seen_docs_identifiers:
            continue
        seen_docs_identifiers.add(d.document_id)

        doc_sem_id = d.semantic_identifier
        if d.source_type == DocumentSource.SLACK.value:
            doc_sem_id = "#" + doc_sem_id

        used_chars = len(doc_sem_id) + 3
        match_str = translate_vespa_highlight_to_slack(d.match_highlights, used_chars)

        included_docs += 1

        header_line = f"{doc_sem_id}\n"
        if d.link:
            header_line = f"<{d.link}|{doc_sem_id}>\n"

        updated_at_line = ""
        if d.updated_at is not None:
            updated_at_line = (
                f"_Updated {timeago.format(d.updated_at, datetime.now(pytz.utc))}_\n"
            )

        body_text = f">{remove_slack_text_interactions(match_str)}"

        block_text = header_line + updated_at_line + body_text

        feedback: ButtonElement | dict = {}
        if message_id is not None:
            feedback = build_doc_feedback_block(
                message_id=message_id,
                document_id=d.document_id,
                document_rank=rank,
            )

        section_blocks.append(
            SectionBlock(text=block_text, accessory=feedback),
        )

        section_blocks.append(DividerBlock())

        if included_docs >= num_docs_to_display:
            break

    return section_blocks


def build_quotes_block(
    quotes: list[DanswerQuote],
) -> list[Block]:
    quote_lines: list[str] = []
    doc_to_quotes: dict[str, list[str]] = {}
    doc_to_link: dict[str, str] = {}
    doc_to_sem_id: dict[str, str] = {}
    for q in quotes:
        quote = q.quote
        doc_id = q.document_id
        doc_link = q.link
        doc_name = q.semantic_identifier
        if doc_link and doc_name and doc_id and quote:
            if doc_id not in doc_to_quotes:
                doc_to_quotes[doc_id] = [quote]
                doc_to_link[doc_id] = doc_link
                doc_to_sem_id[doc_id] = (
                    doc_name
                    if q.source_type != DocumentSource.SLACK.value
                    else "#" + doc_name
                )
            else:
                doc_to_quotes[doc_id].append(quote)

    for doc_id, quote_strs in doc_to_quotes.items():
        quotes_str_clean = [
            replace_whitespaces_w_space(q_str).strip() for q_str in quote_strs
        ]
        longest_quotes = sorted(quotes_str_clean, key=len, reverse=True)[:5]
        single_quote_str = "\n".join([f"```{q_str}```" for q_str in longest_quotes])
        link = doc_to_link[doc_id]
        sem_id = doc_to_sem_id[doc_id]
        quote_lines.append(
            f"<{link}|{sem_id}>:\n{remove_slack_text_interactions(single_quote_str)}"
        )

    if not doc_to_quotes:
        return []

    return [SectionBlock(text="*Relevant Snippets*\n" + "\n".join(quote_lines))]


def build_qa_response_blocks(
    message_id: int | None,
    answer: str | None,
    quotes: list[DanswerQuote] | None,
    source_filters: list[DocumentSource] | None,
    time_cutoff: datetime | None,
    favor_recent: bool,
    skip_quotes: bool = False,
    skip_ai_feedback: bool = False,
) -> list[Block]:
    if DISABLE_GENERATIVE_AI:
        return []

    quotes_blocks: list[Block] = []

    ai_answer_header = HeaderBlock(text="AI Answer")

    filter_block: Block | None = None
    if time_cutoff or favor_recent or source_filters:
        filter_text = "Filters: "
        if source_filters:
            sources_str = ", ".join([s.value for s in source_filters])
            filter_text += f"`Sources in [{sources_str}]`"
            if time_cutoff or favor_recent:
                filter_text += " and "
        if time_cutoff is not None:
            time_str = time_cutoff.strftime("%b %d, %Y")
            filter_text += f"`Docs Updated >= {time_str}` "
        if favor_recent:
            if time_cutoff is not None:
                filter_text += "+ "
            filter_text += "`Prioritize Recently Updated Docs`"

        filter_block = SectionBlock(text=f"_{filter_text}_")

    if not answer:
        answer_block = SectionBlock(
            text="Sorry, I was unable to find an answer, but I did find some potentially relevant docs 🤓"
        )
    else:
        answer_processed = decode_escapes(remove_slack_text_interactions(answer))
        answer_block = SectionBlock(text=answer_processed)
        if quotes:
            quotes_blocks = build_quotes_block(quotes)

        # if no quotes OR `build_quotes_block()` did not give back any blocks
        if not quotes_blocks:
            quotes_blocks = [
                SectionBlock(
                    text="*Warning*: no sources were quoted for this answer, so it may be unreliable 😔"
                )
            ]

    response_blocks: list[Block] = [ai_answer_header]

    if filter_block is not None:
        response_blocks.append(filter_block)

    response_blocks.append(answer_block)

    if message_id is not None and not skip_ai_feedback:
        response_blocks.append(build_qa_feedback_block(message_id=message_id))

    if not skip_quotes:
        response_blocks.extend(quotes_blocks)
    response_blocks.append(DividerBlock())

    return response_blocks


def build_follow_up_block(message_id: int | None) -> ActionsBlock:
    return ActionsBlock(
        block_id=build_feedback_id(message_id) if message_id is not None else None,
        elements=[
            ButtonElement(
                action_id=FOLLOWUP_BUTTON_ACTION_ID,
                style="danger",
                text="I need more help from a human!",
            )
        ],
    )


def build_follow_up_resolved_blocks(
    tag_ids: list[str], group_ids: list[str]
) -> list[Block]:
    tag_str = " ".join([f"<@{tag}>" for tag in tag_ids])
    if tag_str:
        tag_str += " "

    group_str = " ".join([f"<!subteam^{group}>" for group in group_ids])
    if group_str:
        group_str += " "

    text = (
        tag_str
        + group_str
        + "Someone has requested more help.\n\n:point_down:Please mark this resolved after answering!"
    )
    text_block = SectionBlock(text=text)
    button_block = ActionsBlock(
        elements=[
            ButtonElement(
                action_id=FOLLOWUP_BUTTON_RESOLVED_ACTION_ID,
                style="primary",
                text="Mark Resolved",
            )
        ]
    )
    return [text_block, button_block]