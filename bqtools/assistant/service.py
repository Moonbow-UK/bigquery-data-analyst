from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Protocol, Sequence

from openai import OpenAI


@dataclass(slots=True)
class AssistantSection:
    type: str
    title: str | None = None
    text: str | None = None
    items: list[str] | None = None
    headers: list[str] | None = None
    rows: list[list[str]] | None = None


@dataclass(slots=True)
class AssistantResponse:
    summary: str
    highlights: list[str] = field(default_factory=list)
    followups: list[str] = field(default_factory=list)
    sections: list[AssistantSection] = field(default_factory=list)
    sources: list[dict[str, Any]] = field(default_factory=list)
    raw_payload: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "answer": self.summary,
            "highlights": self.highlights,
            "followups": self.followups,
            "sections": [
                {
                    "type": section.type,
                    "title": section.title,
                    "text": section.text,
                    "items": section.items,
                    "headers": section.headers,
                    "rows": section.rows,
                }
                for section in self.sections
            ],
            "sources": self.sources,
            "raw": self.raw_payload,
        }


class BaseChatProvider(Protocol):
    def embed(self, text: str) -> list[float]:
        ...

    def chat(self, *, system_prompt: str, user_prompt: str, response_format: str = "json_object") -> dict[str, Any]:
        ...


@dataclass(slots=True)
class OpenAISettings:
    api_key: str
    chat_model: str
    embed_model: str


class OpenAIChatProvider(BaseChatProvider):
    def __init__(self, settings: OpenAISettings) -> None:
        self._client = OpenAI(api_key=settings.api_key)
        self._chat_model = settings.chat_model
        self._embed_model = settings.embed_model

    def embed(self, text: str) -> list[float]:
        response = self._client.embeddings.create(model=self._embed_model, input=text)
        return response.data[0].embedding

    def chat(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        response_format: str = "json_object",
    ) -> dict[str, Any]:
        completion = self._client.chat.completions.create(
            model=self._chat_model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={"type": response_format},
        )
        if not completion.choices:
            return {}
        content = completion.choices[0].message.content or ""
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return {"summary": content}


class AssistantService:
    def __init__(
        self,
        *,
        provider: BaseChatProvider,
        system_prompt: str,
        default_followups: Sequence[str] | None = None,
    ) -> None:
        self._provider = provider
        self._system_prompt = system_prompt
        self._default_followups = list(default_followups or [])

    def embed_question(self, question: str) -> list[float]:
        return self._provider.embed(question)

    def generate(
        self,
        *,
        question: str,
        contexts: Sequence[str],
        suggestions: Sequence[str],
        sources: list[dict[str, Any]] | None = None,
    ) -> AssistantResponse:
        context_text = "\n\n---\n\n".join(contexts)
        user_prompt = (
            f"User question: {question}\n\n"
            "Use only the following GA4 summary context when answering. "
            "If the context does not contain the required details, clearly explain what is missing.\n\n"
            f"{context_text}"
        )
        payload = self._provider.chat(system_prompt=self._system_prompt, user_prompt=user_prompt)
        if not isinstance(payload, dict):
            payload = {"summary": str(payload)}

        summary_text = str(payload.get("summary") or payload.get("answer") or "").strip()
        highlights = _coerce_list_of_strings(payload.get("highlights"))
        followups = _coerce_list_of_strings(payload.get("followups"))
        sections_payload = payload.get("sections") or []
        sections = _parse_sections(sections_payload)

        if not summary_text:
            summary_text = "The assistant could not derive an answer from the current summaries."
        if not followups:
            followups = list(self._default_followups or suggestions[:3])

        response = AssistantResponse(
            summary=summary_text,
            highlights=highlights,
            followups=followups[:3],
            sections=sections,
            sources=sources or [],
            raw_payload=payload,
        )
        return response


def _coerce_list_of_strings(value: Any) -> list[str]:
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
        return []
    results: list[str] = []
    for entry in value:
        if isinstance(entry, str):
            text = entry.strip()
            if text:
                results.append(text)
    return results


def _parse_sections(payload: Any) -> list[AssistantSection]:
    sections: list[AssistantSection] = []
    if not isinstance(payload, list):
        return sections
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        section_type = str(entry.get("type") or "").strip().lower()
        if not section_type:
            continue
        title = entry.get("title")
        text = entry.get("text")
        items = _coerce_list_of_strings(entry.get("items"))
        headers = _coerce_list_of_strings(entry.get("headers"))
        rows_payload = entry.get("rows")
        rows: list[list[str]] = []
        if isinstance(rows_payload, list):
            for row in rows_payload[:10]:
                if isinstance(row, list):
                    rows.append([str(cell) for cell in row[:10]])
        sections.append(
            AssistantSection(
                type=section_type,
                title=title if isinstance(title, str) else None,
                text=text if isinstance(text, str) else None,
                items=items if items else None,
                headers=headers if headers else None,
                rows=rows if rows else None,
            )
        )
    return sections
