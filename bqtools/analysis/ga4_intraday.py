from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

from ..services.dataset_summary import DatasetSummaryOptions


@dataclass
class GA4IntradaySummary:
    report_date: datetime
    total_events: int
    unique_users: int
    unique_sessions: int
    engaged_sessions: int
    total_engagement_ms: int
    total_revenue: float
    events_counter: Counter[str]
    page_titles: Counter[str]
    device_categories: Counter[str]
    operating_systems: Counter[str]
    countries: Counter[str]
    login_status: Counter[str]
    conversions: int
    conversion_rate: float | None


def generate_ga4_intraday_summary(csv_path: Path, options: DatasetSummaryOptions) -> tuple[GA4IntradaySummary, str]:
    rows = list(_iterate_rows(csv_path))
    if not rows:
        raise RuntimeError(f"CSV {csv_path} did not contain any events")

    events_counter: Counter[str] = Counter()
    page_titles: Counter[str] = Counter()
    device_categories: Counter[str] = Counter()
    operating_systems: Counter[str] = Counter()
    countries: Counter[str] = Counter()
    login_status: Counter[str] = Counter()
    session_engaged: set[str] = set()
    sessions: set[str] = set()
    users: set[str] = set()
    total_events = 0
    engagement_ms_per_session: defaultdict[str, int] = defaultdict(int)
    revenue_total = 0.0
    conversions = 0
    for row in rows:
        total_events += 1
        events_counter[row.event_name] += 1
        if row.user_pseudo_id:
            users.add(row.user_pseudo_id)
        if row.session_id:
            sessions.add(row.session_id)
            if row.session_engaged:
                session_engaged.add(row.session_id)
            if row.engagement_msec:
                engagement_ms_per_session[row.session_id] += row.engagement_msec
        if row.page_title:
            page_titles[row.page_title] += 1
        if row.device_category:
            device_categories[row.device_category] += 1
        if row.operating_system:
            operating_systems[row.operating_system] += 1
        if row.country:
            countries[row.country] += 1
        if row.login_status is not None:
            login_status[row.login_status] += 1
        if row.event_value_usd is not None:
            revenue_total += row.event_value_usd
        if row.event_name.lower() == "purchase":
            conversions += 1

    report_date = datetime.strptime(rows[0].event_date, "%Y%m%d")
    unique_sessions_count = len(sessions)
    conversion_rate = conversions / unique_sessions_count if unique_sessions_count else None
    summary = GA4IntradaySummary(
        report_date=report_date,
        total_events=total_events,
        unique_users=len(users),
        unique_sessions=unique_sessions_count,
        engaged_sessions=len(session_engaged),
        total_engagement_ms=sum(engagement_ms_per_session.values()),
        total_revenue=revenue_total,
        events_counter=events_counter,
        page_titles=page_titles,
        device_categories=device_categories,
        operating_systems=operating_systems,
        countries=countries,
        login_status=login_status,
        conversions=conversions,
        conversion_rate=conversion_rate,
    )

    report_text = _render_report(summary, options)
    return summary, report_text


def _render_report(summary: GA4IntradaySummary, options: DatasetSummaryOptions) -> str:
    lines: list[str] = []
    date_str = summary.report_date.strftime("%d %b %Y")
    lines.append(f"GA4 Intraday Event Overview — {date_str}")
    lines.append("=")
    lines.append("")

    avg_events_per_session = summary.total_events / summary.unique_sessions if summary.unique_sessions else 0
    avg_engagement_seconds = (
        (summary.total_engagement_ms / 1000) / summary.unique_sessions if summary.unique_sessions else 0
    )
    engaged_rate = summary.engaged_sessions / summary.unique_sessions if summary.unique_sessions else 0

    lines.append("Key Metrics")
    lines.append("-----------")
    lines.append(f"• Total events: {summary.total_events:,}")
    lines.append(f"• Unique users: {summary.unique_users:,}")
    lines.append(f"• Unique sessions: {summary.unique_sessions:,}")
    lines.append(f"• Engaged sessions: {summary.engaged_sessions:,} ({engaged_rate:.1%})")
    lines.append(f"• Avg. events per session: {avg_events_per_session:.1f}")
    lines.append(f"• Avg. engagement per session: {avg_engagement_seconds:.1f} sec")
    if summary.conversion_rate is not None:
        lines.append(f"• Conversions: {summary.conversions:,} ({summary.conversion_rate:.1%})")
    else:
        lines.append(f"• Conversions: {summary.conversions:,} (conversion rate unavailable)")
    if summary.total_revenue:
        lines.append(f"• Revenue (USD): ${summary.total_revenue:,.2f}")
    lines.append("")

    lines.append("Event Performance")
    lines.append("-----------------")
    top_events = summary.events_counter.most_common(5)
    for name, count in top_events:
        share = count / summary.total_events if summary.total_events else 0
        lines.append(f"• {name}: {count:,} events ({share:.1%})")
    if not top_events:
        lines.append("(no events recorded)")
    lines.append("")

    if summary.page_titles:
        lines.append("Top Screens / Pages")
        lines.append("-------------------")
        for title, count in summary.page_titles.most_common(options.max_categorical_columns or 5):
            lines.append(f"• {title} — {count:,} events")
        lines.append("")

    if summary.device_categories:
        lines.append("Device Mix")
        lines.append("-----------")
        total_device_events = sum(summary.device_categories.values())
        for category, count in summary.device_categories.most_common():
            share = count / total_device_events if total_device_events else 0
            lines.append(f"• {category.title()}: {count:,} events ({share:.1%})")
        lines.append("")

    if summary.countries:
        lines.append("Top Countries")
        lines.append("-------------")
        total_country_events = sum(summary.countries.values())
        for country, count in summary.countries.most_common(5):
            share = count / total_country_events if total_country_events else 0
            lines.append(f"• {country}: {count:,} events ({share:.1%})")
        lines.append("")

    if summary.login_status:
        total_login = sum(summary.login_status.values())
        lines.append("Login State")
        lines.append("-----------")
        for state, count in summary.login_status.most_common():
            share = count / total_login if total_login else 0
            lines.append(f"• {state.capitalize()}: {count:,} events ({share:.1%})")
        lines.append("")

    insights = _build_insights(summary)
    if insights:
        lines.append("Analyst Highlights")
        lines.append("-------------------")
        for insight in insights:
            lines.append(f"• {insight}")
        lines.append("")

    recommendations = _build_recommendations(summary)
    if recommendations:
        lines.append("Next-Step Suggestions")
        lines.append("----------------------")
        for rec in recommendations:
            lines.append(f"• {rec}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _build_insights(summary: GA4IntradaySummary) -> list[str]:
    insights: list[str] = []
    total_events = summary.total_events or 1
    if summary.events_counter:
        top_event, top_count = summary.events_counter.most_common(1)[0]
        share = top_count / total_events
        insights.append(
            f"{top_event} dominated engagement with {top_count:,} events ({share:.1%} of activity)."
        )
        if share > 0.6:
            insights.append(
                "Event distribution is highly concentrated. Validate instrumentation against funnel expectations."
            )

    if summary.page_titles:
        page, count = summary.page_titles.most_common(1)[0]
        insights.append(
            f"Most viewed page was “{page}” with {count:,} triggered events."
        )

    if summary.device_categories:
        device, count = summary.device_categories.most_common(1)[0]
        share = count / sum(summary.device_categories.values())
        insights.append(
            f"{device.title()} traffic drove {share:.1%} of events—optimise this experience first."
        )

    if summary.countries:
        country, count = summary.countries.most_common(1)[0]
        share = count / sum(summary.countries.values())
        insights.append(
            f"{country} leads geography mix at {share:.1%} of events."
        )

    if summary.conversion_rate is None and summary.unique_sessions:
        insights.append(
            "Conversion rate missing because session identifiers were absent in the export; ensure GA4 sessions are included for conversion analysis."
        )
    elif summary.conversions == 0:
        insights.append(
            "No purchase events recorded in this intraday slice—validate conversion tagging if transactions were expected."
        )

    return insights


def _build_recommendations(summary: GA4IntradaySummary) -> list[str]:
    recommendations: list[str] = []
    if summary.login_status:
        total = sum(summary.login_status.values())
        logged_in = summary.login_status.get("true", 0)
        ratio = logged_in / total if total else 0
        if ratio < 0.2:
            recommendations.append(
                "Low logged-in activity detected; review campaigns encouraging account sign-in or loyalty usage."
            )
    if summary.total_engagement_ms and summary.unique_sessions:
        avg_seconds = (summary.total_engagement_ms / 1000) / summary.unique_sessions
        if avg_seconds < 10:
            recommendations.append(
                "Session engagement is brief (<10s). Surface personalised content or simplify journeys to boost depth."
            )
    if summary.total_revenue == 0:
        recommendations.append(
            "No revenue recorded in this intraday slice—confirm conversion instrumentation if transactions were expected."
        )
    recommendations.append(
        "Feed this digest into an LLM (e.g. ChatGPT GA4 Data Analyst) to brainstorm customer questions and deeper drill-downs."
    )
    return recommendations


@dataclass
class _ParsedRow:
    event_date: str
    event_name: str
    session_id: Optional[str]
    session_engaged: bool
    engagement_msec: int
    event_value_usd: Optional[float]
    page_title: Optional[str]
    device_category: Optional[str]
    operating_system: Optional[str]
    country: Optional[str]
    login_status: Optional[str]
    user_pseudo_id: Optional[str]


def _iterate_rows(csv_path: Path) -> Iterable[_ParsedRow]:
    with csv_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            event_params = _parse_event_params(raw.get("event_params"))
            device_info = _safe_json(raw.get("device"))
            geo_info = _safe_json(raw.get("geo"))

            yield _ParsedRow(
                event_date=raw.get("event_date", ""),
                event_name=raw.get("event_name", "unknown"),
                session_id=_coerce_to_str(event_params.get("ga_session_id")),
                session_engaged=str(event_params.get("session_engaged", "")).lower() in {"1", "true", "yes"},
                engagement_msec=int(event_params.get("engagement_time_msec") or 0),
                event_value_usd=_coerce_float(raw.get("event_value_in_usd")),
                page_title=_coerce_to_str(event_params.get("page_title")) or None,
                device_category=_coerce_to_str(device_info.get("category")) if device_info else None,
                operating_system=_coerce_to_str(device_info.get("operating_system")) if device_info else None,
                country=_coerce_to_str(geo_info.get("country")) if geo_info else None,
                login_status=_normalise_login(event_params.get("login_status")),
                user_pseudo_id=raw.get("user_pseudo_id") or None,
            )


def _parse_event_params(raw: Optional[str]) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    params: dict[str, Any] = {}
    for entry in data:
        if not isinstance(entry, dict):
            continue
        key = entry.get("key")
        if not key:
            continue
        value = entry.get("value", {})
        params[key] = _extract_value(value) if isinstance(value, dict) else value
    return params


def _extract_value(value_dict: dict[str, Any]) -> Any:
    for key in ("string_value", "int_value", "double_value", "float_value"):
        if value_dict.get(key) is not None:
            return value_dict[key]
    return None


def _safe_json(raw: Optional[str]) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _coerce_to_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _coerce_float(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalise_login(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        val = value.strip().lower()
        if val in {"true", "1", "yes"}:
            return "true"
        if val in {"false", "0", "no"}:
            return "false"
        return val
    if isinstance(value, (int, float)):
        return "true" if value else "false"
    return str(value)
