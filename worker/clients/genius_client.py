# worker/clients/genius_client.py
"""
Genius read client — FEAT-lyrics-annotations Thread 1.

Official API only (https://api.genius.com), Bearer client-access-token. Three calls
per track: ``/search`` to find the song, ``/songs/{id}`` for the description and
credits, ``/referents`` for the annotations.

Facts about this API that are expensive to rediscover (RFC §6.3, NOTES §2):

* **There is no album object.** Fifteen song records, no album "about", no label
  field. Album-level facts are a derivative of track rows and never a source.
* **``song_relationships`` includes ``translations``** — 8–18 per track, every one a
  community translation page rather than a musical relationship. The caller drops
  them; counting them inflates "this album has 18 relations" nonsense.
* **``annotation_count`` exceeds the referent annotations by one per described
  track**, because Genius counts the song description as an annotation.
* **``language`` is one tag per song and unreliable** — LUX spans at least 11.
* **Verify with ``GET /songs/{id}``, never ``GET /account``**: account needs the
  ``me`` scope and 401s even on a perfectly good token.

Every request carries an explicit timeout (house rule). Transient failures retry with
linear backoff and then raise, so the caller can park the track rather than record a
false "not found".
"""
from __future__ import annotations

import logging
import time
import unicodedata
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx

from worker.core.config import settings

logger = logging.getLogger(__name__)

_USER_AGENT = "myblog-worker/1.0 (+https://www.ratemymusic.blog)"

# Relationship types that are NOT musical relationships. See the module docstring.
_DROP_RELATIONSHIPS = {"translations", "translation_of"}


class GeniusTransientError(RuntimeError):
    """Network / 5xx / 429 after retries — retry the track later, do not park it."""


class GeniusAuthError(RuntimeError):
    """401/403. The owner rotates the token themselves; stop rather than hunt."""


def _fold(text: str) -> str:
    """Accent- and case-insensitive comparison form, for match scoring."""
    text = unicodedata.normalize("NFKD", text or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join("".join(ch if ch.isalnum() or ch.isspace() else " " for ch in text.lower()).split())


def match_scores(
    want_title: str, want_artist: str, got_title: str, got_artist: str
) -> Tuple[float, float, float]:
    """``(title, artist, blended)`` similarity of a candidate against the request.

    The blend leans on the artist because a wrong artist is the loud failure —
    §6.2 found 로꼬's *2025* resolving to "2025 by Molly Yam (Ft. Loco)".

    **But the blend alone is not a sufficient test, and the caller must not treat it
    as one.** The quiet failure is the mirror image: right artist, wrong song. Our
    "GUIZ CORLEONE" (Guizmo, Freeze Corleone) resolved to Genius's "Braquage" by
    Freeze Corleone — title similarity 0.19, artist 1.0, blend **0.676**, over any
    threshold that lets real matches through. It wrote 18 annotations from a
    different song, all of which then failed to anchor. That is why the title score
    is returned separately and floored independently.
    """
    t = SequenceMatcher(None, _fold(want_title), _fold(got_title)).ratio()
    a = SequenceMatcher(None, _fold(want_artist), _fold(got_artist)).ratio()
    return round(t, 4), round(a, 4), round(0.4 * t + 0.6 * a, 4)


def match_confidence(
    want_title: str, want_artist: str, got_title: str, got_artist: str
) -> float:
    """Blended score only. Prefer :func:`match_scores` — see its docstring."""
    return match_scores(want_title, want_artist, got_title, got_artist)[2]


@dataclass
class GeniusSong:
    song_id: int
    title: str
    artist: str
    url: str
    confidence: float          # blended
    title_score: float = 0.0   # floored independently — see match_scores()
    artist_score: float = 0.0
    description: Optional[str] = None
    annotation_count: Optional[int] = None
    language: Optional[str] = None
    credits: Dict[str, Any] = field(default_factory=dict)
    relationships: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GeniusAnnotation:
    annotation_id: int
    referent_ordinal: int
    fragment: str
    body: Optional[str]
    votes_total: int
    is_verified: bool
    state: Optional[str]


class GeniusClient:
    """Thin, shared-``httpx.Client`` Genius reader."""

    def __init__(self, *, timeout: float = 15.0) -> None:
        self._timeout = timeout
        self._client: Optional[httpx.Client] = None

    @property
    def enabled(self) -> bool:
        return bool(settings.GENIUS_ACCESS_TOKEN)

    def _http(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                base_url=settings.GENIUS_API_BASE,
                timeout=self._timeout,          # explicit, house rule
                headers={
                    "Authorization": f"Bearer {settings.GENIUS_ACCESS_TOKEN}",
                    "User-Agent": _USER_AGENT,
                },
            )
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def _get(self, path: str, params: Dict[str, Any], *, max_retries: int = 3) -> Dict[str, Any]:
        last: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                r = self._http().get(path, params=params)
                if r.status_code in (401, 403):
                    # Never log the token itself.
                    raise GeniusAuthError(f"Genius {r.status_code} on {path} — token rejected")
                if r.status_code == 404:
                    return {}
                if r.status_code == 429 or r.status_code >= 500:
                    last = RuntimeError(f"HTTP {r.status_code}")
                else:
                    r.raise_for_status()
                    return r.json().get("response") or {}
            except GeniusAuthError:
                raise
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                last = exc
            except httpx.HTTPError as exc:
                last = exc
            time.sleep(0.4 * (attempt + 1))
        raise GeniusTransientError(f"Genius {path} failed after {max_retries} attempts: {last}")

    # ── the three calls ─────────────────────────────────────────────────────

    def find_song(self, title: str, artists: Sequence[str]) -> Optional[GeniusSong]:
        """Best ``/search`` hit for one track, scored. None when nothing comes back.

        ``artists`` is EVERY credited artist, most prominent first — not one.
        Genius names a collaboration by whichever credit it considers primary, and
        that is routinely not ours: our "Entertain" credits WILLOW while Genius says
        THE ANXIETY (the project name), and our "YO MA" credits 식케이 first while
        Genius says Leellamarz. Scoring against only the most popular credit rejected
        both as wrong-artist, when the right name was sitting second and third in our
        own list. The artist score is therefore the BEST match across all credits.

        Returns the best candidate WITHOUT applying a threshold — the caller decides
        what to do with a weak match, because "ambiguous" and "not_found" are
        different states and only the caller knows the policy.
        """
        names = [a for a in artists if a] or [""]
        resp = self._get("/search", {"q": f"{title} {names[0]}", "per_page": 5})
        hits = [h for h in (resp.get("hits") or []) if h.get("type") == "song"]
        best: Optional[GeniusSong] = None
        for h in hits:
            res = h.get("result") or {}
            got_title = res.get("title") or ""
            got_artist = (res.get("primary_artist") or {}).get("name") or ""
            ts, as_, conf = max(
                (match_scores(title, n, got_title, got_artist) for n in names),
                key=lambda s: s[2],
            )
            if best is None or conf > best.confidence:
                best = GeniusSong(
                    song_id=int(res.get("id")),
                    title=got_title,
                    artist=got_artist,
                    url=res.get("url") or "",
                    confidence=conf,
                    title_score=ts,
                    artist_score=as_,
                )
        return best

    def load_song(self, song: GeniusSong) -> GeniusSong:
        """Fill description / credits / relationships / language from ``/songs/{id}``."""
        resp = self._get(f"/songs/{song.song_id}", {"text_format": "plain"})
        s = resp.get("song") or {}
        song.description = ((s.get("description") or {}).get("plain") or "").strip() or None
        song.annotation_count = s.get("annotation_count")
        song.language = s.get("language")
        song.credits = {
            "writers": [a.get("name") for a in (s.get("writer_artists") or []) if a.get("name")],
            "producers": [a.get("name") for a in (s.get("producer_artists") or []) if a.get("name")],
            "performances": [
                {"role": p.get("label"), "artists": [a.get("name") for a in (p.get("artists") or [])]}
                for p in (s.get("custom_performances") or [])
            ],
        }
        rels: Dict[str, Any] = {}
        for rel in (s.get("song_relationships") or []):
            kind = rel.get("relationship_type") or rel.get("type")
            if not kind or kind in _DROP_RELATIONSHIPS:
                continue                      # community translation pages, not music
            names = [
                f"{(x.get('primary_artist') or {}).get('name', '')} — {x.get('title', '')}".strip(" —")
                for x in (rel.get("songs") or [])
            ]
            if names:
                rels[kind] = names
        song.relationships = rels
        return song

    def load_annotations(self, song_id: int, *, max_pages: int = 10) -> List[GeniusAnnotation]:
        """Every referent's annotations for one song, in document order.

        ``referent_ordinal`` is the referent's position in the returned order, which
        is the coordinate the reader falls back to when a fragment cannot be anchored.
        """
        out: List[GeniusAnnotation] = []
        ordinal = 0
        for page in range(1, max_pages + 1):
            resp = self._get(
                "/referents",
                {"song_id": song_id, "text_format": "plain", "per_page": 50, "page": page},
            )
            referents = resp.get("referents") or []
            if not referents:
                break
            for ref in referents:
                fragment = (ref.get("fragment") or "").strip()
                if not fragment:
                    continue
                ordinal += 1
                for ann in (ref.get("annotations") or []):
                    aid = ann.get("id")
                    if aid is None:
                        continue
                    body = ((ann.get("body") or {}).get("plain") or "").strip() or None
                    out.append(GeniusAnnotation(
                        annotation_id=int(aid),
                        referent_ordinal=ordinal,
                        fragment=fragment,
                        body=body,
                        votes_total=int(ann.get("votes_total") or 0),
                        is_verified=bool(ann.get("verified")),
                        state=ann.get("state"),
                    ))
            if len(referents) < 50:
                break
        return out


genius = GeniusClient()
