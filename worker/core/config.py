from __future__ import annotations

import json
import logging
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    # App / Env
    APP_NAME: str = "music-backend"
    ENV: str = "local"

    DATABASE_URL: str = ""

    # Spotify
    SPOTIFY_CLIENT_ID: str = ""
    SPOTIFY_CLIENT_SECRET: str = ""
    SPOTIFY_TOKEN_URL: str = "https://accounts.spotify.com/api/token"
    SPOTIFY_API_BASE: str = "https://api.spotify.com/v1"
    SPOTIFY_DEFAULT_MARKET: str = "KR"

    # Last.fm (FEAT-multi-user Phase 3a) — public-profile reads need only an api_key
    # + username (no OAuth). The key lives in the SSM /myblog/worker blob (no new IAM).
    # OPTIONAL: unset ⇒ the poll no-ops (never a boot failure).
    LASTFM_API_KEY: str = ""
    LASTFM_API_BASE: str = "https://ws.audioscrobbler.com/2.0/"
    # Per-tick user bound so the 120s Lambda always finishes.
    LASTFM_MAX_USERS_PER_TICK: int = 50

    # Genius annotations (FEAT-lyrics-annotations Thread 1). Client access token
    # only — the Client ID/Secret are unused for reads. Lives in the SSM
    # /myblog/worker blob (no new IAM). OPTIONAL: unset ⇒ the job no-ops rather
    # than failing boot, exactly like the Last.fm key.
    GENIUS_ACCESS_TOKEN: str = ""
    GENIUS_API_BASE: str = "https://api.genius.com"
    # Tracks per invocation, sized from the WORST measured pass, not the average.
    # Per-track cost swings with how many referent pages a song has: a light batch
    # measured 2.32 s/track, an annotation-heavy one 4.55 s/track. The Lambda budget
    # is 120s, so 15 × 4.55 ≈ 68s leaves real headroom, while the 25 this started at
    # would have been ~114s and timed out on any heavy batch.
    GENIUS_FETCH_BATCH_LIMIT: int = 15
    # Below this blended title+artist similarity the match is `ambiguous` and its
    # annotations are NOT written — §6.2's wrong match (로꼬's "2025" resolved to
    # another artist's song) blends to 0.40 and is rejected here.
    GENIUS_MIN_CONFIDENCE: float = 0.62
    # The title must clear its OWN floor, because the blend leans on the artist and
    # therefore cannot catch the mirror failure: right artist, wrong song. Measured
    # live — our "GUIZ CORLEONE" resolved to Freeze Corleone's "Braquage" at title
    # 0.19 / artist 1.0 / blend 0.676, cleared the threshold above, and wrote 18
    # annotations from a different song. Correct matches score 1.0; a remix or
    # feat-suffixed variant still lands well above 0.5.
    GENIUS_MIN_TITLE_SIMILARITY: float = 0.5

    # Spotify user-scoped player reads (FEAT-member-dashboard Step 3).
    # Refresh token + client creds live in the SSM SecureString /myblog/spotify
    # (Q17; CHORE-secrets-ssm-migration moved them off Secrets Manager). This
    # worker both READS and WRITES that parameter — it is the only writer of the
    # rotated refresh token. SPOTIFY_REFRESH_TOKEN is an env fallback for local
    # dev / tests only.
    SPOTIFY_SECRETS_PARAM: str = ""

    # YouTube Data API v3 (FEAT-youtube-playback-provider Step A5).
    # `videos.list` ONLY — discovery (`search.list`, 100 units) lives in
    # myblog_music and must not gain a second home. This job refreshes stored
    # mappings inside the III.E.4 30-day window and deletes what it could not
    # refresh.
    #
    # Its own SSM parameter, shared with music and backend — one key, one home,
    # one rotation. Deliberately NOT in the required-key check: every other job
    # in this Lambda must keep running when YouTube is unconfigured, and this
    # job no-ops loudly on its own instead.
    YOUTUBE_SECRETS_PARAM: str = ""
    YOUTUBE_API_KEY: str = ""
    YOUTUBE_API_BASE: str = "https://www.googleapis.com/youtube/v3"
    YOUTUBE_HTTP_TIMEOUT: float = 8.0
    # Rows examined per invocation. `videos.list` takes 50 ids per 1-unit call,
    # so 500 rows is 10 units — the whole catalog would be ~618 calls per 30
    # days (~21 units/day) if it were ever fully mapped. Bounded per invocation
    # so one run cannot become unbounded work on a Lambda clock.
    YOUTUBE_REFRESH_BATCH_LIMIT: int = 500
    # III.E.4.c/.d. NOT a tuning knob: a larger value ships stored API data past
    # the policy ceiling. The read path enforces the same number independently
    # (myblog_backend PlaybackService), so both must move together if it ever does.
    YOUTUBE_RETENTION_DAYS: int = 30
    SPOTIFY_REFRESH_TOKEN: str = ""

    # Per-user Spotify listening poll (FEAT-multi-user Phase 3b-d).
    # USER_TOKENS_KMS_KEY_ID = the customer-managed CMK (alias/myblog-user-tokens,
    # 3b-a) — Lambda env var, same as the backend connect path; needed only for the
    # rotation re-encrypt (Decrypt reads the key id from the envelope). Unset ⇒
    # decrypt+poll still work; a rotation keeps the old payload (logged).
    USER_TOKENS_KMS_KEY_ID: str = ""
    # Per-tick member bound so the 120s Lambda always finishes (the ≤5-user tier
    # means one tick covers everyone; stalest-synced-first rotation above that).
    SPOTIFY_MEMBER_MAX_USERS_PER_TICK: int = 10

    # AWS / SQS (for local testing convenience)
    AWS_DEFAULT_REGION: str = "ap-northeast-2"
    LOCALSTACK_ENDPOINT: str | None = None
    SQS_QUEUE_URL: str | None = None

    # Control flags
    DRY_RUN: bool = False

    # Spotify Library two-way sync (FEAT-spotify-library-sync Step 2).
    # PLAN-ONLY by default: the reconcile reads /me/albums, computes diffs, PULLs
    # pre-existing saved albums into the special bucket, stamps source, and updates
    # our DB state + logs the intended PUT/DELETE sets — but issues NO real
    # PUT/DELETE /me/albums. Flip True to execute real Spotify writes. (DB writes
    # always happen; only the Spotify mutations are gated.) The worker reads THIS
    # flag, never the SQS message, so a stray/replayed message can't force a write.
    SPOTIFY_LIBRARY_WRITES_ENABLED: bool = False

    # Scheduled album-catalog ingest (FEAT-album-catalog-ingest Step 2).
    # New-releases-only sweep of catalog artists: gates + bounds are curation
    # policy (owner-accepted 2026-06-10), NOT storage limits. INGEST_SINCE is the
    # mode switch — albums released before it are never batch-ingested (the
    # reactive candidates path covers back-catalog on demand); relax it to
    # enable backfill.
    # ARTIST_POP_MIN 60 → 50 (FEAT-release-calendar OQ5, owner-decided
    # 2026-07-12): aligned with RELEASE_POLL_POP_MIN so every calendar-watchlist
    # artist is also ingest-swept and its announced rows can flip to released.
    ARTIST_POP_MIN: int = 50
    ALBUM_POP_MIN: int = 20
    SWEEP_ARTISTS_PER_TICK: int = 30
    MAX_ENQUEUE_PER_TICK: int = 60
    MAX_CATALOG_ALBUMS: int = 5000
    # RFC DATA-catalog-noise-and-lyrics-coverage Step 2. Reversible by flipping
    # this flag; the ingest filter deletes nothing from the catalog.
    INGEST_EXCLUDE_CLASSICAL: bool = True
    INGEST_CLASSICAL_HOLDOUT_MOD: int = 20  # 1-in-20 keeps flowing → misclassification rate stays measured
    INGEST_CLASSICAL_ALLOWLIST: list[str] = []  # owner-opted-in classical artists (spotify_ids)
    INGEST_SINCE: str = "2026-06-10"

    # Multi-source upcoming-release poller (FEAT-release-calendar Step 4).
    # Watchlist floor + horizon are owner-decided 2026-07-12 (RFC OQ1/OQ2).
    # Per-tick artist bounds keep each EventBridge tick inside the 120s Lambda:
    # MB ~1 req/s (musicbrainzngs limiter) → 70 artists ≈ 70 s; iTunes 3.5 s
    # throttle → 22 artists ≈ 77 s (resolution pre-pass misses cost a 2nd
    # request, which the wall-clock budget absorbs). Coverage cadence at the
    # eventbridge.tf rates (MB hourly / iTunes 30 min): ≥50 tier ≈ 1,530
    # artists → full MB cycle ≈ 22 h, iTunes ≈ 21 h — "fresh within a day".
    RELEASE_POLL_POP_MIN: int = 50
    RELEASE_POLL_HORIZON_DAYS: int = 180
    RELEASE_POLL_MB_ARTISTS_PER_TICK: int = 70
    RELEASE_POLL_ITUNES_ARTISTS_PER_TICK: int = 22
    RELEASE_POLL_TIME_BUDGET_SEC: float = 90.0
    # Failed iTunes artistId resolutions are sentinel-cached (artist_source_ids
    # 'not_found') and re-attempted after this many days — new UPC-bearing
    # catalog albums can make a previously-unresolvable artist resolvable.
    RELEASE_POLL_RESOLVE_RETRY_DAYS: int = 30
    ITUNES_LOOKUP_URL: str = "https://itunes.apple.com/lookup"
    ITUNES_THROTTLE_S: float = 3.5

    # Release-day confirm via album_ingest (FEAT-release-calendar Step 5).
    # An ingested watchlist album confirms announced rows whose release_date is
    # within ±PROXIMITY days (probe: 11/11 exact date agreement across sources,
    # so 7 d absorbs small announce-date slips without fuzzy-merging distinct
    # releases). LOOKBACK bounds how far back an ingested album still counts as
    # calendar-relevant: the ingest rotation revisits an artist once per cycle
    # (~51 d at the OQ5 floor), and announced lead times run to p90 68 d, so
    # 90 d keeps late-swept releases confirmable without dragging deep
    # back-catalog into the calendar. Upper bound = RELEASE_POLL_HORIZON_DAYS.
    RELEASE_CONFIRM_DATE_PROXIMITY_DAYS: int = 7
    RELEASE_CONFIRM_LOOKBACK_DAYS: int = 90

    # Runtime secrets: SSM Parameter Store ONLY (CHORE-secrets-ssm-migration).
    # SECRETS_PARAM is an SSM SecureString name like /myblog/worker. The legacy
    # Secrets Manager fallback (SECRETS_ARN) was removed once the migration
    # completed — AWS Secrets Manager holds zero secrets in this account, so the
    # fallback could only ever turn an SSM failure into a silent empty load.
    SECRETS_PARAM: str = ""

    # Incremental lyrics collection (FEAT-lyrics-corpus Step 3, worker EventBridge job).
    # LRCLIB /api/search freshness path for newly-ingested tracks lacking a corpus row.
    # Bounded per invocation so the job always finishes inside the 120s Lambda timeout;
    # per-row commits make an over-budget batch resumable (leftovers picked up next run).
    # Concurrency mirrors the Phase 2 finding (~2.5 req/s effective LRCLIB cap around 20-30
    # workers; higher only adds throttle-skips). URL is the same endpoint the batch used.
    LYRICS_LRCLIB_SEARCH_URL: str = "https://lrclib.net/api/search"
    LYRICS_INCR_BATCH_LIMIT: int = 150
    LYRICS_INCR_CONCURRENCY: int = 20
    LYRICS_INCR_TIME_BUDGET_SEC: float = 90.0

    # Periodic reassessment (FEAT-lyrics-corpus Step 4, worker EventBridge job). Re-checks the
    # unresolved pool (not_found / ambiguous / review_required, stalest first) since LRCLIB
    # coverage grows over time — promotes on new evidence, refreshes otherwise, never overwrites
    # a good match. Same bounding as Step 3; a lower cadence (the rule) since coverage changes
    # slowly. Same shared eval loop, so the limit/concurrency/budget knobs mirror the Step 3 set.
    LYRICS_REASSESS_BATCH_LIMIT: int = 150
    LYRICS_REASSESS_CONCURRENCY: int = 20
    LYRICS_REASSESS_TIME_BUDGET_SEC: float = 90.0

    # How long a best-of-* matched row rests between supersession re-checks
    # (DATA-catalog-noise Step 3b). The reassessment queue puts the best-of backlog AHEAD of
    # the unresolved pool, which only terminates because a re-checked row goes quiet for this
    # long: ~16% of best-of rows cannot be superseded (a re-check reproduces the same best-of
    # basis, and the replacement guard refuses a lateral swap), so without a rest interval
    # they would sit at the head of a stalest-first queue forever and the job would never
    # return to unresolved recovery. 30 days also paces the genuine second chance — LRCLIB
    # coverage is what changes, and it changes slowly.
    LYRICS_BESTOF_RECHECK_INTERVAL_DAYS: int = 30

    # Album-scoped expedite (DATA-catalog-noise Step 4): re-check ONE album out of turn, fired
    # by an SQS message carrying "album_id". The cooldown is the idempotency bound — SQS is
    # at-least-once and the writer bumps updated_at, so without it a redelivered message would
    # re-run the whole album against LRCLIB. 600s comfortably covers redelivery (visibility
    # timeout is minutes) while still letting a human re-fire the same album within the hour.
    LYRICS_EXPEDITE_COOLDOWN_SEC: float = 600.0

    # Targeted source collection for V57 album demand (FEAT-lyrics-listening-experience
    # Step 3). Same shared eval loop as the two corpus jobs, so the limit/concurrency/budget
    # knobs mirror theirs; what differs is the selection (demanded albums, both the
    # never-evaluated and the parked arm) and the write-back into lyrics_album_tracks.
    LYRICS_DEMAND_BATCH_LIMIT: int = 150
    LYRICS_DEMAND_JOB_LIMIT: int = 50
    LYRICS_DEMAND_CONCURRENCY: int = 20
    LYRICS_DEMAND_TIME_BUDGET_SEC: float = 80.0
    # Whole-invocation ceiling. LYRICS_DEMAND_TIME_BUDGET_SEC bounds only the LRCLIB loop;
    # this job also runs a catalog pass before it and a V57 write-back after it, both of
    # which are per-row-committed round trips to a remote database. 100s leaves headroom
    # inside the 120s Lambda timeout for the phases to hand over cleanly, and per-row
    # commits make an over-budget run resumable rather than lossy.
    LYRICS_DEMAND_TOTAL_BUDGET_SEC: float = 100.0

    # OQ5 backoff ladder (owner-approved 2026-09-09): min(cap, max(base, 2 x previous)).
    # Applied ONLY when an evaluation actually completed and came back unresolved; a
    # transient LRCLIB failure writes nothing at all, so an outage cannot push waiting
    # demand out to the cap. There is no attempt ceiling and no attempts column — demand is
    # never discarded on a failure count, it only re-checks more slowly.
    #
    # 6h base: fast enough that an album demanded today is re-checked several times while
    # the member still remembers asking. 30d cap: the one interval this codebase already
    # justifies (LYRICS_BESTOF_RECHECK_INTERVAL_DAYS) with the reason that applies here
    # too — LRCLIB coverage is what changes, and it changes slowly. Note the cap is still
    # MORE aggressive than the status quo: the global unresolved pass cycles a given row
    # only every ~2-3 months (eventbridge.tf), so demanded albums strictly gain.
    LYRICS_DEMAND_SOURCE_RETRY_BASE_SEC: float = 21_600.0     # 6 hours
    LYRICS_DEMAND_SOURCE_RETRY_CAP_SEC: float = 2_592_000.0   # 30 days

    # Catalog resolution ladder — a different failure with a different time constant. An
    # album missing from the catalog is waiting on album ingest, which resolves in hours,
    # not on third-party lyric coverage. Retrying it on the 30-day ladder would strand a
    # demanded album that became ingestable the same afternoon.
    LYRICS_DEMAND_CATALOG_RETRY_BASE_SEC: float = 900.0       # 15 minutes
    LYRICS_DEMAND_CATALOG_RETRY_CAP_SEC: float = 86_400.0     # 24 hours

    # FEAT-lyrics-listening-experience Step 4 — the automatic demand producers. Read from
    # the worker's OWN settings and never from an SQS message, so a stray or replayed
    # message cannot switch production on. Default TRUE: Step 4's whole purpose is that
    # member saved albums and recent listening start creating demand without owner action,
    # and shipping it dormant would need a Terraform apply (which the workspace does not do
    # automatically) before the step delivered anything.
    #
    # ROLLBACK IS NOT A PLAIN REVERT, and this flag alone is not a rollback either. Both
    # stop *new* production and neither touches what has already been produced: a member's
    # `saved`/`recent` scope stays `active` and the Step 3 collector keeps serving every
    # demand row derived from their library. Worse, reverting also removes the disconnect
    # revoke in the backend's `IntegrationService.disconnect`, so a member who disconnects
    # AFTER the revert keeps a live scope that nothing can then revoke. Demand is DB state;
    # `git revert` does not reach it. The order that actually rolls back is:
    #   1. LYRICS_MEMBER_DEMAND_ENABLED=false      (stop producing)
    #   2. revoke every member's discovery scopes  (LyricsDemandStore.revoke_scopes over
    #      DISCOVERY_ORIGINS, or UPDATE lyrics_discovery_scopes SET active=false + delete
    #      the demand rows) — while the revoking code is still deployed
    #   3. THEN revert the commits
    LYRICS_MEMBER_DEMAND_ENABLED: bool = True

    # ISRC backfill (FEAT-lyrics-corpus Step 1b, worker EventBridge job). Bounded like every
    # other scheduled job so a run always finishes inside the 120s Lambda timeout: 500 tracks
    # is 10 Spotify chunks of 50, and the budget stops the loop rather than letting a 429
    # burst (retry backoff caps at 8s/attempt, 3 attempts) run past the timeout mid-batch.
    # Committed batches survive a cut-off; the remainder is simply re-selected next run.
    ISRC_BACKFILL_BATCH_LIMIT: int = 500
    ISRC_BACKFILL_TIME_BUDGET_SEC: float = 90.0

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    SQS_MAX_MESSAGES: int = 1
    SQS_WAIT_TIME_SECONDS: int = 10
    SQS_RETRY_DELAY_SECONDS: int = 5


def _load_secrets(param: str) -> dict:
    """Load the secret JSON dict from SSM Parameter Store (SecureString).

    SSM is the only source (CHORE-secrets-ssm-migration). A failure is raised,
    not swallowed: the caller's required-key check below would have turned a
    returned ``{}`` into a ValueError naming the wrong subsystem, and an
    IAM/network failure is not the same condition as "the parameter is missing
    a key". Same shape in ``myblog_backend`` and ``myblog_music``.

    Everything that can fail is inside the ``try``: constructing the client
    (``NoRegionError``) and parsing the value (``JSONDecodeError``) are as much
    "the load failed" as the API call is, and each must still produce a log line
    naming the parameter.
    """
    import boto3

    try:
        ssm = boto3.client("ssm", region_name="ap-northeast-2")
        raw = ssm.get_parameter(Name=param, WithDecryption=True)["Parameter"]["Value"]
        return json.loads(raw)
    except Exception as e:
        logger.error("SSM load failed for %s: %s", param, e)
        raise


@lru_cache
def get_settings() -> Settings:
    s = Settings()
    if s.SECRETS_PARAM:
        secrets = _load_secrets(s.SECRETS_PARAM)
        if secrets.get("DATABASE_URL"):
            s.DATABASE_URL = secrets["DATABASE_URL"]
        if secrets.get("SPOTIFY_CLIENT_ID"):
            s.SPOTIFY_CLIENT_ID = secrets["SPOTIFY_CLIENT_ID"]
        if secrets.get("SPOTIFY_CLIENT_SECRET"):
            s.SPOTIFY_CLIENT_SECRET = secrets["SPOTIFY_CLIENT_SECRET"]
        # Last.fm key is OPTIONAL — absent ⇒ the poll no-ops; do NOT add to `missing`.
        if secrets.get("LASTFM_API_KEY"):
            s.LASTFM_API_KEY = secrets["LASTFM_API_KEY"]
        # Genius token is OPTIONAL on the same terms — absent ⇒ the fetch job
        # no-ops. It must never join `missing`, or every other job in this Lambda
        # dies at import time because one optional integration is unconfigured.
        if secrets.get("GENIUS_ACCESS_TOKEN"):
            s.GENIUS_ACCESS_TOKEN = secrets["GENIUS_ACCESS_TOKEN"]
        missing = [k for k, v in {
            "DATABASE_URL": s.DATABASE_URL,
            "SPOTIFY_CLIENT_ID": s.SPOTIFY_CLIENT_ID,
            "SPOTIFY_CLIENT_SECRET": s.SPOTIFY_CLIENT_SECRET,
        }.items() if not v]
        if missing:
            raise ValueError(
                f"Required secrets missing after SSM load: {missing}. "
                f"Check the {s.SECRETS_PARAM} SecureString and the Lambda role's ssm:GetParameter policy."
            )
    # Loaded separately and NOT required — see YOUTUBE_SECRETS_PARAM above. The
    # failure is logged and swallowed, which is correct HERE and nowhere else in
    # this function, because the fallback state is "the YouTube job refuses to
    # run" rather than "some other job proceeds on a default".
    if s.YOUTUBE_SECRETS_PARAM and not s.YOUTUBE_API_KEY:
        try:
            s.YOUTUBE_API_KEY = _load_secrets(s.YOUTUBE_SECRETS_PARAM).get("YOUTUBE_API_KEY", "")
        except Exception:
            logger.error(
                "YouTube secret load failed for %s; the refresh job will refuse to run.",
                s.YOUTUBE_SECRETS_PARAM,
            )

    return s


settings = get_settings()
