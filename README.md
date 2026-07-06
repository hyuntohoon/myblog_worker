# myblog_worker

> **MyBlog + Music Review** 프로젝트의 비동기 동기화 Worker — SQS Consumer + Spotify 데이터 정규화 + DB upsert

🔗 **전체 프로젝트 README:** [MyBlog + Music Review](https://github.com/hyuntohoon/myblog_front#관련-리포지토리)

---

## 개요

`myblog_music`이 SQS에 enqueue한 동기화 메시지를 소비하여 Spotify API에서 앨범·트랙·아티스트 데이터를 가져오고, 정규화한 뒤 DB에 upsert합니다. 요청-응답 경로와 완전히 분리된 **백그라운드 처리 전용** 서비스입니다.

### 반복 함정 (FIX-bug-audit-2026-07 WS-C)

- **DB 세션/트랜잭션을 수 분짜리 외부 API 루프에 걸쳐 열어두지 말 것.** Neon pooler가 idle-in-transaction 커넥션을 끊어 ProtocolViolation이 납니다. `fetch → materialize(읽기 세션 닫기) → 외부 루프 → 새 짧은 쓰기 세션` 패턴을 쓰세요 (lyrics 파이프라인이 레퍼런스). album_ingest / saved_tracks_sync / library_sync가 이 패턴으로 정리됨.
- **bulk `ON CONFLICT` upsert는 conflict key로 정렬**한 뒤 실행 (10-way SQS 동시성에서 row-lock 데드락 방지). artists뿐 아니라 albums/tracks도 정렬됨 (`sync_service.py`).
- **핸들러가 소유한 `session.begin()` 트랜잭션 안에서 raw `conn.commit()` 호출 금지** — 세션 트랜잭션을 deassociate시켜 `InvalidRequestError`가 납니다. 배치 커밋은 소유 세션으로, 실패 시 `session.rollback()`.

---

## 처리 흐름

두 개의 독립 트리거가 있습니다.

### 1. SQS 메시지 (Spotify 앨범 동기화 — 본 경로)

```
SQS 메시지 수신 (앨범 spotify_id 최대 20개)
  ↓
Spotify API 호출: GET /albums?ids=id1,id2,...,id20
  ↓
응답 데이터 정규화
  ├── Artist upsert (spotify_id 기준 멱등)
  ├── Album upsert (spotify_id 기준 멱등)
  ├── Track upsert (spotify_id 기준 멱등)
  ├── album_artists 관계 링크
  └── track_artists 관계 링크
  ↓
처리 완료 → SQS 메시지 삭제 (실패 시 ReportBatchItemFailures → DLQ)
```

### 2. EventBridge `rate(15 minutes)` (MusicBrainz alias 채움 — 별경로)

```
EventBridge 호출 (event['source'] == 'aws.events')
  ↓
musicbrainz_id IS NULL 인 아티스트 10개/tick 조회
  ↓
MusicBrainz lookup
  ├── 매칭 발견 → UPDATE artists.musicbrainz_id + aliases
  └── 미발견   → MBID_NOT_FOUND sentinel (반복 조회 방지)
```

SQS 경로와 분리되어 있어 MusicBrainz 지연/장애가 앨범 동기화를 막지 않습니다. (BUG-14 ~ BUG-19 에서 alias 매칭/대조 알고리즘 보강 — `docs/archive/done/rfcs/` 의 BUG-14/15/17/18/19 참조.)

### 3. EventBridge `rate(1 hour)` + 수동 SQS (Spotify 청취 캐시 — FEAT-member-dashboard Step 3)

```
EventBridge 1h 호출 (constant input {"job": "spotify_listening"})
  또는  수동 "지금 새로고침" SQS 메시지 ({"job": "spotify_refresh"})
  ↓
SpotifyUserClient (refresh token → access token, myblog/spotify 시크릿)
  ├── GET /me/player/recently-played (50개 롤링윈도우)
  │     → distinct 앨범 set 을 spotify_recent_albums 에 upsert + 윈도우 밖 prune (D25)
  │     → 카탈로그에 없는 앨범은 best-effort 로 album-sync SQS 재투입
  └── GET /me/player/currently-playing
        → spotify_now_playing 싱글톤 행 upsert (없으면 is_playing=false)
```

두 EventBridge 크론은 `event['job']` 으로 구분합니다(1h rule 은 constant input, alias rule 은
`source=aws.events`). user-facing 엔드포인트에서 동기 Spotify 호출은 절대 하지 않습니다(hard rule #9).

---

## 핵심 설계

**배치 처리** — Spotify `/albums?ids=` API는 최대 20개를 한 번에 조회할 수 있습니다. SQS 메시지당 20개로 배치하여 외부 호출을 **N → ⌈N/20⌉** 로 축소했습니다.

**멱등성** — 동일 메시지가 중복 전달되거나 재처리되어도 DB가 깨지지 않습니다. `spotify_id` 기준 upsert(INSERT ON CONFLICT UPDATE)로 처리합니다.

**장애 격리** — Worker가 느려지거나 실패해도 사용자의 검색·상세 조회(DB-first)에는 영향이 없습니다. SQS가 재시도를 자동으로 처리합니다.

---

## 기술 스택

| 항목         | 기술                                                       |
|--------------|------------------------------------------------------------|
| 배포         | AWS Lambda (SQS 이벤트 소스 매핑 + EventBridge rate(15 min)) |
| 큐           | Amazon SQS (album-sync FIFO + DLQ + `ReportBatchItemFailures`) |
| 데이터베이스 | Neon Serverless Postgres                                   |
| 외부 API     | Spotify Web API, MusicBrainz API                           |
| 도메인 모델  | `myblog-shared-db` (git-pinned, schema-only — 코드는 raw SQL 사용)  |

---

## 환경 변수

| 변수                    | 설명                                                                |
|-------------------------|---------------------------------------------------------------------|
| `SECRETS_ARN`           | AWS Secrets Manager `myblog/worker` 의 ARN (prod). cold-start 1회 fetch + `@lru_cache` |
| `DATABASE_URL`          | Neon 접속 URL (`postgresql+psycopg://...`) — local dev 시 직접 주입 |
| `SPOTIFY_CLIENT_ID`     | Spotify 앱 Client ID                                                |
| `SPOTIFY_CLIENT_SECRET` | Spotify 앱 Client Secret                                            |
| `SQS_QUEUE_URL`         | SQS 큐 URL                                                          |
| `SQS_QUEUE_NAME`        | SQS 큐 이름                                                         |
| `AWS_REGION`            | AWS 리전                                                            |

---

## 로컬 테스트

DB 의존 픽스처는 `TEST_DB_URL` 환경 변수가 있을 때만 동작합니다 (없으면 자동 skip). 로컬에서 실행하려면 AWS Secrets Manager 에서 받아옵니다:

```bash
export TEST_DB_URL=$(aws secretsmanager get-secret-value \
  --secret-id myblog/test-db --query SecretString --output text \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['TEST_DB_URL'])")
pytest tests/ -v
```

CI 는 GitHub Actions `secrets.TEST_DB_URL` 로 주입합니다 (`.github/workflows/deploy.yml`).

---

## 왜 분리했는가

"외부 API 호출 + DB 쓰기"는 **비용·지연·실패 가능성**이 큰 작업입니다. 리소스 할당·타임아웃·재시도 전략이 요청-응답 API와 완전히 다르기 때문에 배포 단위를 분리하는 것이 합리적이었습니다.

- API Lambda는 빠른 응답이 핵심 → 짧은 타임아웃, 작은 메모리
- Worker Lambda는 안정적 처리가 핵심 → 긴 타임아웃, 재시도 허용

---

## 향후 개선

- 실패 알림 (DLQ depth alarm 은 IAC-1 으로 도입됨; 알림 채널 연결만 남음)
- CloudWatch 메트릭 기반 동기화 지연 대시보드
- MusicBrainz alias 비매칭(non-English 아티스트) 보강 — 후속 RFC 필요

---

## 관련 리포지토리

| 리포                                                                   | 역할                                  |
|------------------------------------------------------------------------|---------------------------------------|
| [`myblog_front`](https://github.com/hyuntohoon/myblog_front)           | 정적 사이트 + 글쓰기 UI               |
| [`myblog_backend`](https://github.com/hyuntohoon/myblog_backend)       | 글·카테고리 API + 인증 + 발행         |
| [`myblog_music`](https://github.com/hyuntohoon/myblog_music)           | DB-first 검색 + Sync 트리거           |
| **myblog_worker** (현재)                                               | SQS Consumer + Spotify 동기화 + alias generation |
| [`myblog_shared_db`](https://github.com/hyuntohoon/myblog_shared_db)   | 공유 SQLAlchemy 모델 (git-pinned)     |

> 옛 `myblog_publish` 서비스는 ARCH-11 으로 backend 에 흡수되었고 업스트림은 archived 됨.
