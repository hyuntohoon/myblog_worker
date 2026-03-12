# myblog_worker

> **MyBlog + Music Review** 프로젝트의 비동기 동기화 Worker — SQS Consumer + Spotify 데이터 정규화 + DB upsert

🔗 **전체 프로젝트 README:** [MyBlog + Music Review](https://github.com/hyuntohoon/myblog_front#관련-리포지토리)

---

## 개요

`myblog_music`이 SQS에 enqueue한 동기화 메시지를 소비하여 Spotify API에서 앨범·트랙·아티스트 데이터를 가져오고, 정규화한 뒤 DB에 upsert합니다. 요청-응답 경로와 완전히 분리된 **백그라운드 처리 전용** 서비스입니다.

---

## 처리 흐름

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
처리 완료 → SQS 메시지 삭제
```

---

## 핵심 설계

**배치 처리** — Spotify `/albums?ids=` API는 최대 20개를 한 번에 조회할 수 있습니다. SQS 메시지당 20개로 배치하여 외부 호출을 **N → ⌈N/20⌉** 로 축소했습니다.

**멱등성** — 동일 메시지가 중복 전달되거나 재처리되어도 DB가 깨지지 않습니다. `spotify_id` 기준 upsert(INSERT ON CONFLICT UPDATE)로 처리합니다.

**장애 격리** — Worker가 느려지거나 실패해도 사용자의 검색·상세 조회(DB-first)에는 영향이 없습니다. SQS가 재시도를 자동으로 처리합니다.

---

## 기술 스택

| 항목 | 기술 |
|------|------|
| 배포 | AWS Lambda (SQS 이벤트 소스 매핑) |
| 큐 | Amazon SQS |
| 데이터베이스 | Amazon RDS (PostgreSQL) |
| 외부 API | Spotify Web API |

---

## 환경 변수

| 변수 | 설명 |
|------|------|
| `DATABASE_URL` | RDS 접속 URL |
| `SPOTIFY_CLIENT_ID` | Spotify 앱 Client ID |
| `SPOTIFY_CLIENT_SECRET` | Spotify 앱 Client Secret |
| `SQS_QUEUE_URL` | SQS 큐 URL |
| `SQS_QUEUE_NAME` | SQS 큐 이름 |
| `AWS_REGION` | AWS 리전 |

---

## 왜 분리했는가

"외부 API 호출 + DB 쓰기"는 **비용·지연·실패 가능성**이 큰 작업입니다. 리소스 할당·타임아웃·재시도 전략이 요청-응답 API와 완전히 다르기 때문에 배포 단위를 분리하는 것이 합리적이었습니다.

- API Lambda는 빠른 응답이 핵심 → 짧은 타임아웃, 작은 메모리
- Worker Lambda는 안정적 처리가 핵심 → 긴 타임아웃, 재시도 허용

---

## 향후 개선

- SQS Dead Letter Queue(DLQ) 도입 + 실패 알림
- Worker 멱등성·중복 처리 강화
- CloudWatch 메트릭 기반 동기화 지연/실패 모니터링

---

## 관련 리포지토리

| 리포 | 역할 |
|------|------|
| [`myblog_front`](https://github.com/hyuntohoon/myblog_front) | 정적 사이트 + 글쓰기 UI |
| [`myblog_backend`](https://github.com/hyuntohoon/myblog_backend) | 글·카테고리 API + 인증 |
| [`myblog_music`](https://github.com/hyuntohoon/myblog_music) | DB-first 검색 + Sync 트리거 |
| **myblog_worker** (현재) | SQS Consumer + Spotify 동기화 |
| [`myblog_publish`](https://github.com/hyuntohoon/myblog_publish) | 정적 사이트 발행 |
