# worker/run_local.py
import time, random
import boto3
from worker.handler import lambda_handler
from worker.core.config import settings

def main():
    # ----- 설정 -----
    max_msgs = int(getattr(settings, "SQS_MAX_MESSAGES", 1))         # 안전하게 1 유지 권장
    wait_secs = int(getattr(settings, "SQS_WAIT_TIME_SECONDS", 10))
    RATE_MIN_INTERVAL_SEC = float(getattr(settings, "RATE_MIN_INTERVAL_SEC", 1.2))  # 최소 호출 간격(초)
    POST_PROCESS_SLEEP_BASE = float(getattr(settings, "POST_PROCESS_SLEEP_BASE", 0.3))  # 처리 후 기본 슬립
    POST_PROCESS_SLEEP_JITTER = float(getattr(settings, "POST_PROCESS_SLEEP_JITTER", 0.2))  # 지터

    # ----- 클라이언트 & 큐 URL -----
    sqs = boto3.client(
        "sqs",
        region_name=settings.AWS_DEFAULT_REGION,
        endpoint_url=settings.LOCALSTACK_ENDPOINT,
    )
    queue_url = settings.SQS_QUEUE_URL or f"{settings.LOCALSTACK_ENDPOINT}/{getattr(settings, 'AWS_ACCOUNT_ID', '000000000000')}/{getattr(settings, 'QUEUE_NAME', 'test-queue')}"
    print(f"[POLLING] Listening on {queue_url} (max={max_msgs}, wait={wait_secs}s)")

    last_call_ts = 0.0

    while True:
        # ---- 최소 간격 보장 (전역 Rate Limit 완화) ----
        now = time.time()
        since = now - last_call_ts
        if since < RATE_MIN_INTERVAL_SEC:
            time.sleep(RATE_MIN_INTERVAL_SEC - since)

        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_msgs,
            WaitTimeSeconds=wait_secs,
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        msgs = resp.get("Messages", [])
        if not msgs:
            time.sleep(0.2)
            continue

        print(f"[POLLING] Got {len(msgs)} message(s)")
        event = {"Records": [{"body": m["Body"]} for m in msgs]}

        # ---- 핸들러 호출 ----
        try:
            lambda_handler(event, None)
        except Exception as e:
            print(f"[POLLING] Handler raised: {e}")
            # 이번 루프도 처리로 간주하고 다음 루프로 진행(메시지는 아래에서 삭제)

        # ---- 성공/실패 상관없이 삭제 (재시도 비활성 정책) ----
        for m in msgs:
            try:
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=m["ReceiptHandle"])
                print(f"[POLLING] Deleted message {m['MessageId']}")
            except Exception as e:
                print(f"[POLLING] Delete failed for {m.get('MessageId')}: {e}")

        # ---- 호출 간격/처리 후 슬립 ----
        last_call_ts = time.time()
        # 고정 슬립 + 지터(서로 다른 워커/루프가 겹치지 않도록)
        time.sleep(POST_PROCESS_SLEEP_BASE + random.uniform(0, POST_PROCESS_SLEEP_JITTER))

if __name__ == "__main__":
    main()