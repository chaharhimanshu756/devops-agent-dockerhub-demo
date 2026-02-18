import os, time
import boto3
from botocore.exceptions import ClientError

LOG_GROUP = os.environ["LOG_GROUP"]
LOG_STREAM = os.environ["LOG_STREAM"]
LOG_FILE = os.environ["LOG_FILE"]
REGION = os.environ.get("AWS_REGION", "us-east-1")

client = boto3.client("logs", region_name=REGION)

def ensure_stream():
    try:
        client.create_log_group(logGroupName=LOG_GROUP)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise
    try:
        client.create_log_stream(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
            raise

def seq_token():
    r = client.describe_log_streams(logGroupName=LOG_GROUP, logStreamNamePrefix=LOG_STREAM, limit=1)
    s = r.get("logStreams", [])
    return s[0].get("uploadSequenceToken") if s else None

def put(events, token):
    args = dict(logGroupName=LOG_GROUP, logStreamName=LOG_STREAM, logEvents=events)
    if token:
        args["sequenceToken"] = token
    return client.put_log_events(**args)["nextSequenceToken"]

def main():
    ensure_stream()
    token = seq_token()

    with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
        f.seek(0, 2)  # tail from end

        buf = []
        buf_bytes = 0
        last = time.time()

        while True:
            line = f.readline()
            if not line:
                if buf and (time.time() - last) > 1.5:
                    token = flush(buf, token)
                    buf, buf_bytes = [], 0
                    last = time.time()
                time.sleep(0.2)
                continue

            if line.strip() == "__CW_STOP__":
                break

            buf.append({"timestamp": int(time.time() * 1000), "message": line})
            buf_bytes += len(line.encode("utf-8", "ignore"))

            if buf_bytes > 800_000 or len(buf) > 500:
                token = flush(buf, token)
                buf, buf_bytes = [], 0
                last = time.time()

        if buf:
            flush(buf, token)

def flush(buf, token):
    buf.sort(key=lambda e: e["timestamp"])
    try:
        return put(buf, token)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("InvalidSequenceTokenException", "DataAlreadyAcceptedException"):
            return put(buf, seq_token())
        raise

if __name__ == "__main__":
    main()
