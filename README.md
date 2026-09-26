# ready-bell-demo

An example backend that uses [ready-bell.com](https://www.ready-bell.com), a UDP wake-up
service on `ready-bell.com:3137`.

The protocol has three plain-text UDP messages:

| Message             | Sent by              | Meaning                                         |
|---------------------|----------------------|-------------------------------------------------|
| `Listen <uuid> <s>` | whoever waits        | "Wake me up about `<uuid>` within `<s>` seconds" |
| `Notify <uuid>`     | whoever finishes     | "Work on `<uuid>` is done"                       |
| `Ready <uuid>`      | ready-bell.com       | Sent to every listener of `<uuid>` after a `Notify` |

With these messages a client can wait for a job without polling in a loop. It polls once
when `Ready` arrives, plus an occasional fallback poll in case a UDP packet is lost.

This is a Quarkus application, deployed on `home.bin932.com` on HTTPS port 3160. The
examples below use:

```sh
BASE=https://home.bin932.com:3160
```

---

## Example 1: hello-async (backend **fires** a notification)

`POST /hello-async/{uuid}` starts a slow job. When the job finishes, the backend sends
`Notify <uuid>` to ready-bell.com, and anyone listening on that uuid is woken up.

### 1. Generate a uuid

```sh
uuid=$(uuidgen | tr A-Z a-z)
echo $uuid
```

### 2. Terminal 1: listen for 120 seconds

```sh
echo "Listen $uuid 120" | nc -u -w 120 ready-bell.com 3137
```

### 3. Terminal 2: submit the job

Use the same `$uuid` here. `dbg-delay-sec` makes the job pretend to work for N seconds
(0–600).

```sh
curl -X POST -H 'Content-Type: application/json' \
  -d '{"input":{"name":"World","dbg-delay-sec":10}}' \
  $BASE/hello-async/$uuid
```

```json
{"id":"…","created":"…","modified":"…","status":"IN_PROGRESS",
 "input":{"name":"World"},"output":{},"error_message":null}
```

### 4. Terminal 2: get the job (poll)

```sh
curl $BASE/hello-async/$uuid
```

While the job is running it returns `IN_PROGRESS`. Once it is finished:

```json
{"id":"…","created":"…","modified":"…","status":"READY",
 "input":{"name":"World"},"output":{"greeting":"Hello, World!"},"error_message":null}
```

### What happens in terminal 1

`nc` waits without printing anything. About 10 seconds after the POST, the backend
finishes the job and sends `Notify <uuid>`. ready-bell.com passes it on, and terminal 1
prints:

```
Ready 3f0c…-your-uuid
```

That line tells a real client to make one final `GET`, which returns `READY` with the
greeting. If no `Ready` arrives within 120 seconds, `nc` exits without printing anything.

---

## Example 2: hello-cloud (backend **consumes** a notification)

In this example the backend is the one that waits. The work runs in AWS and signals
completion through ready-bell.com.

### Setup

- S3 bucket `ready-bell-demo` (region `eu-central-1`). Each job gets a folder `<uuid>/`.
- An AWS Lambda is triggered when `<uuid>/input.txt` is uploaded. It reads the input,
  writes `<uuid>/output.txt`, and sends `Notify <uuid>` to `ready-bell.com:3137`.
- The backend never calls the Lambda. It only talks to S3 and ready-bell.com.

```
client ──POST /hello-cloud/{uuid}──▶ backend ── presigned PUT/GET URLs ──▶ client
client ──PUT input.txt (presigned)──▶ S3 ──trigger──▶ Lambda
Lambda ──write output.txt──▶ S3
Lambda ──Notify <uuid>──▶ ready-bell.com ──Ready <uuid>──▶ backend
backend ──HEAD output.txt──▶ S3  →  status READY  ──SSE──▶ client
client ──GET output.txt (presigned)──▶ S3
```

### What the backend does

- **Presigned URLs**: for each job it creates a presigned PUT URL for `input.txt` and a
  presigned GET URL for `output.txt`, so the client transfers files to and from S3 directly.
- **Waits via ready-bell**: it sends `Listen <uuid> 60` and checks S3 with `HEAD`. It checks
  right away when `Ready <uuid>` arrives, and every 10 seconds as a fallback.
- **Status**: `NEW` → `IN_PROGRESS` (input.txt exists) → `READY` (output.txt exists).
  A job becomes `FAILED` if it isn't ready 2 minutes after creation. Jobs and their S3
  files are deleted after 2 hours.
- **SSE endpoint**: `GET /hello-cloud/{uuid}/sse` streams what happens in the backend
  for that job, so you can watch it live.

| SSE event  | Data                                      |
|------------|-------------------------------------------|
| `status`   | `NEW` / `IN_PROGRESS` / `READY` / `FAILED` |
| `check_s3` | Result of a HEAD, e.g. `HEAD output.txt 200` |
| `ready`    | `UDP received: Ready <uuid>`              |
| `timeout`  | `Job timed out`                           |
| `ping`     | Sent to keep the connection alive (once a minute when idle) |
| `end`      | Last event; the stream is closed after it |

### Walkthrough

```sh
uuid=$(uuidgen | tr A-Z a-z)

# 1. Create the job and get the presigned URLs
job=$(curl -s -X POST $BASE/hello-cloud/$uuid)
inputPutUrl=$(echo "$job" | jq -r .inputPutUrl)
outputGetUrl=$(echo "$job" | jq -r .outputGetUrl)

# 2. In another terminal, watch the backend (use the same uuid)
curl -N $BASE/hello-cloud/$uuid/sse

# 3. Upload the input, which triggers the Lambda
echo "World" | curl -X PUT --data-binary @- "$inputPutUrl"

# 4. Get the job status, then download the result
curl $BASE/hello-cloud/$uuid
curl "$outputGetUrl"
```

The SSE terminal shows something like this:

```
event: status     data: NEW
event: check_s3   data: HEAD input.txt 404
event: check_s3   data: HEAD output.txt 404
event: check_s3   data: HEAD input.txt 200
event: status     data: IN_PROGRESS
event: ready      data: UDP received: Ready <uuid>
event: check_s3   data: HEAD output.txt 200
event: status     data: READY
event: end        data: end of stream
```

The `ready` event is the ready-bell notification from the Lambda. It makes the backend
check S3 right away instead of waiting for the next 10-second poll.
