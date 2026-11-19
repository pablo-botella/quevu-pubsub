---
mkskill:
  pos: 100
  in: "*"
---

## Two directions that are not symmetric

In Pub/Sub the two ends have different names:

- **Output** publishes to a **topic**.
- **Input** reads from a **subscription** to that topic. A subscription is
  created on the Google side and is a different thing from the topic.

A message that has been received must be acknowledged, or it is delivered
again. So receiving is *at least once*. The source acknowledges a message
only after the log has accepted it (`quevu.Intake.Append` returned without
error). The worst that can happen is that a message arrives twice; it is
never lost between the subscription and the log.

One chunk is one message.

## Configuration

```go
qpubsub.Config{
	ProjectID:       "...", // required
	Topic:           "...", // for a Consumer
	Subscription:    "...", // for a Source
	CredentialsFile: "",    // a service account key file
	CredentialsJSON: nil,   // or its contents
	Endpoint:        "",    // an emulator: no credentials, no encryption
}
```

With no credentials given they are looked for the way the Google libraries
do (Application Default Credentials). Credentials belong in the
configuration of the deployment. **Never write real ones in source code,
in an example or in a test.**

Only service account keys are accepted through `Config`: the Google
library warns against loading a credential configuration of unknown type,
so this module does not offer it.

## What travels

Pub/Sub carries bytes, so a chunk travels as it is, whatever it contains,
even empty. Every message carries the position and the time the chunk had
in its log as attributes (`quevu-seq`, `quevu-time`). The receiving log
gives it a position of its own.

## Limits

- A publish request is at most **10 MB**.
- **A chunk too large for a message is not published and not split.**
  `Write` returns `ErrTooLarge` for it and publishes the rest; the log
  counts the batch as failed.

## Rules and gotchas

- **`Write` waits for the service to confirm** every message of the batch,
  so an error means something was not published.
- **Duplicates are possible** on the input side, and **order is not
  guaranteed**: the library delivers messages concurrently.
- **A subscription that fails ends the source** with that error; a log
  that refuses a message ends it too, and the message is delivered again
  later.
- **`Stop` lets the messages in hand finish**: they are in the log and
  acknowledged before it returns.
- **The emulator** takes `Config.Endpoint`. Not tried.
