---
mkskill:
  pos: 5
  in: ai*
---

# quevu-pubsub - agent notes

Go package `github.com/pablo-botella/quevu-pubsub`, imported as `qpubsub`:
`qpubsub.Consumer` (a `quevu.Consumer`) and `qpubsub.Source` (a
`quevu.Unit` and `quevu.Exiter` that feeds a `quevu.Intake`). Depends on
`quevu` and on `cloud.google.com/go/pubsub/v2`, and on no other provider's
SDK.

Everything goes through two small interfaces, `publisher` and `receiver`,
which are what the tests replace. The module has never been run against
Google: treat any claim about the service as coming from the SDK, not from
experience.
