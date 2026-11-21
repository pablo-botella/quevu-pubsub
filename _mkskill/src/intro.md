---
mkskill:
  pos: 10
  in: readme
---

# quevu-pubsub

Google Cloud Pub/Sub as [quevu](https://github.com/pablo-botella/quevu-kernel)
components, in both directions.

> **Not verified against the real service.** It is tested against
> stand-ins for a topic and a subscription. Nothing here has talked to
> Google yet.

```go
cfg := qpubsub.Config{
	ProjectID:    os.Getenv("GCP_PROJECT"),
	Topic:        "orders",        // output goes to a topic
	Subscription: "orders-quevu",  // input comes from a subscription to it
}

out, _ := qpubsub.NewWriter(ctx, cfg)
orders, _ := quevu.NewQHub("orders", quevu.WithWriter(out))

in, _ := qpubsub.NewSource(ctx, cfg, incoming)
panel.Add("orders subscription", in)
```
