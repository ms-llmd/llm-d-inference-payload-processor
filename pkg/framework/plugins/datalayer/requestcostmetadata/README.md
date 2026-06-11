# Request Cost Metadata Extractor

## What it is

A datasource extractor that turns each completed inference response into a per-request
cost sample and folds it into a per-model [t-digest](https://github.com/caio/go-tdigest)
stored on the Model's `AttributeMap`. It is registered as type
`request-cost-metadata-extractor` and runs on the same response-event loop as
`request-metadata-extractor`. It is a building block for the CostGuard scorer
(see [docs/proposals/050-costguard-scorer/README.md](../../../../../../docs/proposals/050-costguard-scorer/README.md)).

## What it does

1. Ignores `RequestEventType` events. Cost is observed only after a response.
2. On each `ResponseEventType` event:
   - Reads the model name from the request body's `model` field.
   - Reads `prompt_tokens` and `completion_tokens` from the response's `usage` block.
     Skips the sample (with a debug log) if either is absent or non-positive.
   - Reads the model's `*pricing.TokenPrices` from the AttributeMap under
     `pricing.TokenPricesAttributeKey`. Skips the sample (with a debug log) if absent —
     a model with no declared pricing has no defined cost. A model declared with
     `TokenPrices{0,0}` (a free model) is *not* skipped: it records `cost=0`.
   - Computes
     `cost = prompt_tokens * InputTokenPrice + completion_tokens * OutputTokenPrice`
     and adds the value to the model's running t-digest.
3. At the end of each `Extract` batch, for every model whose digest was updated and
   whose flush interval has elapsed since the last publish, writes a *clone* of the
   digest to the Model's AttributeMap under `pricing.CostDigestAttributeKey`. The
   stored value is a `*pricing.CostDigest`.

This extractor does not freeze and replace the digest at epoch boundaries — the
digest accumulates without bound. Epoch handling lands in a follow-up PR.

## Inputs consumed

- `dlsrc.ResponsePayload.Request.Body["model"]` — the model name (string).
- `dlsrc.ResponsePayload.Response.Body["usage"]` — a `map[string]any` containing
  `prompt_tokens` and `completion_tokens` as `float64`.
- `pricing.TokenPricesAttributeKey` on the Model's AttributeMap — populated by the
  `modelconfigcollector` plugin at startup and on config-file changes.

## Configuration

```json
{
  "compression":            200,
  "flushIntervalDuration":  "5s"
}
```

- `compression` (optional, default `200`): t-digest compression. Higher values
  trade memory for quantile accuracy. Must be `> 0`.
- `flushIntervalDuration` (optional, default `"5s"`): aggregation window before a
  per-model digest snapshot is published to the AttributeMap. Set to `"0s"` to
  publish on every event (used in unit tests). Must be `>= 0`.

## Known limitations

- **Side-effect creation of empty Models for unconfigured names.** When a
  response arrives for a model name that the operator never declared (i.e. a
  model with no `pricing.TokenPrices` attribute), this extractor's lookup
  goes through `Datastore.GetOrCreateModel`, which registers an empty Model
  in the datastore as a side effect. The cost sample is correctly skipped,
  but the model name leaks into `Datastore.Models()` and becomes visible to
  every other plugin that enumerates the store. This is a limitation of the
  current `Datastore` interface, which has no read-only `GetModel(name)`
  method. A follow-up PR will add `GetModel` to the interface and migrate
  this extractor to use it; once that lands, responses for unconfigured
  models will be skipped without any datastore mutation.
