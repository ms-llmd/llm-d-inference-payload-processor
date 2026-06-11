/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package requestcostmetadata

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/datastore"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	dlsrc "github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer/datasource"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer/pricing"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
)

// fakeHandle implements plugin.Handle for unit tests.
type fakeHandle struct{ ds datalayer.Datastore }

func (f *fakeHandle) Context() context.Context                         { return context.Background() }
func (f *fakeHandle) Client() client.Client                            { return nil }
func (f *fakeHandle) ReconcilerBuilder() *ctrlbuilder.Builder          { return nil }
func (f *fakeHandle) Datastore() datalayer.Datastore                   { return f.ds }
func (f *fakeHandle) EventNotifier() datalayer.EventNotifier           { return nil }
func (f *fakeHandle) Plugin(string) plugin.Plugin                      { return nil }
func (f *fakeHandle) AddPlugin(string, plugin.Plugin)                  {}
func (f *fakeHandle) GetAllPlugins() []plugin.Plugin                   { return nil }
func (f *fakeHandle) GetAllPluginsWithNames() map[string]plugin.Plugin { return nil }

// makeResponseEvent builds a ResponseEventType event for the named model whose
// usage block reports promptTokens and completionTokens. Pass <= 0 to omit a
// field; pass omitUsage=true to omit the entire usage block.
func makeResponseEvent(model string, promptTokens, completionTokens float64, omitUsage bool) dlsrc.Event {
	req := requesthandling.NewInferenceRequest()
	req.Body["model"] = model
	resp := requesthandling.NewInferenceResponse()
	if !omitUsage {
		usage := map[string]any{}
		if promptTokens > 0 {
			usage["prompt_tokens"] = promptTokens
		}
		if completionTokens > 0 {
			usage["completion_tokens"] = completionTokens
		}
		resp.Body["usage"] = usage
	}
	return dlsrc.Event{
		Type:    dlsrc.ResponseEventType,
		Payload: dlsrc.ResponsePayload{Request: req, Response: resp},
	}
}

// setTokenPrices attaches a TokenPrices attribute to the named model in ds.
func setTokenPrices(ds datalayer.Datastore, model string, in, out float64) {
	ds.GetOrCreateModel(model).GetAttributes().Put(
		pricing.TokenPricesAttributeKey,
		&pricing.TokenPrices{InputTokenPrice: in, OutputTokenPrice: out},
	)
}

// readDigest fetches the *pricing.CostDigest for model from ds, returning
// (digest, true) if present and well-typed, or (nil, false) otherwise.
func readDigest(ds datalayer.Datastore, model string) (*pricing.CostDigest, bool) {
	v, ok := ds.GetOrCreateModel(model).GetAttributes().Get(pricing.CostDigestAttributeKey)
	if !ok {
		return nil, false
	}
	cd, ok := v.(*pricing.CostDigest)
	return cd, ok
}

// newTestExtractor builds an extractor with flushInterval=0 so every event
// flushes immediately, mirroring the requestmetadata test pattern. Tests
// exercising non-zero flush intervals build their extractor inline.
func newTestExtractor(t *testing.T) (*RequestCostMetadataExtractor, datalayer.Datastore) {
	t.Helper()
	ds := datastore.NewFakeDataStore()
	ext := NewRequestCostMetadataExtractor(ds).WithFlushInterval(0)
	return ext, ds
}

// --- Factory tests ---

func TestExtractorFactory_Defaults(t *testing.T) {
	ds := datastore.NewFakeDataStore()
	p, err := ExtractorFactory("x", nil, &fakeHandle{ds: ds})
	if err != nil {
		t.Fatalf("ExtractorFactory: %v", err)
	}
	ext := p.(*RequestCostMetadataExtractor)
	if ext.compression != defaultCompression {
		t.Errorf("compression = %v, want %v", ext.compression, defaultCompression)
	}
	if ext.flushInterval != defaultFlushIntervalDuration {
		t.Errorf("flushInterval = %v, want %v", ext.flushInterval, defaultFlushIntervalDuration)
	}
}

func TestExtractorFactory_HonorsConfig(t *testing.T) {
	ds := datastore.NewFakeDataStore()
	raw := json.RawMessage(`{"compression":50,"flushIntervalDuration":"1m"}`)
	p, err := ExtractorFactory("x", raw, &fakeHandle{ds: ds})
	if err != nil {
		t.Fatalf("ExtractorFactory: %v", err)
	}
	ext := p.(*RequestCostMetadataExtractor)
	if ext.compression != 50 {
		t.Errorf("compression = %v, want 50", ext.compression)
	}
	if ext.flushInterval != time.Minute {
		t.Errorf("flushInterval = %v, want 1m", ext.flushInterval)
	}
}

func TestExtractorFactory_RejectsInvalidJSON(t *testing.T) {
	ds := datastore.NewFakeDataStore()
	if _, err := ExtractorFactory("x", json.RawMessage(`{broken`), &fakeHandle{ds: ds}); err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestExtractorFactory_RejectsNonPositiveCompression(t *testing.T) {
	ds := datastore.NewFakeDataStore()
	raw := json.RawMessage(`{"compression":0,"flushIntervalDuration":"5s"}`)
	if _, err := ExtractorFactory("x", raw, &fakeHandle{ds: ds}); err == nil {
		t.Error("expected error for compression=0, got nil")
	}
}

func TestExtractorFactory_RejectsInvalidFlushInterval(t *testing.T) {
	ds := datastore.NewFakeDataStore()
	raw := json.RawMessage(`{"compression":200,"flushIntervalDuration":"not-a-duration"}`)
	if _, err := ExtractorFactory("x", raw, &fakeHandle{ds: ds}); err == nil {
		t.Error("expected error for invalid flushIntervalDuration, got nil")
	}
}

func TestExtractorFactory_RejectsNegativeFlushInterval(t *testing.T) {
	ds := datastore.NewFakeDataStore()
	raw := json.RawMessage(`{"compression":200,"flushIntervalDuration":"-1s"}`)
	if _, err := ExtractorFactory("x", raw, &fakeHandle{ds: ds}); err == nil {
		t.Error("expected error for negative flushIntervalDuration, got nil")
	}
}

// --- Extract tests ---

// TestExtract_PublishesCostDigest verifies the happy path: a response event
// for a model with TokenPrices produces a digest snapshot on the AttributeMap
// whose count includes the new sample.
func TestExtract_PublishesCostDigest(t *testing.T) {
	ext, ds := newTestExtractor(t)
	setTokenPrices(ds, "m1", 1e-6, 4e-6) // input $1/M, output $4/M (per token)

	ev := makeResponseEvent("m1", 100, 50, false)
	if err := ext.Extract(context.Background(), []dlsrc.Event{ev}); err != nil {
		t.Fatalf("Extract: %v", err)
	}

	cd, ok := readDigest(ds, "m1")
	if !ok {
		t.Fatal("expected CostDigest attribute to be present")
	}
	if cd.Digest.Count() != 1 {
		t.Errorf("digest count = %d, want 1", cd.Digest.Count())
	}
	// Cost = 100 * 1e-6 + 50 * 4e-6 = 1e-4 + 2e-4 = 3e-4. With one sample,
	// the digest's median should equal the inserted value.
	wantCost := 100.0*1e-6 + 50.0*4e-6
	if got := cd.Digest.Quantile(0.5); got != wantCost {
		t.Errorf("Quantile(0.5) = %v, want %v", got, wantCost)
	}
}

// TestExtract_AccumulatesMultipleSamples verifies that successive responses
// add samples to the same model's digest. With flushInterval=0 every event
// publishes, so the final attribute reflects all samples.
func TestExtract_AccumulatesMultipleSamples(t *testing.T) {
	ext, ds := newTestExtractor(t)
	setTokenPrices(ds, "m1", 1e-6, 1e-6)

	for i := range 5 {
		ev := makeResponseEvent("m1", 100, 100, false)
		if err := ext.Extract(context.Background(), []dlsrc.Event{ev}); err != nil {
			t.Fatalf("Extract iter %d: %v", i, err)
		}
	}

	cd, ok := readDigest(ds, "m1")
	if !ok {
		t.Fatal("expected CostDigest attribute to be present")
	}
	if cd.Digest.Count() != 5 {
		t.Errorf("digest count = %d, want 5", cd.Digest.Count())
	}
}

// TestExtract_SkipsRequestEvents verifies that RequestEventType events do not
// produce cost samples. (Cost is observable only on the response.)
func TestExtract_SkipsRequestEvents(t *testing.T) {
	ext, ds := newTestExtractor(t)
	setTokenPrices(ds, "m1", 1e-6, 1e-6)

	req := requesthandling.NewInferenceRequest()
	req.Body["model"] = "m1"
	ev := dlsrc.Event{Type: dlsrc.RequestEventType, Payload: dlsrc.RequestPayload{Request: req}}

	if err := ext.Extract(context.Background(), []dlsrc.Event{ev}); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if _, ok := readDigest(ds, "m1"); ok {
		t.Error("expected no CostDigest attribute after request-only batch")
	}
}

// TestExtract_SkipsMissingUsage verifies that a response with no usage block
// is skipped without panicking and without publishing a digest.
func TestExtract_SkipsMissingUsage(t *testing.T) {
	ext, ds := newTestExtractor(t)
	setTokenPrices(ds, "m1", 1e-6, 1e-6)

	ev := makeResponseEvent("m1", 0, 0, true)
	if err := ext.Extract(context.Background(), []dlsrc.Event{ev}); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if _, ok := readDigest(ds, "m1"); ok {
		t.Error("expected no CostDigest attribute when usage is missing")
	}
}

// TestExtract_SkipsMissingPromptTokens verifies that a usage block missing
// prompt_tokens is skipped (we do not impute zero — the sample is unusable).
func TestExtract_SkipsMissingPromptTokens(t *testing.T) {
	ext, ds := newTestExtractor(t)
	setTokenPrices(ds, "m1", 1e-6, 1e-6)

	ev := makeResponseEvent("m1", 0, 50, false) // promptTokens omitted
	if err := ext.Extract(context.Background(), []dlsrc.Event{ev}); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if _, ok := readDigest(ds, "m1"); ok {
		t.Error("expected no CostDigest attribute when prompt_tokens is missing")
	}
}

// TestExtract_SkipsMissingCompletionTokens mirrors the above for completion_tokens.
func TestExtract_SkipsMissingCompletionTokens(t *testing.T) {
	ext, ds := newTestExtractor(t)
	setTokenPrices(ds, "m1", 1e-6, 1e-6)

	ev := makeResponseEvent("m1", 100, 0, false) // completionTokens omitted
	if err := ext.Extract(context.Background(), []dlsrc.Event{ev}); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if _, ok := readDigest(ds, "m1"); ok {
		t.Error("expected no CostDigest attribute when completion_tokens is missing")
	}
}

// TestExtract_SkipsModelWithoutTokenPrices verifies that a model that has
// never been seen by the modelconfigcollector (and therefore has no
// TokenPrices attribute) is silently skipped — there is no defined cost.
func TestExtract_SkipsModelWithoutTokenPrices(t *testing.T) {
	ext, ds := newTestExtractor(t)
	// Note: setTokenPrices NOT called.

	ev := makeResponseEvent("m1", 100, 50, false)
	if err := ext.Extract(context.Background(), []dlsrc.Event{ev}); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if _, ok := readDigest(ds, "m1"); ok {
		t.Error("expected no CostDigest attribute when TokenPrices is absent")
	}
}

// TestExtract_FreeModelRecordsZeroSample verifies the locked-in decision: a
// model with TokenPrices{0,0} still produces a sample (cost=0) so CostGuard's
// arm-pull bookkeeping is not skewed by free models.
func TestExtract_FreeModelRecordsZeroSample(t *testing.T) {
	ext, ds := newTestExtractor(t)
	setTokenPrices(ds, "free", 0, 0)

	ev := makeResponseEvent("free", 100, 50, false)
	if err := ext.Extract(context.Background(), []dlsrc.Event{ev}); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	cd, ok := readDigest(ds, "free")
	if !ok {
		t.Fatal("expected CostDigest attribute even for free model")
	}
	if cd.Digest.Count() != 1 {
		t.Errorf("digest count = %d, want 1", cd.Digest.Count())
	}
	if got := cd.Digest.Quantile(0.5); got != 0 {
		t.Errorf("Quantile(0.5) = %v, want 0", got)
	}
}

// TestExtract_PerModelIsolation verifies that two models accumulate
// independent digests; a sample for model A does not appear in model B's
// digest.
func TestExtract_PerModelIsolation(t *testing.T) {
	ext, ds := newTestExtractor(t)
	setTokenPrices(ds, "a", 1e-6, 1e-6)
	setTokenPrices(ds, "b", 2e-6, 2e-6)

	batch := []dlsrc.Event{
		makeResponseEvent("a", 100, 100, false),
		makeResponseEvent("b", 100, 100, false),
		makeResponseEvent("a", 200, 200, false),
	}
	if err := ext.Extract(context.Background(), batch); err != nil {
		t.Fatalf("Extract: %v", err)
	}

	a, ok := readDigest(ds, "a")
	if !ok {
		t.Fatal("expected CostDigest for a")
	}
	if a.Digest.Count() != 2 {
		t.Errorf("a digest count = %d, want 2", a.Digest.Count())
	}
	b, ok := readDigest(ds, "b")
	if !ok {
		t.Fatal("expected CostDigest for b")
	}
	if b.Digest.Count() != 1 {
		t.Errorf("b digest count = %d, want 1", b.Digest.Count())
	}
}

// TestExtract_RejectsNonFloatTokenFields verifies that usage fields of the
// wrong Go type (e.g. int) are treated as malformed and the sample skipped.
// In production encoding/json decodes JSON numbers as float64 so this never
// happens, but a misconfigured upstream that constructs the map directly in
// Go could trip this — we want the failure mode to be "skip", not "panic".
func TestExtract_RejectsNonFloatTokenFields(t *testing.T) {
	ext, ds := newTestExtractor(t)
	setTokenPrices(ds, "m1", 1e-6, 1e-6)

	req := requesthandling.NewInferenceRequest()
	req.Body["model"] = "m1"
	resp := requesthandling.NewInferenceResponse()
	// int values, not float64 — the type assertion in extractTokenCounts must reject.
	resp.Body["usage"] = map[string]any{"prompt_tokens": 100, "completion_tokens": 50}
	ev := dlsrc.Event{
		Type:    dlsrc.ResponseEventType,
		Payload: dlsrc.ResponsePayload{Request: req, Response: resp},
	}

	if err := ext.Extract(context.Background(), []dlsrc.Event{ev}); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if _, ok := readDigest(ds, "m1"); ok {
		t.Error("expected no CostDigest when usage fields are non-float64")
	}
}

// TestExtract_MultipleEventsSameModelInBatch verifies that a single Extract
// call carrying several response events for the same model adds each cost
// sample to the digest and publishes the snapshot exactly once. The
// end-of-batch publish loop iterates a map keyed by model, so a regression
// that double-published or only retained the last sample would be visible
// here.
func TestExtract_MultipleEventsSameModelInBatch(t *testing.T) {
	ext, ds := newTestExtractor(t)
	setTokenPrices(ds, "m1", 1e-6, 1e-6)

	batch := []dlsrc.Event{
		makeResponseEvent("m1", 100, 100, false),
		makeResponseEvent("m1", 200, 200, false),
		makeResponseEvent("m1", 300, 300, false),
	}
	if err := ext.Extract(context.Background(), batch); err != nil {
		t.Fatalf("Extract: %v", err)
	}

	cd, ok := readDigest(ds, "m1")
	if !ok {
		t.Fatal("expected CostDigest for m1")
	}
	if cd.Digest.Count() != 3 {
		t.Errorf("digest count = %d, want 3 (one per response event in the batch)", cd.Digest.Count())
	}
}

// TestExtract_WrongPayloadTypeForResponseEvent verifies that an event tagged
// as ResponseEventType but carrying a RequestPayload (a programming error
// upstream) is silently skipped via the type-assertion guard rather than
// panicking. Locks the defensive `if !ok { continue }` branch.
func TestExtract_WrongPayloadTypeForResponseEvent(t *testing.T) {
	ext, ds := newTestExtractor(t)
	setTokenPrices(ds, "m1", 1e-6, 1e-6)

	req := requesthandling.NewInferenceRequest()
	req.Body["model"] = "m1"
	ev := dlsrc.Event{
		Type:    dlsrc.ResponseEventType,
		Payload: dlsrc.RequestPayload{Request: req}, // wrong payload type for ResponseEventType
	}

	if err := ext.Extract(context.Background(), []dlsrc.Event{ev}); err != nil {
		t.Fatalf("Extract: %v", err)
	}
	if _, ok := readDigest(ds, "m1"); ok {
		t.Error("expected no CostDigest when payload type does not match event type")
	}
}

// TestExtract_FlushIntervalDelaysPublish verifies that with a non-zero
// flushInterval, samples accumulate in memory but the AttributeMap snapshot
// is only refreshed once the interval has elapsed since the last publish.
//
// Uses small real durations: 50 ms keeps the test under 100 ms while being
// long enough to be robust on slow CI runners. The flush interval is set
// just longer than the per-iteration sleep to keep the early-iteration
// "no publish yet" assertion reliable.
func TestExtract_FlushIntervalDelaysPublish(t *testing.T) {
	ds := datastore.NewFakeDataStore()
	setTokenPrices(ds, "m1", 1e-6, 1e-6)

	const flushInterval = 50 * time.Millisecond
	ext := NewRequestCostMetadataExtractor(ds).WithFlushInterval(flushInterval)

	// First event creates the accumulator with lastFlush=now and adds a
	// sample. No publish: zero time has elapsed since lastFlush.
	ev := makeResponseEvent("m1", 100, 100, false)
	if err := ext.Extract(context.Background(), []dlsrc.Event{ev}); err != nil {
		t.Fatalf("Extract iter 0: %v", err)
	}
	if _, ok := readDigest(ds, "m1"); ok {
		t.Error("expected no published CostDigest before flush interval elapses")
	}

	// Wait past the flush interval, then send another event. With elapsed
	// >= flushInterval, the publish gate opens and the snapshot lands on
	// the AttributeMap with both samples.
	time.Sleep(2 * flushInterval)

	if err := ext.Extract(context.Background(), []dlsrc.Event{ev}); err != nil {
		t.Fatalf("Extract post-interval: %v", err)
	}
	cd, ok := readDigest(ds, "m1")
	if !ok {
		t.Fatal("expected CostDigest after flush interval elapses")
	}
	if cd.Digest.Count() != 2 {
		t.Errorf("digest count = %d, want 2", cd.Digest.Count())
	}
}
