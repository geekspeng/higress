---
title: Cluster Concurrency Limit
keywords: [higress, concurrency-limit]
description: Cluster concurrency limit plugin configuration reference
---

## Description

The `cluster-concurrency-limit` plugin implements **cluster-level concurrency limiting** with **Gauge semantics** backed by Redis, for scenarios that require **globally consistent concurrency control** across multiple Higress Gateway instances — typically protecting LLM APIs with streaming (SSE) requests.

### Gauge vs windowed counting

- `cluster-key-rate-limit` uses **windowed counting** (Counter) semantics: it counts the number of requests within a fixed/sliding time window; the window resets automatically and no end-of-request hook is needed.
- This plugin uses **concurrency** (Gauge) semantics: a request **INCR**s the counter when it starts (acquires a slot) and **DECR**s it when the stream ends (releases). The counter represents "requests currently in flight"; requests above the concurrency limit are rejected immediately (429 by default).

For LLM streaming, a single SSE request can last minutes; windowed rate limiting cannot express "at most N requests being generated at the same time". This plugin's window is the request's own lifetime, which matches that need naturally.

### Streaming and non-streaming treated identically

The plugin does not distinguish streaming from non-streaming requests: counters are INCR'd at the request-header phase and DECR'd on `onHttpStreamDone` (end/disconnect of the response stream). A streaming request occupies its concurrency slot for the **entire duration of the stream**; a non-streaming request holds it for its processing time.

### The `cluster-` prefix and hard Redis dependency

- The `cluster-` prefix means counters are stored in Redis and shared across Higress Gateway instances, providing cluster-wide consistency.
- Redis is a **hard dependency**: when Redis is unavailable the plugin **fails open** (the request is allowed and a `concurrency_limit_redis_error` metric is recorded), so the gateway does not reject all traffic — but concurrency limiting is ineffective meanwhile.
- Acquisition at request start happens in a **single atomic Lua EVAL**: all matched counters are INCR'd together; if any exceeds its limit, **all increments are rolled back** (all-or-nothing), so there is no partial-acquisition drift.

### Multi-rule matching semantics (all-match OR)

Consistent with current `cluster-key-rate-limit` behavior: `global_threshold` and `rule_items` can be combined; **all matching** rules are evaluated. Rules are OR-ed: **any triggered rule rejects the request**. Rule order does not affect the result.

### Runtime behavior notes

- **Execution stage: after the auth stage.** This plugin must run **after** auth plugins such as `key-auth` / `jwt-auth` / `basic-auth`; otherwise `limit_by_consumer` / `limit_by_per_consumer` cannot read the `x-mse-consumer` header injected by those plugins, and consumer-dimension rules never match.
- **Redis key format** (`{rule_name}` is a hash tag so Redis Cluster co-locates a rule's keys in one slot):
  - Global: `higress-concurrency-limit:{rule_name}:global_threshold`
  - Rule item: `higress-concurrency-limit:{rule_name}:{limit_type}:{limit key name}:{actual value}`
- **Lease auto-recovery (`lease_ttl_seconds`)**: each counter gets a TTL on first INCR, and every successful acquisition renews the TTL of all counters the request hit (a counter with a steady stream of admitted requests is never evicted by TTL). If a gateway process dies abnormally (e.g. `kill -9`, OOM) so `onHttpStreamDone` never runs, leaked counts are automatically cleared by Redis when the TTL expires — no manual intervention needed. An individual in-flight request has no dedicated renewal, so the TTL **must be greater than the longest request duration** in the target scenario (for LLM streaming, greater than the maximum generation time); otherwise a long request's slot may be reclaimed early.

## Runtime Attributes

Plugin execution stage: `default stage` (UNSPECIFIED_PHASE; in practice runs after the auth plugins)
Plugin execution priority: `610`

> ⚠️ `limit_by_consumer` / `limit_by_per_consumer` rely on the `x-mse-consumer` request header injected by auth plugins (`key-auth` / `jwt-auth` / `basic-auth`, etc.). pilot assembles the filter chain in the order AUTHN → AUTHZ → STATS → default stage, so keeping the default phase guarantees this plugin runs after authentication; if you explicitly set the phase to `AuthN` and end up ahead of the auth plugins, consumer-dimension rules will not see the header and never match.

## Configuration

| Name | Type | Required | Default | Description |
| ------------------------ | ------ | -------- | ------- | ----------- |
| rule_name | string | yes | - | Rule name. The Redis key is built from rule name + limit type + limit key name + the actual key value |
| global_threshold | object | no; at least one of `global_threshold` / `rule_items` must be set; both may be set | - | Concurrency limit for the whole rule group |
| rule_items | array of object | no; same as above | - | Limit rule items, at most **10**. All matching `rule_item`s are evaluated; rules are OR-ed — any triggered rule rejects. Order does not matter. See below. |
| rejected_code | int | no | 429 | HTTP status code returned when a request is rejected |
| rejected_msg | string | no | concurrency limit exceeded | Response body returned when a request is rejected |
| show_limit_quota_header | bool | no | false | Whether to expose `X-Concurrency-Limit` (concurrency limit) and `X-Concurrency-Remaining` (remaining concurrency) response headers. Note: these headers are returned **only on rejection**, and `X-Concurrency-Remaining` is always 0 at that point (unlike cluster-key-rate-limit, which also sends headers on success) |
| lease_ttl_seconds | int | no | 600 | Lease duration (seconds) of a concurrency counter, must be a positive integer. Only a safety net for counts leaked when a gateway process dies before releasing; **must be greater than the longest request duration** in the target scenario (for LLM streaming, greater than the maximum generation time) |
| redis | object | yes | - | Redis configuration |

Fields of `global_threshold`.

| Name | Type | Required | Default | Description |
| ----------- | ---- | -------- | ------- | ----------- |
| concurrency | int | yes | - | Maximum number of in-flight requests, must be a positive integer |

Fields of each `rule_items` entry.

| Name | Type | Required | Default | Description |
| --------------------- | ------ | -------- | ------- | ----------- |
| limit_by_header | string | no, choose one of `limit_by_*` | - | Name of the HTTP request header to read the limit key from |
| limit_by_param | string | no, choose one of `limit_by_*` | - | Name of the URL parameter to read the limit key from |
| limit_by_consumer | string | no, choose one of `limit_by_*` | - | Limit by consumer name; no actual value needed. Depends on the `x-mse-consumer` header injected by auth plugins — this plugin must run after them |
| limit_by_cookie | string | no, choose one of `limit_by_*` | - | Name of the Cookie key to read the limit key from |
| limit_by_per_header | string | no, choose one of `limit_by_*` | - | Limit each header value separately; `limit_keys` **must not contain literal names — only `*` or `regexp:...`**. For exact matching use `limit_by_header` (drop `per_`) |
| limit_by_per_param | string | no, choose one of `limit_by_*` | - | Limit each URL parameter value separately; `limit_keys` **must not contain literal names — only `*` or `regexp:...`**. For exact matching use `limit_by_param` (drop `per_`) |
| limit_by_per_consumer | string | no, choose one of `limit_by_*` | - | Limit each consumer separately; `limit_keys` **must not contain literal names — only `*` or `regexp:...`**. For exact matching use `limit_by_consumer` (drop `per_`) |
| limit_by_per_cookie | string | no, choose one of `limit_by_*` | - | Limit each Cookie value separately; `limit_keys` **must not contain literal names — only `*` or `regexp:...`**. For exact matching use `limit_by_cookie` (drop `per_`) |
| limit_by_per_ip | string | no, choose one of `limit_by_*` | - | Select the client IP source: `from-header-<header name>` or `from-remote-addr`; IPs/CIDRs must be written in `limit_keys[].key` |
| limit_keys | array of object | yes | - | Concurrency limits for matched key values |

Fields of each `limit_keys` entry.

| Name | Type | Required | Default | Description |
| ---- | ------ | -------- | ------- | ----------- |
| key | string | yes | - | Non-`per_` types accept an **exact literal value** or a `regexp:`-prefixed regular expression (matched against the actual value). Note: **a bare `*` does NOT work under a non-per type** (it is parsed as the literal `"*"` and never matches) — use `regexp:.*` as the catch-all; `limit_by_per_header`, `limit_by_per_param`, `limit_by_per_consumer`, `limit_by_per_cookie` accept **only `*` or `regexp:...`**; `limit_by_per_ip` accepts IP addresses or CIDRs |
| concurrency | int | yes | - | Maximum number of in-flight requests, must be a positive integer |

Fields of `redis`.

| Name | Type | Required | Default | Description |
| ------------ | ------ | -------- | ------- | ----------- |
| service_name | string | yes | - | Redis service name as a full FQDN including the service type, e.g. my-redis.dns, redis.my-ns.svc.cluster.local |
| service_port | int | no | 80 for static services, otherwise 6379 | Redis service port |
| username | string | no | - | Redis username |
| password | string | no | - | Redis password |
| timeout | int | no | 1000 | Redis connection timeout in milliseconds |
| database | int | no | 0 | Database id, e.g. 1 for `SELECT 1` |

## Configuration Examples

### Per-consumer concurrency limit

```yaml
rule_name: llm-consumer-concurrency-rule
rule_items:
  - limit_by_consumer: ''
    limit_keys:
      - key: consumer1
        concurrency: 5
      - key: consumer2
        concurrency: 20
  - limit_by_per_consumer: ''
    limit_keys:
      # catch-all: every other consumer gets 3 concurrent requests at most
      - key: "*"
        concurrency: 3
rejected_code: 429
rejected_msg: '{"code":-1,"msg":"concurrency limit exceeded"}'
lease_ttl_seconds: 1800  # for LLM streaming, greater than the maximum generation time
redis:
  service_name: redis.static
show_limit_quota_header: true
```

### Differentiated concurrency by apikey request parameter

```yaml
rule_name: llm-apikey-concurrency-rule
rule_items:
  - limit_by_param: apikey
    limit_keys:
      - key: 9a342114-ba8a-11ec-b1bf-00163e1250b5
        concurrency: 10
      - key: a6a6d7f2-ba8a-11ec-bec2-00163e1250b5
        concurrency: 50
  - limit_by_per_param: apikey
    limit_keys:
      # regexp: all apikeys starting with 'a' get 2 concurrent requests each
      - key: "regexp:^a.*"
        concurrency: 2
      # catch-all: every other apikey gets 1 concurrent request
      # note: this is a per_ type, so "*" is allowed; non-per types must use regexp:.*
      - key: "*"
        concurrency: 1
redis:
  service_name: redis.static
show_limit_quota_header: true
```

### Global fallback + per-rule fine-grained (mixed) configuration

```yaml
rule_name: llm-mixed-concurrency-rule
# global fallback: the whole rule group (this route) allows at most 100 in-flight requests
global_threshold:
  concurrency: 100
rule_items:
  # per consumer
  - limit_by_consumer: ''
    limit_keys:
      - key: consumer1
        concurrency: 10
      - key: consumer2
        concurrency: 10
  # per exact x-api-key header value
  - limit_by_header: x-api-key
    limit_keys:
      - key: 102234
        concurrency: 5
      # regexp catch-all: every other key value gets 2 concurrent requests
      - key: "regexp:.*"
        concurrency: 2
lease_ttl_seconds: 1800
redis:
  service_name: redis.static
show_limit_quota_header: true
```

All matching rules are evaluated together: if the consumer, the x-api-key, or the group-wide total of 100 is exhausted — any single trigger rejects the request.

## Common Mistakes

### Using a bare `*` as catch-all under a non-per type

```yaml
# ❌ Wrong: limit_by_header is a non-per type, so key "*" is parsed as the
#    literal "*" and only matches a header whose value is exactly "*" — never a catch-all
rule_items:
  - limit_by_header: x-api-key
    limit_keys:
      - key: "*"
        concurrency: 2

# ✅ Correct: use regexp:.* as the catch-all for non-per types
rule_items:
  - limit_by_header: x-api-key
    limit_keys:
      - key: "regexp:.*"
        concurrency: 2
```

### Using literal names under a per_ type

```yaml
# ❌ Wrong: per_consumer's limit_keys accept only * or regexp:...;
#    config parsing fails and the error message names the non-per variant to use
rule_items:
  - limit_by_per_consumer: ''
    limit_keys:
      - key: "alice"
        concurrency: 5

# ✅ Correct: drop the per_ prefix for exact matching
rule_items:
  - limit_by_consumer: ''
    limit_keys:
      - key: "alice"
        concurrency: 5
```

### Putting a CIDR into `limit_by_per_ip`

```yaml
# ❌ Wrong: limit_by_per_ip selects the IP source, it does not accept CIDRs
rule_items:
  - limit_by_per_ip: "0.0.0.0/0"
    limit_keys:
      - key: "regexp:.*"
        concurrency: 100

# ✅ Correct: put the CIDR in limit_keys
rule_items:
  - limit_by_per_ip: "from-remote-addr"
    limit_keys:
      - key: "0.0.0.0/0"
        concurrency: 1000
```

### Omitting `limit_keys` or `redis`

```yaml
# ❌ Wrong 1: rule_item without limit_keys — the whole config is rejected
rule_items:
  - limit_by_per_ip: "from-remote-addr"

# ❌ Wrong 2: missing redis — Redis is a hard dependency, the config is rejected
rule_name: demo
global_threshold:
  concurrency: 10

# ✅ Correct: every rule_item provides limit_keys and redis is configured
rule_name: demo
global_threshold:
  concurrency: 10
redis:
  service_name: redis.static
```

### `lease_ttl_seconds` shorter than the longest request

If `lease_ttl_seconds` is shorter than the maximum LLM generation time, a long streaming request's slot is reclaimed by Redis before the stream ends, allowing actual concurrency to exceed the configured limit. Set `lease_ttl_seconds` **greater than the longest request duration** (default 600 seconds).

## Troubleshooting

### Inspect the live concurrency Gauge in Redis

```bash
# list all concurrency counters (current in-flight request counts)
redis-cli KEYS "higress-concurrency-limit:*"
# inspect a rule's global concurrency
redis-cli GET "higress-concurrency-limit:{llm-mixed-concurrency-rule}:global_threshold"
# inspect a rule item keyed by the x-api-key request header; sk-aaa111 is the actual header value
redis-cli GET "higress-concurrency-limit:{my-rule}:limit_by_header:x-api-key:sk-aaa111"
```

> Note: `{rule_name}` in the key is a Redis Cluster hash tag; use `redis-cli -c` in cluster mode. In production iterate with `SCAN` instead of `KEYS` (which blocks the Redis main thread — development use only).

### Plugin metrics

Filter `concurrency_limit` in the gateway `/stats`:

| Metric | Meaning |
| --- | --- |
| `concurrency_limit_rejected` | Number of requests rejected due to concurrency exhaustion (route/cluster/consumer dimensions) |
| `concurrency_limit_redis_error` | Number of Redis call failures; sustained growth indicates Redis connectivity problems |

### Fail-open behavior

When Redis is unavailable the plugin **allows requests** (fail-open) and increments `concurrency_limit_redis_error`; the gateway does not reject all traffic, but concurrency limiting is ineffective meanwhile. If your business depends on the concurrency cap (e.g. cost protection), monitor this metric and ensure Redis availability.

### Gateway log inspection

```bash
kubectl -n higress-system logs <gateway-pod> --tail=2000 \
  | grep -Ei 'cluster-concurrency-limit|missing limit_keys|must start with|regexp'
```

Seeing the config in `config_dump` only proves the control plane has pushed it, not that the data plane loaded the plugin; correlate with `/stats` and gateway logs.

## Streaming Notes

### Slot acquisition and release for SSE streams

For LLM streaming (SSE) requests the plugin acquires the slot at the **request-header phase** and releases it via `onHttpStreamDone` when the **response stream ends or is disconnected** (client disconnect, upstream completion, timeout). A streaming request occupies a concurrency slot for its entire generation time — exactly the semantics LLM concurrency protection needs.

### Crash recovery path (lease_ttl_seconds)

Release depends on the gateway process running `onHttpStreamDone` normally. If a gateway process dies abnormally (`kill -9`, OOM, node failure), acquired slots cannot be released and leak. The `lease_ttl_seconds` lease is the safety net: each counter gets a TTL on first INCR, and leaked counts are automatically cleared by Redis when the TTL expires.

Guidance:

- `lease_ttl_seconds` must be **greater than the longest request duration** in the scenario; for LLM streaming, greater than the maximum generation time (e.g. set 1500+ if generation can run up to 20 minutes).
- A smaller TTL recovers from crashes faster but risks reclaiming slots of long in-flight requests; a larger TTL is safer but slower to recover leaked slots after a crash.
