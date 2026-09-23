package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func validRedis() string {
	return `"redis": {"service_name": "redis.static", "service_port": 6379}`
}

func TestParseConfig_GlobalThreshold(t *testing.T) {
	json := gjson.Parse(`{"rule_name": "r1", "global_threshold": {"concurrency": 100}, ` + validRedis() + `}`)
	var cfg Config
	err := ParseClusterConcurrencyLimitConfig(json, &cfg)
	require.NoError(t, err)
	assert.Equal(t, "r1", cfg.RuleName)
	require.NotNil(t, cfg.GlobalThreshold)
	assert.EqualValues(t, 100, cfg.GlobalThreshold.Count)
}

func TestParseConfig_RuleItems(t *testing.T) {
	json := gjson.Parse(`{"rule_name": "r1",
		"rule_items": [
			{"limit_by_consumer": "", "limit_keys": [
				{"key": "alice", "concurrency": 50},
				{"key": "regexp:.*", "concurrency": 5}]},
			{"limit_by_header": "X-API-Key", "limit_keys": [
				{"key": "sk-aaa", "concurrency": 10}]}
		], ` + validRedis() + `}`)
	var cfg Config
	err := ParseClusterConcurrencyLimitConfig(json, &cfg)
	require.NoError(t, err)
	require.Len(t, cfg.RuleItems, 2)
	assert.Equal(t, LimitByConsumerType, cfg.RuleItems[0].LimitType)
	// consumer 维度的 Key 固定为 util.ConsumerHeader
	assert.Equal(t, "x-mse-consumer", cfg.RuleItems[0].Key)
	require.Len(t, cfg.RuleItems[0].ConfigItems, 2)
	assert.Equal(t, RegexpType, cfg.RuleItems[0].ConfigItems[1].ConfigType)
	assert.EqualValues(t, 50, cfg.RuleItems[0].ConfigItems[0].Concurrency)
	assert.Equal(t, LimitByHeaderType, cfg.RuleItems[1].LimitType)
	assert.Equal(t, "X-API-Key", cfg.RuleItems[1].Key)
}

func TestParseConfig_RejectsMissingConcurrency(t *testing.T) {
	json := gjson.Parse(`{"rule_name": "r1",
		"rule_items": [{"limit_by_header": "X-API-Key", "limit_keys": [{"key": "k", "query_per_second": 10}]}],
		` + validRedis() + `}`)
	var cfg Config
	err := ParseClusterConcurrencyLimitConfig(json, &cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "concurrency")
}

func TestParseConfig_RejectsNonPositiveConcurrency(t *testing.T) {
	json := gjson.Parse(`{"rule_name": "r1",
		"rule_items": [{"limit_by_consumer": "", "limit_keys": [{"key": "a", "concurrency": 0}]}],
		` + validRedis() + `}`)
	var cfg Config
	err := ParseClusterConcurrencyLimitConfig(json, &cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "positive")
}

func TestParseConfig_PerConsumerRejectsLiteralKey(t *testing.T) {
	json := gjson.Parse(`{"rule_name": "r1",
		"rule_items": [{"limit_by_per_consumer": "", "limit_keys": [{"key": "alice", "concurrency": 5}]}],
		` + validRedis() + `}`)
	var cfg Config
	err := ParseClusterConcurrencyLimitConfig(json, &cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "regexp")
}

func TestParseConfig_RuleItemsLimit(t *testing.T) {
	items := ""
	for i := 0; i < 11; i++ {
		if i > 0 {
			items += ","
		}
		items += `{"limit_by_consumer": "", "limit_keys": [{"key": "a", "concurrency": 1}]}`
	}
	json := gjson.Parse(`{"rule_name": "r1", "rule_items": [` + items + `], ` + validRedis() + `}`)
	var cfg Config
	err := ParseClusterConcurrencyLimitConfig(json, &cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum")
}

func TestParseConfig_RequiresGlobalOrRuleItems(t *testing.T) {
	json := gjson.Parse(`{"rule_name": "r1", ` + validRedis() + `}`)
	var cfg Config
	err := ParseClusterConcurrencyLimitConfig(json, &cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one")
}

func TestParseConfig_Defaults(t *testing.T) {
	json := gjson.Parse(`{"rule_name": "r1", "global_threshold": {"concurrency": 10}, ` + validRedis() + `}`)
	var cfg Config
	err := ParseClusterConcurrencyLimitConfig(json, &cfg)
	require.NoError(t, err)
	assert.EqualValues(t, DefaultRejectedCode, cfg.RejectedCode)
	assert.Equal(t, DefaultRejectedMsg, cfg.RejectedMsg)
	assert.EqualValues(t, DefaultLeaseTTLSeconds, cfg.LeaseTTLSeconds)
	assert.False(t, cfg.ShowQuotaHeader)

	json2 := gjson.Parse(`{"rule_name": "r1", "global_threshold": {"concurrency": 10},
		"rejected_code": 503, "rejected_msg": "busy", "show_limit_quota_header": true,
		"lease_ttl_seconds": 1200, ` + validRedis() + `}`)
	var cfg2 Config
	err = ParseClusterConcurrencyLimitConfig(json2, &cfg2)
	require.NoError(t, err)
	assert.EqualValues(t, 503, cfg2.RejectedCode)
	assert.Equal(t, "busy", cfg2.RejectedMsg)
	assert.True(t, cfg2.ShowQuotaHeader)
	assert.EqualValues(t, 1200, cfg2.LeaseTTLSeconds)
}
