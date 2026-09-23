package main

import (
	"encoding/json"
	"testing"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/test"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/resp"
)

// concurrencyResp 构造 CheckScript 的扁平应答：{allowed, current, exceededIndex}
// 注意：必须用 MarshalRESP() 输出 RESP wire 格式；resp.Value.Bytes() 返回的是显示用字符串。
func concurrencyResp(allowed int64, current int64, exceededIndex int64) []byte {
	b, err := resp.ArrayValue([]resp.Value{
		resp.IntegerValue(int(allowed)),
		resp.IntegerValue(int(current)),
		resp.IntegerValue(int(exceededIndex)),
	}).MarshalRESP()
	if err != nil {
		panic(err)
	}
	return b
}

var globalConcurrencyConfig = func() json.RawMessage {
	data, _ := json.Marshal(map[string]interface{}{
		"rule_name": "cc-global",
		"global_threshold": map[string]interface{}{
			"concurrency": 10,
		},
		"redis": map[string]interface{}{
			"service_name": "redis.static",
			"service_port": 6379,
		},
	})
	return data
}()

var consumerConcurrencyConfig = func() json.RawMessage {
	data, _ := json.Marshal(map[string]interface{}{
		"rule_name": "cc-consumer",
		"rule_items": []map[string]interface{}{
			{
				"limit_by_consumer": "",
				"limit_keys": []map[string]interface{}{
					{"key": "alice", "concurrency": 2},
					// 非 per_ 规则下 ExactType 需要精确匹配 consumer 名，通配须用 "regexp:.*"
					{"key": "regexp:.*", "concurrency": 1},
				},
			},
		},
		"redis": map[string]interface{}{
			"service_name": "redis.static",
			"service_port": 6379,
		},
	})
	return data
}()

func TestOnHttpRequestHeaders_Allowed(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host, status := test.NewTestHost(globalConcurrencyConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		action := host.CallOnHttpRequestHeaders([][2]string{
			{":authority", "example.com"},
			{":path", "/v1/chat"},
			{":method", "POST"},
		})
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		// 发起了 Redis 调用（CheckScript）
		callouts := host.GetRedisCalloutAttributes()
		require.Len(t, callouts, 1)

		// 模拟 Redis：允许（脚本应答 {1, 0, 0}）
		host.CallOnRedisCall(0, concurrencyResp(1, 0, 0))
		host.CompleteHttp() // 触发 OnHttpStreamDone → 应发起 ReleaseScript 调用
	})
}

func TestOnHttpRequestHeaders_Rejected(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host, status := test.NewTestHost(globalConcurrencyConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		action := host.CallOnHttpRequestHeaders([][2]string{
			{":authority", "example.com"},
			{":path", "/v1/chat"},
			{":method", "POST"},
		})
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		// 模拟 Redis：拒绝（脚本应答 {0, 11, 1}——current 11 > 10）
		host.CallOnRedisCall(0, concurrencyResp(0, 11, 1))

		localResp := host.GetLocalResponse()
		require.NotNil(t, localResp)
		require.Equal(t, uint32(429), localResp.StatusCode)
	})
}

func TestOnHttpRequestHeaders_NoRuleMatch_PassesWithoutRedis(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		// consumer 维度配置，但请求无 x-mse-consumer 头 → 不命中，直接放行
		host, status := test.NewTestHost(consumerConcurrencyConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		action := host.CallOnHttpRequestHeaders([][2]string{
			{":authority", "example.com"},
			{":path", "/v1/chat"},
			{":method", "POST"},
		})
		require.Equal(t, types.ActionContinue, action)
		require.Len(t, host.GetRedisCalloutAttributes(), 0)
	})
}

func TestOnHttpRequestHeaders_MultiKeyRollback(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		// global + consumer 双规则同时命中的配置
		cfg := func() json.RawMessage {
			data, _ := json.Marshal(map[string]interface{}{
				"rule_name":        "cc-multi",
				"global_threshold": map[string]interface{}{"concurrency": 100},
				"rule_items": []map[string]interface{}{
					{"limit_by_consumer": "", "limit_keys": []map[string]interface{}{
						// 注意：非 per_ 规则下 "*" 会被解析为 ExactType（不匹配任意值），
						// 通配须用 "regexp:.*"（与 config.go 解析约定一致）
						{"key": "regexp:.*", "concurrency": 5},
					}},
				},
				"redis": map[string]interface{}{"service_name": "redis.static", "service_port": 6379},
			})
			return data
		}()
		host, status := test.NewTestHost(cfg)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		action := host.CallOnHttpRequestHeaders([][2]string{
			{":authority", "example.com"},
			{":path", "/v1/chat"},
			{":method", "POST"},
			{"x-mse-consumer", "alice"},
		})
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		// 2 个 key（global + consumer）合并为一次 EVAL 调用
		callouts := host.GetRedisCalloutAttributes()
		require.Len(t, callouts, 1)
		// 脚本应答：第 2 个 key（consumer）超限 {0, 6, 2}
		host.CallOnRedisCall(0, concurrencyResp(0, 6, 2))
		localResp := host.GetLocalResponse()
		require.NotNil(t, localResp)
		require.Equal(t, uint32(429), localResp.StatusCode)
	})
}

func TestOnHttpStreamDone_ReleasesViaRedisEval(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host, status := test.NewTestHost(globalConcurrencyConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		action := host.CallOnHttpRequestHeaders([][2]string{
			{":authority", "example.com"},
			{":path", "/v1/chat"},
			{":method", "POST"},
		})
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		// 占用成功前：仅 1 次 Redis 调用（CheckScript EVAL）
		require.Len(t, host.GetRedisCalloutAttributes(), 1)

		host.CallOnRedisCall(0, concurrencyResp(1, 0, 0))
		// 注意：已应答的 callout 会从列表移除（见 proxytest CallOnRedisCallout），此时列表为空
		require.Len(t, host.GetRedisCalloutAttributes(), 0)

		host.CompleteHttp() // 触发 OnHttpStreamDone → ReleaseScript EVAL

		// stream 结束后应发起新的 Redis 调用（ReleaseScript EVAL），且内容为 EVAL 脚本
		callouts := host.GetRedisCalloutAttributes()
		require.Len(t, callouts, 1)
		require.Contains(t, string(callouts[0].Query), "decr")
	})
}

func TestOnHttpRequestHeaders_RedisError_FailOpen(t *testing.T) {
	test.RunTest(t, func(t *testing.T) {
		host, status := test.NewTestHost(globalConcurrencyConfig)
		defer host.Reset()
		require.Equal(t, types.OnPluginStartStatusOK, status)

		action := host.CallOnHttpRequestHeaders([][2]string{
			{":authority", "example.com"},
			{":path", "/v1/chat"},
			{":method", "POST"},
		})
		require.Equal(t, types.HeaderStopAllIterationAndWatermark, action)

		// Redis 故障（status != 0）→ fail-open 放行（ResumeHttpRequest，无 local response）
		host.CallOnRedisCall(1, []byte("ERR"))
		require.Nil(t, host.GetLocalResponse())
	})
}
