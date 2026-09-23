// Copyright (c) 2024 Alibaba Group Holding Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"cluster-concurrency-limit/config"
	"cluster-concurrency-limit/util"

	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm/types"
	"github.com/higress-group/wasm-go/pkg/log"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
	"github.com/tidwall/resp"
)

func main() {}

func init() {
	wrapper.SetCtx(
		"cluster-concurrency-limit",
		wrapper.ParseConfig(parseConfig),
		wrapper.ProcessRequestHeaders(onHttpRequestHeaders),
		wrapper.ProcessStreamDone(onHttpStreamDone),
	)
}

const (
	RedisKeyPrefix string = "higress-concurrency-limit"
	// ConcurrencyGlobalKeyFormat 全局限流 key: higress-concurrency-limit:{rule_name}:global_threshold
	ConcurrencyGlobalKeyFormat = RedisKeyPrefix + ":{%s}:global_threshold"
	// ConcurrencyRuleKeyFormat 规则限流 key: higress-concurrency-limit:{rule_name}:{limit_type}:{limit key 名}:{实际值}
	ConcurrencyRuleKeyFormat = RedisKeyPrefix + ":{%s}:%s:%s:%s"

	// CheckScript 请求阶段原子检查+占用脚本（Gauge 语义，全有或全无）
	// KEYS = [key1..keyN]; ARGV = [threshold1, leaseTtl1, ...]
	// 返回扁平数组 {allowed, current, exceededIndex}
	// 任一 key 超限 → 回滚本次全部 INCR，返回 {0, 超限key当前值, 超限key下标}（回滚的拒绝不刷新租约）
	// 全部通过 → 允许，并对所有 key 刷新租约 TTL（每次成功获取都续租，
	// 即"最后一次成功 INCR 即最后一次续租"，持续负载下计数不会因 TTL 过期被误删）
	CheckScript = `
		for i = 1, #KEYS do
			local threshold = tonumber(ARGV[2*i - 1])
			local ttl = tonumber(ARGV[2*i])
			local current = redis.call('incr', KEYS[i])
			if current == 1 then
				redis.call('expire', KEYS[i], ttl)
			end
			if current > threshold then
				for j = 1, i do
					redis.call('decr', KEYS[j])
				end
				return {0, current, i}
			end
		end
		for i = 1, #KEYS do
			redis.call('expire', KEYS[i], ARGV[2*i])
		end
		return {1, 0, 0}
	`
	// ReleaseScript stream 结束时释放脚本
	// 仅当当前值 > 0 时 DECR，防止重复释放导致负数（脚本原子执行，GET+DECR 无竞态）
	ReleaseScript = `
		for i = 1, #KEYS do
			local current = tonumber(redis.call('get', KEYS[i]) or "0")
			if current > 0 then
				redis.call('decr', KEYS[i])
			end
		end
		return #KEYS
	`

	LimitContextKey = "ClusterConcurrencyLimitContext"

	CookieHeader = "cookie"

	ConcurrencyLimitHeader     = "X-Concurrency-Limit"
	ConcurrencyRemainingHeader = "X-Concurrency-Remaining"

	MetricRejected   = "concurrency_limit_rejected"
	MetricRedisError = "concurrency_limit_redis_error"
)

// MatchedRule 请求阶段命中的单条并发规则（global 或 rule_item 命中的 config item）
type MatchedRule struct {
	key       string // 完整 Redis key
	threshold int64  // 并发上限
}

// LimitContext 暂存本请求已成功占用的计数器 key，供 stream 结束时释放
type LimitContext struct {
	rules []MatchedRule
}

func parseConfig(json gjson.Result, cfg *config.Config) error {
	if err := config.InitRedisClusterClient(json, cfg); err != nil {
		return err
	}
	if err := config.ParseClusterConcurrencyLimitConfig(json, cfg); err != nil {
		return err
	}
	// Metric settings
	cfg.CounterMetrics = make(map[string]proxywasm.MetricCounter)
	return nil
}

func onHttpRequestHeaders(ctx wrapper.HttpContext, cfg config.Config) types.Action {
	ctx.DisableReroute()

	matched := collectMatchedRules(ctx, cfg)
	if len(matched) == 0 {
		// 无任何规则命中：直接放行，不发起 Redis 调用
		log.Debugf("cluster-concurrency-limit: no rule matched, path=%s host=%s", ctx.Path(), ctx.Host())
		return types.ActionContinue
	}

	n := len(matched)
	keys := make([]interface{}, n)
	args := make([]interface{}, 0, n*2)
	for i, m := range matched {
		keys[i] = m.key
		args = append(args, m.threshold, cfg.LeaseTTLSeconds)
	}

	err := cfg.RedisClient.Eval(CheckScript, n, keys, args, func(response resp.Value) {
		if response.Error() != nil {
			log.Errorf("cluster-concurrency-limit: redis response error: %v", response.Error())
			cfg.IncrementCounter(metricName(MetricRedisError), 1)
			// fail-open：Redis 故障不拒绝
			_ = proxywasm.ResumeHttpRequest()
			return
		}
		arr := response.Array()
		if len(arr) != 3 {
			log.Errorf("cluster-concurrency-limit: unexpected redis response length: %d", len(arr))
			// 应答异常按 fail-open 处理
			cfg.IncrementCounter(metricName(MetricRedisError), 1)
			_ = proxywasm.ResumeHttpRequest()
			return
		}
		allowed := arr[0].Integer()
		if allowed != 1 {
			current := arr[1].Integer()
			exceededIndex := arr[2].Integer()
			if exceededIndex < 1 || int(exceededIndex) > n {
				log.Errorf("cluster-concurrency-limit: unexpected exceeded index: %d", exceededIndex)
				_ = proxywasm.ResumeHttpRequest()
				return
			}
			log.Debugf("cluster-concurrency-limit: rejected, key=%s current=%d threshold=%d",
				matched[exceededIndex-1].key, current, matched[exceededIndex-1].threshold)
			rejected(ctx, cfg, int64(current), matched[exceededIndex-1].threshold)
			return
		}
		// 占用成功：记录已占用的计数器，供 stream 结束释放
		ctx.SetContext(LimitContextKey, LimitContext{rules: matched})
		_ = proxywasm.ResumeHttpRequest()
	})
	if err != nil {
		log.Errorf("cluster-concurrency-limit: redis call failed: %v", err)
		cfg.IncrementCounter(metricName(MetricRedisError), 1)
		// fail-open：Redis 故障不拒绝
		return types.ActionContinue
	}
	return types.HeaderStopAllIterationAndWatermark
}

// collectMatchedRules 收集全部命中规则（global + rule_items，all-match），返回去重后的计数器列表。
// 顺序：global_threshold（如有）→ rule_items 中所有命中项（按数组顺序追加）。
func collectMatchedRules(ctx wrapper.HttpContext, cfg config.Config) []MatchedRule {
	var matched []MatchedRule
	seen := make(map[string]bool)
	if cfg.GlobalThreshold != nil {
		key := fmt.Sprintf(ConcurrencyGlobalKeyFormat, cfg.RuleName)
		if !seen[key] {
			seen[key] = true
			matched = append(matched, MatchedRule{key: key, threshold: cfg.GlobalThreshold.Count})
		}
	}
	for _, item := range cfg.RuleItems {
		val, _, configItem := hitRateRuleItem(ctx, item)
		if configItem == nil || val == "" {
			continue
		}
		key := fmt.Sprintf(ConcurrencyRuleKeyFormat, cfg.RuleName, item.LimitType, item.Key, val)
		if !seen[key] {
			seen[key] = true
			matched = append(matched, MatchedRule{key: key, threshold: configItem.Concurrency})
		}
	}
	return matched
}

func onHttpStreamDone(ctx wrapper.HttpContext, cfg config.Config) {
	val, ok := ctx.GetContext(LimitContextKey).(LimitContext)
	if !ok || len(val.rules) == 0 {
		return
	}
	n := len(val.rules)
	keys := make([]interface{}, n)
	for i, r := range val.rules {
		keys[i] = r.key
	}
	// 释放是尽力而为：失败只记日志，租约 TTL 兜底
	err := cfg.RedisClient.Eval(ReleaseScript, n, keys, []interface{}{}, func(response resp.Value) {
		if response.Error() != nil {
			log.Errorf("cluster-concurrency-limit: release redis response error: %v", response.Error())
			return
		}
		log.Debugf("cluster-concurrency-limit: released %d counter(s)", n)
	})
	if err != nil {
		log.Errorf("cluster-concurrency-limit: release redis call failed: %v", err)
	}
}

func rejected(ctx wrapper.HttpContext, cfg config.Config, current int64, threshold int64) {
	ctx.SetUserAttribute("concurrency_limit_status", "limited")
	_ = ctx.WriteUserAttributeToLogWithKey(wrapper.AILogKey)
	headers := make(map[string][]string)
	if cfg.ShowQuotaHeader {
		remaining := threshold - current
		if remaining < 0 {
			remaining = 0
		}
		headers[ConcurrencyLimitHeader] = []string{strconv.FormatInt(threshold, 10)}
		headers[ConcurrencyRemainingHeader] = []string{strconv.FormatInt(remaining, 10)}
	}
	_ = proxywasm.SendHttpResponseWithDetail(
		cfg.RejectedCode, "cluster-concurrency-limit.rejected", util.ReconvertHeaders(headers), []byte(cfg.RejectedMsg), -1)

	route, _ := util.GetRouteName()
	cluster, _ := util.GetClusterName()
	consumer, _ := util.GetConsumer()
	cfg.IncrementCounter(fmt.Sprintf("route.%s.upstream.%s.consumer.%s.metric.%s", route, cluster, consumer, MetricRejected), 1)
}

func metricName(name string) string {
	route, _ := util.GetRouteName()
	cluster, _ := util.GetClusterName()
	consumer, _ := util.GetConsumer()
	return fmt.Sprintf("route.%s.upstream.%s.consumer.%s.metric.%s", route, cluster, consumer, name)
}

// hitRateRuleItem 根据限流类型提取请求的维度值，并找到匹配的 limit_keys 配置项。
// 从蓝本 ai-token-ratelimit/main.go 移植（去掉 window 字段，item 只有 Concurrency）。
func hitRateRuleItem(ctx wrapper.HttpContext, rule config.LimitRuleItem) (string, *config.LimitRuleItem, *config.LimitConfigItem) {
	switch rule.LimitType {
	// 根据HTTP请求头限流
	case config.LimitByHeaderType, config.LimitByPerHeaderType:
		val, err := proxywasm.GetHttpRequestHeader(rule.Key)
		if err != nil {
			return logDebugAndReturnEmpty("failed to get request header %s: %v", rule.Key, err)
		}
		return val, &rule, findMatchingItem(rule.LimitType, rule.ConfigItems, val)
	// 根据HTTP请求参数限流
	case config.LimitByParamType, config.LimitByPerParamType:
		parse, err := url.Parse(ctx.Path())
		if err != nil {
			return logDebugAndReturnEmpty("failed to parse request path: %v", err)
		}
		query, err := url.ParseQuery(parse.RawQuery)
		if err != nil {
			return logDebugAndReturnEmpty("failed to parse query params: %v", err)
		}
		val, ok := query[rule.Key]
		if !ok {
			return logDebugAndReturnEmpty("request param %s is empty", rule.Key)
		}
		return val[0], &rule, findMatchingItem(rule.LimitType, rule.ConfigItems, val[0])
	// 根据consumer限流
	case config.LimitByConsumerType, config.LimitByPerConsumerType:
		val, err := proxywasm.GetHttpRequestHeader(util.ConsumerHeader)
		if err != nil {
			return logDebugAndReturnEmpty("failed to get request header %s: %v", util.ConsumerHeader, err)
		}
		return val, &rule, findMatchingItem(rule.LimitType, rule.ConfigItems, val)
	// 根据cookie中key值限流
	case config.LimitByCookieType, config.LimitByPerCookieType:
		cookie, err := proxywasm.GetHttpRequestHeader(CookieHeader)
		if err != nil {
			return logDebugAndReturnEmpty("failed to get request cookie : %v", err)
		}
		val := util.ExtractCookieValueByKey(cookie, rule.Key)
		if val == "" {
			return logDebugAndReturnEmpty("cookie key '%s' extracted from cookie '%s' is empty.", rule.Key, cookie)
		}
		return val, &rule, findMatchingItem(rule.LimitType, rule.ConfigItems, val)
	// 根据客户端IP限流
	case config.LimitByPerIpType:
		realIp, err := getDownStreamIp(rule)
		if err != nil {
			log.Warnf("failed to get down stream ip: %v", err)
			return "", &rule, nil
		}
		for i, item := range rule.ConfigItems {
			if _, found, _ := item.IpNet.Get(realIp); !found {
				continue
			}
			return realIp.String(), &rule, &rule.ConfigItems[i]
		}
	}
	return "", nil, nil
}

func logDebugAndReturnEmpty(errMsg string, args ...interface{}) (string, *config.LimitRuleItem, *config.LimitConfigItem) {
	log.Debugf(errMsg, args...)
	return "", nil, nil
}

// findMatchingItem 按 ConfigType 统一匹配（Exact/Regexp/All/IpNet），
// 不依赖该 ConfigType 由哪种 LimitRuleItemType 产生（非 per 规则同样支持 regexp: 前缀 key）。
func findMatchingItem(limitType config.LimitRuleItemType, items []config.LimitConfigItem, key string) *config.LimitConfigItem {
	for i := range items {
		item := &items[i]
		switch item.ConfigType {
		case config.AllType:
			return item
		case config.RegexpType:
			if item.Regexp != nil && item.Regexp.MatchString(key) {
				return item
			}
		case config.IpNetType:
			if item.IpNet != nil {
				if ip := util.ParseIP(key); ip != "" {
					if realIP := net.ParseIP(ip); realIP != nil {
						if _, found, _ := item.IpNet.Get(realIP); found {
							return item
						}
					}
				}
			}
		default: // config.ExactType
			if item.Key == key {
				return item
			}
		}
	}
	return nil
}

func getDownStreamIp(rule config.LimitRuleItem) (net.IP, error) {
	var (
		realIpStr string
		err       error
	)
	if rule.LimitByPerIp.SourceType == config.HeaderSourceType {
		realIpStr, err = proxywasm.GetHttpRequestHeader(rule.LimitByPerIp.HeaderName)
		if err == nil {
			realIpStr = strings.Split(strings.Trim(realIpStr, " "), ",")[0]
		}
	} else {
		var bs []byte
		bs, err = proxywasm.GetProperty([]string{"source", "address"})
		realIpStr = string(bs)
	}
	if err != nil {
		return nil, err
	}
	ip := util.ParseIP(realIpStr)
	realIP := net.ParseIP(ip)
	if realIP == nil {
		return nil, fmt.Errorf("invalid ip[%s]", ip)
	}
	return realIP, nil
}
