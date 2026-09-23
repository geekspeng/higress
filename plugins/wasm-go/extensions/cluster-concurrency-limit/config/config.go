package config

import (
	"errors"
	"fmt"
	re "regexp"
	"strings"

	"cluster-concurrency-limit/util"
	"github.com/higress-group/proxy-wasm-go-sdk/proxywasm"
	"github.com/higress-group/wasm-go/pkg/log"
	"github.com/higress-group/wasm-go/pkg/wrapper"
	"github.com/tidwall/gjson"
	"github.com/zmap/go-iptree/iptree"
)

// LimitRuleItemType 限流规则项类型
type LimitRuleItemType string

// LimitConfigItemType 限流配置项key类型
type LimitConfigItemType string

const (
	LimitByHeaderType      LimitRuleItemType = "limit_by_header"
	LimitByParamType       LimitRuleItemType = "limit_by_param"
	LimitByConsumerType    LimitRuleItemType = "limit_by_consumer"
	LimitByCookieType      LimitRuleItemType = "limit_by_cookie"
	LimitByPerHeaderType   LimitRuleItemType = "limit_by_per_header"
	LimitByPerParamType    LimitRuleItemType = "limit_by_per_param"
	LimitByPerConsumerType LimitRuleItemType = "limit_by_per_consumer"
	LimitByPerCookieType   LimitRuleItemType = "limit_by_per_cookie"
	LimitByPerIpType       LimitRuleItemType = "limit_by_per_ip"

	ExactType  LimitConfigItemType = "exact"  // 精确匹配
	RegexpType LimitConfigItemType = "regexp" // 正则表达式
	AllType    LimitConfigItemType = "*"      // 匹配所有情况
	IpNetType  LimitConfigItemType = "ipNet"  // ip段

	RemoteAddrSourceType = "remote-addr"
	HeaderSourceType     = "header"

	DefaultRejectedCode uint32 = 429
	DefaultRejectedMsg  string = "concurrency limit exceeded"

	// DefaultLeaseTTLSeconds 并发计数器租约时长（秒）。仅在网关进程异常退出
	// 导致 OnHttpStreamDone 未执行时，靠 TTL 到期自动清零泄漏的计数。
	// 需大于目标场景的最长请求时长（LLM 流式场景建议大于最大生成时长）。
	DefaultLeaseTTLSeconds int64 = 600

	// MaxRuleItems 限制 rule_items 数组最大长度，与 cluster-key-rate-limit / ai-token-ratelimit 一致。
	MaxRuleItems = 10
)

type Config struct {
	RuleName        string
	GlobalThreshold *GlobalThreshold
	RuleItems       []LimitRuleItem
	RejectedCode    uint32
	RejectedMsg     string
	ShowQuotaHeader bool
	LeaseTTLSeconds int64
	RedisClient     wrapper.RedisClient
	CounterMetrics  map[string]proxywasm.MetricCounter
}

type GlobalThreshold struct {
	Count int64 // 并发上限（Gauge）
}

type LimitRuleItem struct {
	LimitType    LimitRuleItemType // 限流类型
	Key          string            // limit_by_consumer/per_consumer 为 util.ConsumerHeader，其余为对应的 header 名/参数名等
	LimitByPerIp LimitByPerIp
	ConfigItems  []LimitConfigItem
}

type LimitByPerIp struct {
	SourceType string // ip来源类型
	HeaderName string // 根据该请求头获取客户端ip
}

type LimitConfigItem struct {
	ConfigType  LimitConfigItemType
	Key         string
	IpNet       *iptree.IPTree // 仅用于 IpNetType
	Regexp      *re.Regexp     // 仅用于 RegexpType
	Concurrency int64          // 并发上限（Gauge）
}

func (cfg *Config) IncrementCounter(metricName string, inc uint64) {
	if inc == 0 {
		return
	}
	counter, ok := cfg.CounterMetrics[metricName]
	if !ok {
		counter = proxywasm.DefineCounterMetric(metricName)
		cfg.CounterMetrics[metricName] = counter
	}
	counter.Increment(inc)
}

func InitRedisClusterClient(json gjson.Result, config *Config) error {
	redisConfig := json.Get("redis")
	if !redisConfig.Exists() {
		return errors.New("missing redis in config")
	}

	serviceName := redisConfig.Get("service_name").String()
	if serviceName == "" {
		return errors.New("redis service name must not be empty")
	}

	servicePort := int(redisConfig.Get("service_port").Int())
	if servicePort == 0 {
		if strings.HasSuffix(serviceName, ".static") {
			// use default logic port which is 80 for static service
			servicePort = 80
		} else {
			servicePort = 6379
		}
	}

	username := redisConfig.Get("username").String()
	password := redisConfig.Get("password").String()
	timeout := int(redisConfig.Get("timeout").Int())
	if timeout == 0 {
		timeout = 1000
	}

	config.RedisClient = wrapper.NewRedisClusterClient(wrapper.FQDNCluster{
		FQDN: serviceName,
		Port: int64(servicePort),
	})
	database := int(redisConfig.Get("database").Int())
	err := config.RedisClient.Init(username, password, int64(timeout), wrapper.WithDataBase(database))
	if config.RedisClient.Ready() {
		log.Info("redis init successfully")
	} else {
		log.Error("redis init failed, will try later")
	}
	return err
}

func ParseClusterConcurrencyLimitConfig(json gjson.Result, config *Config) error {
	ruleName := json.Get("rule_name")
	if !ruleName.Exists() {
		return errors.New("missing rule_name in config")
	}
	config.RuleName = ruleName.String()

	if err := initLimitRule(json, config); err != nil {
		return err
	}

	rejectedCode := json.Get("rejected_code")
	if rejectedCode.Exists() {
		config.RejectedCode = uint32(rejectedCode.Uint())
	} else {
		config.RejectedCode = DefaultRejectedCode
	}
	rejectedMsg := json.Get("rejected_msg")
	if rejectedMsg.Exists() {
		config.RejectedMsg = rejectedMsg.String()
	} else {
		config.RejectedMsg = DefaultRejectedMsg
	}
	showQuotaHeader := json.Get("show_limit_quota_header")
	config.ShowQuotaHeader = showQuotaHeader.Exists() && showQuotaHeader.Bool()
	leaseTTL := json.Get("lease_ttl_seconds")
	if leaseTTL.Exists() {
		if leaseTTL.Int() <= 0 {
			return errors.New("'lease_ttl_seconds' must be a positive integer")
		}
		config.LeaseTTLSeconds = leaseTTL.Int()
	} else {
		config.LeaseTTLSeconds = DefaultLeaseTTLSeconds
	}
	return nil
}

func initLimitRule(json gjson.Result, config *Config) error {
	globalThresholdResult := json.Get("global_threshold")
	ruleItemsResult := json.Get("rule_items")

	hasGlobal := globalThresholdResult.Exists()
	hasRule := ruleItemsResult.Exists()
	if !hasGlobal && !hasRule {
		return errors.New("at least one of 'global_threshold' or 'rule_items' must be set")
	}

	if hasGlobal {
		threshold, err := parseGlobalThreshold(globalThresholdResult)
		if err != nil {
			return fmt.Errorf("failed to parse global_threshold: %w", err)
		}
		config.GlobalThreshold = threshold
	}

	if !hasRule {
		return nil
	}
	items := ruleItemsResult.Array()
	if len(items) == 0 {
		return errors.New("config rule_items cannot be empty")
	}
	if len(items) > MaxRuleItems {
		return fmt.Errorf("rule_items length %d exceeds maximum %d", len(items), MaxRuleItems)
	}

	var ruleItems []LimitRuleItem
	seenLimitRules := make(map[string]bool)
	for _, item := range items {
		ruleItem, err := parseLimitRuleItem(item)
		if err != nil {
			return fmt.Errorf("failed to parse rule_item in rule_items: %w", err)
		}
		ruleKey := string(ruleItem.LimitType) + ":" + ruleItem.Key
		if seenLimitRules[ruleKey] {
			log.Warnf("duplicate rule found: %s='%s' in rule_items", ruleItem.LimitType, ruleItem.Key)
		} else {
			seenLimitRules[ruleKey] = true
		}
		ruleItems = append(ruleItems, *ruleItem)
	}
	config.RuleItems = ruleItems
	return nil
}

func parseGlobalThreshold(item gjson.Result) (*GlobalThreshold, error) {
	q := item.Get("concurrency")
	if !q.Exists() {
		return nil, errors.New("'concurrency' must be set for global_threshold")
	}
	count := q.Int()
	if count <= 0 {
		return nil, fmt.Errorf("'concurrency' must be a positive integer, got %d", count)
	}
	return &GlobalThreshold{Count: count}, nil
}

func parseLimitRuleItem(item gjson.Result) (*LimitRuleItem, error) {
	var ruleItem LimitRuleItem

	var limitType LimitRuleItemType
	setLimitByKeyIfExists := func(field gjson.Result, limitTypeStr LimitRuleItemType) {
		if field.Exists() && field.String() != "" {
			ruleItem.Key = field.String()
			limitType = limitTypeStr
		}
	}
	setLimitByKeyIfExists(item.Get("limit_by_header"), LimitByHeaderType)
	setLimitByKeyIfExists(item.Get("limit_by_param"), LimitByParamType)
	setLimitByKeyIfExists(item.Get("limit_by_cookie"), LimitByCookieType)
	setLimitByKeyIfExists(item.Get("limit_by_per_header"), LimitByPerHeaderType)
	setLimitByKeyIfExists(item.Get("limit_by_per_param"), LimitByPerParamType)
	setLimitByKeyIfExists(item.Get("limit_by_per_cookie"), LimitByPerCookieType)

	limitByConsumer := item.Get("limit_by_consumer")
	if limitByConsumer.Exists() {
		ruleItem.Key = util.ConsumerHeader
		limitType = LimitByConsumerType
	}
	limitByPerConsumer := item.Get("limit_by_per_consumer")
	if limitByPerConsumer.Exists() {
		ruleItem.Key = util.ConsumerHeader
		limitType = LimitByPerConsumerType
	}

	limitByPerIpResult := item.Get("limit_by_per_ip")
	if limitByPerIpResult.Exists() && limitByPerIpResult.String() != "" {
		limitByPerIp := limitByPerIpResult.String()
		ruleItem.Key = limitByPerIp
		if strings.HasPrefix(limitByPerIp, "from-header-") {
			headerName := limitByPerIp[len("from-header-"):]
			if headerName == "" {
				return nil, errors.New("limit_by_per_ip parse error: empty after 'from-header-'")
			}
			ruleItem.LimitByPerIp = LimitByPerIp{
				SourceType: HeaderSourceType,
				HeaderName: headerName,
			}
		} else if limitByPerIp == "from-remote-addr" {
			ruleItem.LimitByPerIp = LimitByPerIp{
				SourceType: RemoteAddrSourceType,
				HeaderName: "",
			}
		} else {
			return nil, errors.New("the 'limit_by_per_ip' restriction must start with 'from-header-' or be exactly 'from-remote-addr'")
		}
		limitType = LimitByPerIpType
	}

	if limitType == "" {
		return nil, errors.New("at least one of 'limit_by_header', 'limit_by_param', 'limit_by_consumer', 'limit_by_cookie', 'limit_by_per_header', 'limit_by_per_param', 'limit_by_per_consumer', 'limit_by_per_cookie', 'limit_by_per_ip' must be set")
	}
	ruleItem.LimitType = limitType

	if err := initConfigItems(item, &ruleItem); err != nil {
		return nil, err
	}
	return &ruleItem, nil
}

func initConfigItems(json gjson.Result, rule *LimitRuleItem) error {
	limitKeys := json.Get("limit_keys")
	if !limitKeys.Exists() {
		return fmt.Errorf("missing limit_keys in config for %s; add at least one entry", rule.LimitType)
	}
	if len(limitKeys.Array()) == 0 {
		return fmt.Errorf("config limit_keys cannot be empty for %s; add at least one entry", rule.LimitType)
	}
	var configItems []LimitConfigItem
	for _, item := range limitKeys.Array() {
		itemKey := item.Get("key").String()
		if itemKey == "" {
			return fmt.Errorf("limit_keys key is required for %s", rule.LimitType)
		}
		var itemType LimitConfigItemType
		var ipNet *iptree.IPTree
		var regexp *re.Regexp
		if rule.LimitType == LimitByPerIpType {
			parsed, err := util.ParseIPNet(itemKey)
			if err != nil {
				return fmt.Errorf("failed to parse IP Net: %w", err)
			}
			ipNet = parsed
			itemType = IpNetType
		} else if rule.LimitType == LimitByPerHeaderType || rule.LimitType == LimitByPerParamType ||
			rule.LimitType == LimitByPerConsumerType || rule.LimitType == LimitByPerCookieType {
			// per_ 类型的 limit_keys 仅接受 * 或 regexp:...（与蓝本一致）
			if strings.HasPrefix(itemKey, "regexp:") {
				regexpStr := itemKey[len("regexp:"):]
				compiled, err := re.Compile(regexpStr)
				if err != nil {
					return fmt.Errorf("failed to compile regexp %q: %w", regexpStr, err)
				}
				regexp = compiled
				itemType = RegexpType
			} else if itemKey == "*" {
				itemType = AllType
			} else {
				nonPerType := LimitRuleItemType("limit_by_" + strings.TrimPrefix(string(rule.LimitType), "limit_by_per_"))
				return fmt.Errorf("the %q restriction must start with 'regexp:' or be exactly '*' (got %q); "+
					"to match an exact name, use the non-per variant %q instead",
					rule.LimitType, itemKey, nonPerType)
			}
		} else if strings.HasPrefix(itemKey, "regexp:") {
			// 非 per_ 类型也支持 regexp: 前缀（如 limit_by_consumer 下对 consumer 名做正则匹配）
			regexpStr := itemKey[len("regexp:"):]
			compiled, err := re.Compile(regexpStr)
			if err != nil {
				return fmt.Errorf("failed to compile regexp %q: %w", regexpStr, err)
			}
			regexp = compiled
			itemType = RegexpType
		} else {
			itemType = ExactType
		}
		configItem, err := createConfigItem(item, itemType, itemKey, ipNet, regexp)
		if err != nil {
			return err
		}
		configItems = append(configItems, *configItem)
	}
	rule.ConfigItems = configItems
	return nil
}

func createConfigItem(item gjson.Result, itemType LimitConfigItemType, key string, ipNet *iptree.IPTree, regexp *re.Regexp) (*LimitConfigItem, error) {
	concurrency := item.Get("concurrency")
	if !concurrency.Exists() {
		return nil, fmt.Errorf("'concurrency' must be set for key: %s", key)
	}
	count := concurrency.Int()
	if count <= 0 {
		return nil, fmt.Errorf("'concurrency' must be a positive integer, got %d, key: %s", count, key)
	}
	return &LimitConfigItem{
		ConfigType:  itemType,
		Key:         key,
		IpNet:       ipNet,
		Regexp:      regexp,
		Concurrency: count,
	}, nil
}
