using NewLife.Configuration;
using NewLife.Xml;
using System.ComponentModel;

namespace NewLife.Kafka;

/// <summary>设置</summary>
[Config("Kafka")]
public class KfkSetting : XmlConfig<KfkSetting>
{
    /// <summary>重新开始偏移量</summary>
    [Description("重新开始偏移量 auto.offset.reset（smallest, earliest, beginning, largest, latest, end, error）")]
    public String AutoReset { get; set; } = "earliest";

    /// <summary>一次调度的最大消息数量"consume.callback.max.messages"</summary>
    [Description("consume.callback.max.messages 一次调度的最大消息数量，（0 .. 1000000	）默认（0表示不限制）")]
    public Int32 MaxMessages { get; set; }

    /// <summary></summary>
    [Description("最大消息字节 message.max.bytes")]
    public Int32 MaxMessageBytes { get; set; } = 1_000_000;

    /// <summary></summary>
    [Description("receive.message.max.bytes")]
    public Int32 RecMessageMaxBytes { get; set; } = 100_002_976;

    /// <summary>1 .. 1000000000</summary>
    [Description("fetch.max.bytes 0 .. 2147483135）")]
    public Int32 FetchMaxBytes { get; set; } = 100_000_000;

    /// <summary>1 .. 2097151</summary>
    [Description("queued.max.messages.kbytes 队列最大消息大小（1 .. 2097151）")]
    public Int32 QMaxMessagesKbytes { get; set; } = 1_048_576;

    /// <summary>offset.store.sync.interval.ms 默认-1</summary>
    [Description("偏移异步存储同步间隔 offset.store.sync.interval.ms")]
    public Int32 StoreSyncInterval { get; set; } = -1;

    /// <summary>抓取等待时间</summary>
    [Description("fetch.wait.max.ms 抽取等待最大时间 0 .. 300000（默认100）")]
    public Int32 FetchWaitTime { get; set; }

    /// <summary>经过测试最大长度如果过长会导致抽取速度降低</summary>
    [Description("fetch.message.max.bytes 抽取消息最大字节默认1mb，经过测试最大长度如果过长会导致抽取速度降低，建议降低最大消息长度")]
    public Int32 FetchMessageMaxBytes { get; set; } = 524288;
}
