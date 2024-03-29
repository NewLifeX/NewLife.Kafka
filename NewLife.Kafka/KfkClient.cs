﻿using Confluent.Kafka;
using NewLife.Log;
using NewLife.Serialization;
using NewLife.Threading;

namespace NewLife.Kafka;

/// <summary>消费客户端</summary>
public class KfkClient : DisposeBase
{
    #region 属性
    /// <summary>是否使用中</summary>
    public Boolean Active { get; private set; }

    /// <summary>服务器集群地址</summary>
    public String Servers { get; set; }

    /// <summary>主题</summary>
    public String Topic { get; set; }

    /// <summary>消费者</summary>
    public String GroupID { get; set; }

    /// <summary>批大小。消费后整批处理，默认1000</summary>
    public Int32 BatchSize { get; set; } = 1000;

    /// <summary>消费者</summary>
    public IConsumer<String, String> Consumer { get; set; }

    ///// <summary>每次消费完成后自动提交偏移量</summary>
    //public Boolean AutoCommited { get; set; } = true;

    /// <summary>分区</summary>
    protected IList<Int32> Partitions { get; } = new List<Int32>();

    /// <summary>性能追踪</summary>
    public ITracer Tracer { get; set; }
    #endregion

    #region 构造
    /// <summary>销毁</summary>
    /// <param name="disposing"></param>
    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        Stop();
    }
    #endregion

    #region 开始停止
    /// <summary>确保已创建</summary>
    public virtual void EnsureCreate()
    {
        var csm = Consumer;
        if (csm != null) return;

        if (Topic.IsNullOrEmpty()) throw new Exception($"消费主题不能为空！");

        var setting = LoadSetting();
        DefaultSpan.Current?.AppendTag(setting.ToJson());

        //csm = new Consumer(setting);
        var builder = new ConsumerBuilder<String, String>(setting.ToDictionary(e => e.Key, e => e.Value + ""));
        csm = builder.Build();

        //// 加载错误事件和消费错误事件处理函数
        //csm.OnConsumeError += WriteLog;
        //csm.OnError += WriteLog;
        //csm.OnLog += WriteLog;

        Consumer = csm;
    }

    /// <summary>开始</summary>
    public void Start()
    {
        if (Active) return;

        using var span = Tracer?.NewSpan($"kafka:{Topic}:Start");
        try
        {
            EnsureCreate();

            // 挂载消费主题
            var ps = Partitions;
            if (ps != null && ps.Count > 0)
            {
                XTrace.WriteLine($"挂在分区总数：{ps.Count}，分区索引：{ps.Join(",")}");
                Consumer.Assign(ps.Select(e => new TopicPartition(Topic, e)));
            }
            else
            {
                XTrace.WriteLine($"挂载全局,主题为：{Topic}");
                Consumer.Subscribe(Topic);
            }

            _timer = new TimerX(DoConsume, null, 0, 1000) { Async = true };

            Active = true;
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }

    /// <summary>停止</summary>
    public void Stop()
    {
        if (!Active) return;

        using var span = Tracer?.NewSpan($"kafka:{Topic}:Stop");
        try
        {
            _timer.TryDispose();
            _timer = null;

            Consumer.Unassign();
            Consumer.Unsubscribe();

            XTrace.WriteLine("停止接收消息");

            Active = false;
        }
        catch (Exception ex)
        {
            span?.SetError(ex, null);
            throw;
        }
    }
    #endregion

    #region 消费
    private TimerX _timer;

    private void DoConsume(Object state)
    {
        var list = new List<ConsumeResult<String, String>>();

        // 多次拉取，批量处理
        for (var i = 0; i < BatchSize; i++)
        {
            var result = Consumer.Consume(10);
            if (result != null)
            {
                list.Add(result);
            }
        }

        if (list.Count > 0)
        {
            using var span = Tracer?.NewSpan($"mq:{Topic}:Consume");
            try
            {
                // 批量处理
                OnProcess(list.Select(e => e.Message).ToList());

                // 提交确认
                Consumer.Commit(list.Select(e => e.TopicPartitionOffset));

                // 马上开始下一次
                TimerX.Current.SetNext(-1);
            }
            catch (Exception ex)
            {
                span?.SetError(ex, null);

                throw;
            }
        }
    }

    /// <summary>收到消息事件</summary>
    public event EventHandler<IList<Message<String, String>>> OnMessage;

    /// <summary>处理一批消息</summary>
    /// <param name="messages"></param>
    protected virtual void OnProcess(IList<Message<String, String>> messages) => OnMessage?.Invoke(this, messages);
    #endregion

    #region 辅助
    /// <summary>
    /// 加载设置
    /// </summary>
    /// <returns></returns>
    protected virtual Dictionary<String, Object> LoadSetting()
    {
        var cfg = KfkSetting.Current;

        //// 从配置中心读取集群地址
        //if (Servers.IsNullOrEmpty()) Servers = ConfigClient.Instance.Get("Kafka.Server");

        var dic = new Dictionary<String, Object>
        {
            { "group.id", GroupID },
            { "bootstrap.servers", Servers },
            { "enable.auto.commit", false },
            { "auto.offset.reset",cfg.AutoReset}
        };

        // 设置属性
        if (cfg.MaxMessages >= 0) dic.Add("consume.callback.max.messages", cfg.MaxMessages);
        if (cfg.FetchMaxBytes >= 0) dic.Add("fetch.max.bytes", cfg.FetchMaxBytes);
        if (cfg.MaxMessageBytes >= 0) dic.Add("message.max.bytes", cfg.MaxMessageBytes);
        if (cfg.QMaxMessagesKbytes >= 0) dic.Add("queued.max.messages.kbytes", cfg.QMaxMessagesKbytes);
        if (cfg.StoreSyncInterval >= 0) dic.Add("offset.store.sync.interval.ms", cfg.StoreSyncInterval);
        //if (cfg.AutoCommitInterval >= 0) dic.Add("auto.commit.interval.ms", cfg.AutoCommitInterval);
        if (cfg.FetchWaitTime >= 0) dic.Add("fetch.wait.max.ms", cfg.FetchWaitTime);
        if (cfg.RecMessageMaxBytes >= 0) dic.Add("receive.message.max.bytes", cfg.RecMessageMaxBytes);
        if (cfg.FetchMessageMaxBytes >= 0) dic.Add("fetch.message.max.bytes", cfg.FetchMessageMaxBytes);

        return dic;
    }

    /// <summary>日志</summary>
    public ILog Log { get; set; } = Logger.Null;

    /// <summary>
    /// 写日志
    /// </summary>
    /// <param name="format"></param>
    /// <param name="args"></param>
    public virtual void WriteLog(String format, params Object[] args) => Log?.Info(format, args);
    #endregion
}