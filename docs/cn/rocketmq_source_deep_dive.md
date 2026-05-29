# Apache RocketMQ 源码深度剖析

> 基于 RocketMQ **5.5.0** 源码，从架构、流程、底层实现到各特性原理的系统梳理。  
> 分析日期：2026-05-29

---

## 目录

1. [整体架构](#一整体架构)
2. [核心工作流程](#二核心工作流程)
3. [NameServer 底层原理](#三nameserver-底层原理)
4. [Broker 底层原理](#四broker-底层原理)
5. [Store 存储引擎](#五store-存储引擎)
6. [Client 客户端原理](#六client-客户端原理)
7. [Remoting 通信层](#七remoting-通信层)
8. [各特性实现原理](#八各特性实现原理)
9. [5.x 新架构演进](#九5x-新架构演进)
10. [技术亮点与设计亮点](#十技术亮点与设计亮点)
11. [源码阅读导航](#十一源码阅读导航)
12. [Broker 启动恢复机制](#十二broker-启动恢复机制)
13. [getMessage 读取路径详解](#十三getmessage-读取路径详解)
14. [文件清理与磁盘管理](#十四文件清理与磁盘管理)
15. [Client 消费流控与线程模型](#十五client-消费流控与线程模型)
16. [PullAPIWrapper 与读写分离](#十六pullapiwrapper-与读写分离)
17. [Pop Buffer 合并与 ACK 机制](#十七pop-buffer-合并与-ack-机制)
18. [消息压缩与批量发送](#十八消息压缩与批量发送)
19. [认证鉴权体系](#十九认证鉴权体系)
20. [Hook 与 Pipeline 扩展机制](#二十hook-与-pipeline-扩展机制)
21. [Netty Pipeline 与连接管理](#二十一netty-pipeline-与连接管理)
22. [BrokerOuterAPI 与外部交互](#二十二brokerouterapi-与外部交互)
23. [Compaction Topic 与 KV 存储](#二十三compaction-topic-与-kv-存储)
24. [典型故障场景分析](#二十四典型故障场景分析)
25. [刷盘引擎与 TransientStorePool](#二十五刷盘引擎与-transientstorepool)
26. [MappedFile 预分配与文件生命周期](#二十六mappedfile-预分配与文件生命周期)
27. [Proxy gRPC 全链路剖析](#二十七proxy-grpc-全链路剖析)
28. [Controller 选主与 ReplicasManager](#二十八controller-选主与-replicasmanager)
29. [冷数据流控 / Reply / Recall](#二十九冷数据流控--reply--recall)
30. [消息轨迹 Trace](#三十消息轨迹-trace)
31. [ResponseCode 与异常语义](#三十一responsecode-与异常语义)
32. [可观测性 OpenTelemetry Metrics](#三十二可观测性-opentelemetry-metrics)
33. [单元化部署 Unit Mode](#三十三单元化部署unit-mode)
34. [Lite Topic 与通知机制](#三十四lite-topic-与通知机制)
35. [DLedger 多副本 CommitLog](#三十五dledger-多副本-commitlog)
36. [Proxy ServiceManager 服务分层](#三十六proxy-servicemanager-服务分层)
37. [Proxy Remoting 协议层](#三十七proxy-remoting-协议层)
38. [JRaft Controller 状态机](#三十八jraft-controller-状态机)
39. [RocksDB 存储后端](#三十九rocksdb-存储后端)
40. [BrokerContainer 多 Broker 容器](#四十brokercontainer-多-broker-容器)
41. [Static Topic 深度剖析](#四十一static-topic-深度剖析)
42. [RunningFlags 与读写状态控制](#四十二runningflags-与读写状态控制)
43. [HAConnection 传输协议](#四十三haconnection-传输协议)
44. [Namespace 多租户隔离](#四十四namespace-多租户隔离)
45. [Pop 顺序消费](#四十五pop-顺序消费)
46. [Broker Config v2 与 OpenMessaging](#四十六broker-config-v2-与-openmessaging)
47. [msgId 生成与构建部署](#四十七msgid-生成与构建部署)
48. [TieredStore 冷热分层深度](#四十八tieredstore-冷热分层深度)
49. [gRPC Telemetry 双向流](#四十九grpc-telemetry-双向流)
50. [ConsumeQueueExt 与过滤位图](#五十consumequeueext-与过滤位图)
51. [QueueOffset 分配机制](#五十一queueoffset-分配机制)
52. [MessageStore 插件链](#五十二messagestore-插件链)
53. [Producer 异步背压](#五十三producer-异步背压)
54. [Peek 消息与 Pop 消费者锁](#五十四peek-消息与-pop-消费者锁)
55. [Broker 运行时统计体系](#五十五broker-运行时统计体系)
56. [全局顺序消息 Order Topic](#五十六全局顺序消息-order-topic)

---

## 一、整体架构

### 1.1 模块分层

RocketMQ 采用**经典分层 + 插件化扩展**的 Maven 多模块架构（可选 Bazel 构建）：

| 模块 | Artifact | 职责 |
|------|----------|------|
| `common` | rocketmq-common | 消息模型、配置、常量、工具类 |
| `remoting` | rocketmq-remoting | 基于 Netty 的自定义 RPC 协议 |
| `store` | rocketmq-store | 消息持久化引擎（CommitLog / CQ / Index） |
| `broker` | rocketmq-broker | 消息路由、存储编排、消费管理 |
| `namesrv` | rocketmq-namesrv | 无状态路由注册中心 |
| `client` | rocketmq-client | Producer / Consumer SDK |
| `filter` | rocketmq-filter | SQL92 表达式过滤引擎 |
| `controller` | rocketmq-controller | 基于 Raft/DLedger 的 HA 自动选主 |
| `proxy` | rocketmq-proxy | gRPC 网关，计算存储分离 |
| `tieredstore` | rocketmq-tieredstore | 冷热分层存储 |
| `auth` | rocketmq-auth | 认证鉴权 |
| `tools` | rocketmq-tools | mqadmin CLI |
| `distribution` | rocketmq-distribution | 打包与启动脚本 |

**依赖链（简化）**：

```
common → remoting → {client, namesrv, broker}
                         broker → store + filter + auth + tieredstore
                         proxy → broker (Local/Cluster 模式)
                         controller → remoting
```

### 1.2 运行时角色与入口

| 组件 | Main Class | 默认端口 | 脚本 |
|------|-----------|----------|------|
| NameServer | `NamesrvStartup` | 9876 | `mqnamesrv` |
| Broker | `BrokerStartup` | 10911 | `mqbroker` |
| Proxy | `ProxyStartup` | gRPC + Remoting | `mqproxy` |
| Controller | `ControllerStartup` | 可配置 | `mqcontroller` |
| Admin | `MQAdminStartup` | — | `mqadmin` |

客户端无独立进程，通过 SDK 接入：

| 角色 | 公开 API | 内部实现 |
|------|---------|---------|
| Producer | `DefaultMQProducer` | `DefaultMQProducerImpl` |
| Push Consumer | `DefaultMQPushConsumer` | `DefaultMQPushConsumerImpl` |
| Lite Pull | `DefaultLitePullConsumer` | `DefaultLitePullConsumerImpl` |
| Transaction | `TransactionMQProducer` | `DefaultMQProducerImpl` |

### 1.3 架构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client Layer                              │
│   DefaultMQProducer    DefaultMQPushConsumer    LitePullConsumer │
│              └────────── MQClientInstance ──────────┘            │
└────────────────────────────┬────────────────────────────────────┘
                             │ Remoting (Netty)
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
   │  NameServer │   │    Proxy    │   │   Broker    │
   │  (无状态)    │   │  (可选网关)  │   │   (有状态)   │
   └─────────────┘   └──────┬──────┘   └──────┬──────┘
                              │                  │
                              └────────┬─────────┘
                                       ▼
                              ┌─────────────────┐
                              │ DefaultMessageStore│
                              │ CommitLog/CQ/Index │
                              └─────────────────┘
                                       ▲
                              ┌────────┴────────┐
                              │   Controller    │
                              │  (Raft 选主)     │
                              └─────────────────┘
```

**核心设计哲学**：

- **NameServer 无状态**：节点间零通信，Broker 向所有 NS 注册，客户端任选一个查询
- **Broker 有状态**：承担存储、投递、消费进度、过滤、事务等全部有状态逻辑
- **Client 无状态**：Producer/Consumer 可水平扩展，路由本地缓存 + 定时刷新
- **5.x 方向**：Proxy 计算存储分离、Pop 无状态消费、gRPC 多语言、Controller 自动选主

---

## 二、核心工作流程

### 2.1 集群启动与注册

```
Phase 1: NameServer 启动
  NamesrvStartup.main()
    → parseCommandlineAndConfigFile()  // 默认端口 9876
    → NamesrvController.initialize()
        → RouteInfoManager
        → NettyRemotingServer
        → registerProcessor(ClientRequestProcessor, DefaultRequestProcessor)
        → 定时 scanNotActiveBroker (默认 5s)
    → NamesrvController.start()

Phase 2: Broker 启动
  BrokerStartup.main()
    → BrokerController.initialize()
        → initializeMetadata()       // Topic/Subscription/Consumer 元数据
        → initializeMessageStore()   // DefaultMessageStore / DLedger / TieredStore
        → recoverAndInitService()
            → messageStore.load()    // 恢复 CommitLog / ConsumeQueue
            → registerProcessor()    // 注册全部 RequestProcessor
            → initialTransaction()
    → registerBrokerAll()            // REGISTER_BROKER (103) 向所有 NS 注册
    → 定时心跳 10~60s + BROKER_HEARTBEAT (904)

Phase 3: Client 启动
  MQClientManager.getOrCreateMQClientInstance(clientId)
    → MQClientInstance.start()
        → mQClientAPIImpl.start()
        → startScheduledTask()       // 路由刷新 30s、心跳、offset 持久化
        → pullMessageService.start()
        → rebalanceService.start()
```

### 2.2 消息发送全流程

```
Producer.send(msg)
  │
  ├─ DefaultMQProducerImpl.sendDefaultImpl()
  │    ├─ tryToFindTopicPublishInfo(topic)
  │    │    └─ MQClientInstance.updateTopicRouteInfoFromNameServer()
  │    │         └─ GET_ROUTEINFO_BY_TOPIC (105) → NameServer
  │    ├─ MQFaultStrategy.selectOneMessageQueue()  // 队列选择 + 延迟隔离
  │    └─ sendKernelImpl()  // 同步/异步/单向
  │         └─ MQClientAPIImpl.sendMessage()
  │              └─ SEND_MESSAGE (310) / SEND_MESSAGE_V2 (311) → Broker
  │
  └─ Broker: SendMessageProcessor.sendMessage()
       ├─ 权限校验、Static Topic 映射、Hook 链
       ├─ 特殊消息分流（延迟/事务/定时）
       └─ messageStore.asyncPutMessage()
            └─ CommitLog → 刷盘 → HA 同步 → 返回 PutMessageResult
                 └─ ReputMessageService 异步构建 CQ/Index（与写入解耦）
```

### 2.3 消息消费全流程（Push 模式）

> **关键认知**：Push Consumer 本质是 **Client 主动 Pull + Broker 长轮询**，并非 Broker 主动推送。

```
RebalanceService (每 20s 或 rebalanceImmediately)
  └─ RebalanceImpl.doRebalance()
       ├─ findConsumerIdList() → GET_CONSUMER_LIST_BY_GROUP
       ├─ AllocateMessageQueueStrategy.allocate()  // 队列分配
       └─ updateProcessQueueTableInRebalance()
            └─ dispatchPullRequest() → PullMessageService

PullMessageService (单线程调度)
  └─ DefaultMQPushConsumerImpl.pullMessage()
       └─ pullAPIWrapper.pullKernelImpl() [ASYNC + 长轮询]
            └─ PULL_MESSAGE (11) → Broker

Broker: PullMessageProcessor
  ├─ messageStore.getMessage()  // CQ 索引 → CommitLog 读消息
  ├─ MessageFilter 过滤 (Tag/SQL92)
  ├─ 有消息 → 立即返回 FOUND
  └─ 无消息 → PullRequestHoldService.suspendPullRequest() 挂起 5~30s
       └─ ReputMessageService.notifyMessageArrive() 唤醒

Client 收到 FOUND:
  └─ ConsumeMessageConcurrentlyService.submitConsumeRequest()
       ├─ MessageListener 业务回调
       ├─ CONSUME_SUCCESS → updateOffset
       └─ RECONSUME_LATER → sendMessageBack() → %RETRY%Group%
```

---

## 三、NameServer 底层原理

### 3.1 核心类职责

| 类 | 文件 | 职责 |
|----|------|------|
| `NamesrvStartup` | namesrv/NamesrvStartup.java | CLI 解析、Controller 启动入口 |
| `NamesrvController` | namesrv/NamesrvController.java | 生命周期、Netty Server/Client、线程池 |
| `RouteInfoManager` | namesrv/routeinfo/RouteInfoManager.java | 内存路由表，读写锁保护 |
| `DefaultRequestProcessor` | namesrv/processor/DefaultRequestProcessor.java | Broker/Admin 请求 |
| `ClientRequestProcessor` | namesrv/processor/ClientRequestProcessor.java | 仅处理路由查询 (105) |
| `BatchUnregistrationService` | namesrv/routeinfo/BatchUnregistrationService.java | 批量异步注销 |
| `BrokerHousekeepingService` | namesrv/BrokerHousekeepingService.java | Channel 生命周期监听 |

### 3.2 路由数据结构

所有路由数据**仅存内存、不持久化**，Broker 重启后通过注册恢复。

| 表 | 类型 | 用途 |
|----|------|------|
| `topicQueueTable` | `Map<topic, Map<brokerName, QueueData>>` | Topic 在各 Broker 的队列元数据 |
| `brokerAddrTable` | `Map<brokerName, BrokerData>` | 集群/Zone/brokerId→Address |
| `clusterAddrTable` | `Map<cluster, Set<brokerName>>` | 集群成员 |
| `brokerLiveTable` | `Map<BrokerAddrInfo, BrokerLiveInfo>` | 存活 Broker + Channel + 心跳时间戳 |
| `filterServerTable` | Filter Server 地址 | SQL92 过滤服务 |
| `topicQueueMappingInfoTable` | Static Topic 映射 | 逻辑队列 → 物理队列 |

### 3.3 Broker 注册流程（REGISTER_BROKER = 103）

```
BrokerController.registerBrokerAll()  [定时 10~60s + 启动时立即执行]
  → BrokerOuterAPI.registerBrokerAll()  [并行 fan-out 到所有 NS]
  → DefaultRequestProcessor.registerBroker()
       ├─ CRC32 校验 RegisterBrokerBody
       ├─ RouteInfoManager.registerBroker()
       │    ├─ 更新 clusterAddrTable / brokerAddrTable
       │    ├─ Master 或 Prime Slave 发布 TopicConfig → topicQueueTable
       │    ├─ 更新 brokerLiveTable (Channel + heartbeatTimeout + DataVersion)
       │    └─ 处理 brokerId 冲突（Slave 升 Master 时清理旧地址）
       └─ 返回 masterAddr, haServerAddr, orderTopic KV

增量优化:
  needRegister() → QUERY_DATA_VERSION (322)
  若 NS 已有相同 DataVersion，跳过全量 REGISTER_BROKER
```

### 3.4 心跳与 Broker 过期

| 机制 | 触发 | 作用 |
|------|------|------|
| REGISTER_BROKER | 10~60s | 顺带刷新 brokerLiveTable |
| BROKER_HEARTBEAT (904) | brokerHeartbeatInterval | Acting Master / Controller 模式 |
| QUERY_DATA_VERSION (322) | 兼容旧 NS | 检查配置变更 + 刷新时间戳 |
| scanNotActiveBroker | 每 5s | 超时（默认 2min）关闭 Channel → 异步注销 |
| BrokerHousekeepingService | Channel Close/Idle/Exception | onChannelDestroy → BatchUnregistrationService |

### 3.5 客户端路由发现

```
MQClientInstance.updateTopicRouteInfoFromNameServer(topic)
  → MQClientAPIImpl.getTopicRouteInfoFromNameServer()
  → GET_ROUTEINFO_BY_TOPIC (105)  [invokeSync(null, ...) 随机选 NS]
  → ClientRequestProcessor.getRouteInfoByTopic()
       └─ RouteInfoManager.pickupTopicRouteData(topic)
            ├─ 收集 QueueData + BrokerData
            ├─ Acting Master 提升（min brokerId Slave 冒充 Master）
            └─ 附加 filterServer / staticTopicMapping

Client 端:
  topicRouteDataChanged() → 更新 topicRouteTable / brokerAddrTable
  → 通知 Producer (TopicPublishInfo) / Consumer (subscribe set)
  → cleanOfflineBroker() 清理下线 Broker
```

**Acting Master**：`supportActingMaster=true` 时，Master 宕机后 min brokerId 的 Slave 在路由响应中"冒充" Master，支持只读消费。

**Zone 路由**：`ZoneRouteRPCHook` 在 GET_ROUTEINFO 响应后过滤，优先返回同 Zone 的 Broker。

### 3.6 线程池隔离设计

```java
// NamesrvController.registerProcessor()
clientRequestExecutor  → 仅 GET_ROUTEINFO_BY_TOPIC (105)  // 客户端路由查询
defaultExecutor        → 其余所有请求                        // Broker 注册/Admin
```

生产环境 Broker 注册流量不会阻塞 Client 路由查询。

---

## 四、Broker 底层原理

### 4.1 BrokerController 初始化三阶段

```java
// broker/BrokerController.java
initialize() {
    initializeMetadata();      // TopicConfigManager, SubscriptionGroupManager, ConsumerManager...
    initializeMessageStore();  // DefaultMessageStore / DLedgerCommitLog / TieredMessageStore 插件
    recoverAndInitService();   // load → Remoting → Processor → Transaction → ScheduledTasks
}
```

**recoverAndInitService 关键步骤**：

1. Controller 模式：`ReplicasManager` 初始化，`setFenced(true)` 等待选主
2. `messageStore.load()` 恢复 CommitLog / ConsumeQueue
3. `timerMessageStore.load()` / `scheduleMessageService.load()`
4. `initializeRemotingServer()` — 普通 + Fast 双 Remoting Server
5. `registerProcessor()` — 按业务隔离线程池
6. `initialTransaction()` — 事务消息服务
7. `initializeScheduledTasks()` — 注册 NS、统计、清理等定时任务

### 4.2 Processor 注册与线程池隔离

| RequestCode | Processor | 线程池 | 说明 |
|-------------|-----------|--------|------|
| SEND_MESSAGE / V2 / BATCH | SendMessageProcessor | sendMessageExecutor | 消息发送 |
| CONSUMER_SEND_MSG_BACK | SendMessageProcessor | sendMessageExecutor | 消费重试 |
| PULL_MESSAGE / LITE_PULL | PullMessageProcessor | pullMessageExecutor | 经典 Pull |
| POP_MESSAGE / POP_LITE | PopMessageProcessor | pullMessageExecutor | Pop 消费 |
| ACK_MESSAGE / BATCH_ACK | AckMessageProcessor | ackMessageExecutor | Pop ACK |
| CHANGE_MESSAGE_INVISIBLETIME | ChangeInvisibleTimeProcessor | ackMessageExecutor | Pop 续期 |
| END_TRANSACTION | EndTransactionProcessor | — | 事务提交/回滚 |
| GET_CONSUMER_LIST_BY_GROUP | AdminBrokerProcessor | — | Rebalance 元数据 |

**Fast Remoting Server**：独立端口处理 SEND/ACK 等高频低延迟请求，与普通 Admin 请求隔离。

### 4.3 SendMessageProcessor 发送路径

```java
processRequest(ctx, request) {
    switch (request.getCode()) {
        case CONSUMER_SEND_MSG_BACK:  // 消费失败重试
            return consumerSendMsgBack(ctx, request);
        default:
            SendMessageRequestHeader header = parseRequestHeader(request);
            // Static Topic: 逻辑队列 → 物理队列映射
            TopicQueueMappingContext ctx = topicQueueMappingManager.buildContext(header);
            rewriteRequestForStaticTopic(header, ctx);

            if (header.isBatch())
                return sendBatchMessage(...);
            else
                return sendMessage(...);  // 核心路径
    }
}
```

**sendMessage 内部**：

1. Topic 权限校验（写权限、Slave 拒绝写入，`rejectRequest()` 流控）
2. 构建 `MessageExtBrokerInner`（storeHost、uniqId、sysFlag、压缩）
3. **特殊消息分流**：
   - 延迟消息 → `ScheduleMessageService` 改写 Topic 为 `SCHEDULE_TOPIC_XXXX`
   - 事务消息 → `TransactionalMessageService.asyncPrepareMessage()` → Half Topic
   - 定时消息 → `TimerMessageStore` 时间轮
4. `messageStore.asyncPutMessage()` → CommitLog
5. 同步刷盘时等待 HA 确认
6. Hook 链：`SendMessageHook` before/after

**流控 rejectRequest()**：

```java
// PageCache 繁忙 或 TransientStorePool 不足时快速拒绝
if (messageStore.isOSPageCacheBusy() || messageStore.isTransientStorePoolDeficient())
    return true;
// 普通 Slave 拒绝写入（Acting Master 除外）
if (!enableSlaveActingMaster && brokerRole == SLAVE)
    return true;
```

### 4.4 PullMessageProcessor 拉取路径

```
1. 解析 PullMessageRequestHeader
2. Static Topic: globalOffset → physicalOffset 转换
3. 构建 MessageFilter:
   - Tag: ExpressionMessageFilter.isMatchedByConsumeQueue(tagsCode)
   - SQL92: BloomFilter 预过滤 + isMatchedByCommitLog() 精确匹配
4. messageStore.getMessage(offset, maxMsgNums, filter):
   a. ConsumeQueue.getIndexBuffer(offset) → 读取 20 字节索引
   b. 根据 commitLogOffset 从 CommitLog 读消息体
   c. Filter 过滤
5. 结果处理:
   - FOUND → 返回消息 + suggestWhichBrokerId (Master/Slave 建议)
   - NO_MATCHED_MESSAGE / OFFSET_FOUND_NULL → 尝试长轮询
   - OFFSET_ILLEGAL → 返回错误，Client rebalance
6. 长轮询: PullRequestHoldService.suspendPullRequest(topic, queueId, pullRequest)
```

**读 Master/Slave 建议**：Broker 根据 Consumer offset 与 maxOffset 的距离（是否读老消息）、Slave 是否可读，在响应 Header 中建议下次从 Master 还是 Slave 拉取。

### 4.5 长轮询机制（PullRequestHoldService）

```java
// 按 topic@queueId 维度挂起 Pull 请求
ConcurrentMap<String, ManyPullRequest> pullRequestTable;

suspendPullRequest(topic, queueId, pullRequest) {
    pullRequest.getRequestCommand().setSuspended(true);
    pullRequestTable.get(topic@queueId).add(pullRequest);
}

// ReputMessageService 分发新消息时
notifyMessageArrive(topic, queueId, ...) {
    ManyPullRequest mpr = pullRequestTable.remove(key);
    mpr.executePullRequestImmediately();  // 唤醒挂起的 Pull 请求
}
```

- 长轮询默认挂起 **5s**（可配 30s）
- 短轮询 fallback：**15ms** 间隔轮询
- Pop 模式有独立的 `PopLongPollingService`

### 4.6 消费进度管理（ConsumerOffsetManager）

```java
// broker/offset/ConsumerOffsetManager.java
ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer/* queueId */, Long>> offsetTable;
```

| 模式 | 存储位置 | Client 类 |
|------|---------|-----------|
| 集群 | Broker 远程 | `RemoteBrokerOffsetStore` |
| 广播 | Client 本地文件 | `LocalFileOffsetStore` |

- Client 消费成功后 `UPDATE_CONSUMER_OFFSET` 上报
- Broker 定时持久化 offset 到 JSON 配置文件
- Rebalance 新增队列时 `computePullFromWhere()` 决定起始 offset

---

## 五、Store 存储引擎

### 5.1 混合存储架构

RocketMQ 最核心的设计决策：**所有 Topic 共享一个 CommitLog，ConsumeQueue 作为二级索引**。

```
                    ┌──────────────────────────────────────┐
                    │            CommitLog                  │
                    │  所有 Topic 消息顺序追加写入           │
                    │  默认 1GB/文件, Mmap 映射             │
                    │  路径: $STORE/commitlog/              │
                    └──────────────────┬───────────────────┘
                                       │
                          ReputMessageService (异步分发)
                                       │
              ┌────────────────────────┼────────────────────────┐
              ▼                        ▼                        ▼
       ConsumeQueue              IndexFile              Timer/Trans Index
       (20B/条定长索引)          (Hash 索引)            (特殊消息索引)
       topic/queueId/file        index/fileName         内部 Topic
```

**设计优势**：

- CommitLog **顺序写**，充分利用磁盘顺序 IO + OS PageCache
- ConsumeQueue **定长 20 字节**，可像数组随机访问，读性能接近内存
- 写入与索引构建**解耦**，ReputMessageService 异步构建，不阻塞 Producer ACK

### 5.2 CommitLog 消息二进制格式

每条消息在 CommitLog 中的布局（`MessageDecoder` 解析）：

```
┌──────────┬──────────┬──────────┬─────────┬──────┬─────────────┬─────────────────┬─────────┬
│ TOTALSIZE│ MAGICCODE│  BODYCRC │ QUEUEID │ FLAG │ QUEUEOFFSET │ PHYSICALOFFSET  │ SYSFLAG │
│ 4 Bytes  │ 4 Bytes  │ 4 Bytes  │ 4 Bytes │4Bytes│  8 Bytes    │   8 Bytes       │ 4 Bytes │
├──────────┴──────────┴──────────┴─────────┴──────┴─────────────┴─────────────────┴─────────┤
│ BORNTIMESTAMP(8) │ BORNHOST(8/20) │ STORETIMESTAMP(8) │ STOREHOST(8/20) │ RECONSUMETIMES(4)│
├────────────────┴────────────────┴───────────────────┴─────────────────┴──────────────────┤
│ Prepared Transaction Offset (8) │ Body Size (4) │ Body (N) │ Topic Length (1) │ Topic (N)  │
├───────────────────────────────┴───────────────┴──────────┴──────────────────┴─────────────┤
│ Properties Length (2) │ Properties (N, key\001value\002...)                            │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

**MessageId** = `storeHost(ip+port)` + `physicalOffset(8)`，共 16 字节（IPv4）或 28 字节（IPv6）。

### 5.3 ConsumeQueue 索引结构

```java
// store/ConsumeQueue.java — 每条索引 20 字节定长
// ┌──────────────────┬────────────┬──────────────────┐
// │ CommitLog Offset │ Body Size  │   Tag HashCode   │
// │    (8 Bytes)     │ (4 Bytes)  │    (8 Bytes)     │
// └──────────────────┴────────────┴──────────────────┘
public static final int CQ_STORE_UNIT_SIZE = 20;
```

- 路径：`$STORE/consumequeue/{topic}/{queueId}/{fileName}`
- 单文件约 30 万条 × 20B ≈ **5.72MB**
- Tag 过滤直接在 CQ 层通过 HashCode 匹配，无需读 CommitLog

### 5.4 IndexFile 索引结构

```
路径: $STORE/index/{timestamp}

┌──────────── Header (40B) ────────────┐
│ beginTimestamp, endTimestamp, ...    │
├──────────── Slot Table ──────────────┤
│ 500W × 4B (Hash 槽位 → Index 链表头)  │
├──────────── Index Entry ─────────────┤
│ 2000W × 20B                          │
│ KeyHash(4) + PhyOffset(8) + TimeDiff(4) + NextIndex(4) │
└──────────────────────────────────────┘
```

- 支持按 MessageKey / UNIQ_KEY / 时间范围查询
- 底层 HashMap + 链表解决冲突

### 5.5 写入路径（CommitLog.asyncPutMessage）

```java
// store/CommitLog.java — 核心写入流程
asyncPutMessage(msg) {
    // 1. 预处理
    msg.setStoreTimestamp(now);
    msg.setBodyCRC(crc32(body));

    // 2. Controller 模式: 检查 inSyncReplicas 是否满足 minInSyncReplicas
    if (enableControllerMode && inSyncReplicas < minInSyncReplicas)
        return IN_SYNC_REPLICAS_NOT_ENOUGH;

    // 3. Topic-Queue 级锁 (避免同队列并发写导致 offset 乱序)
    topicQueueLock.lock(topicQueueKey);
    try {
        assignOffset(msg);  // 分配 queueOffset

        // 4. CommitLog 全局写锁
        putMessageLock.lock();  // SpinLock / ReentrantLock / ABSLock
        try {
            mappedFile = mappedFileQueue.getLastMappedFile();
            if (mappedFile == null || mappedFile.isFull())
                mappedFile = mappedFileQueue.getLastMappedFile(0);  // 创建新文件

            // 5. 追加写入 MappedByteBuffer
            result = mappedFile.appendMessage(msg, appendMessageCallback, ctx);
        } finally {
            putMessageLock.unlock();
        }
    } finally {
        topicQueueLock.unlock();
    }

    // 6. 刷盘
    flushManager.handleFlush(result, msg);

    // 7. HA 同步 (同步刷盘时 GroupTransferService 等待 Slave 确认)
    if (needHandleHA)
        haService.putRequest(groupCommitRequest);

    return PutMessageResult;
}
```

**两级锁设计**：

- `topicQueueLock`：同 Topic+QueueId 串行，保证 queueOffset 单调递增
- `putMessageLock`：CommitLog 全局写锁，保证物理 offset 单调递增

**MappedFile 机制**：

- `FileChannel.map()` → MappedByteBuffer，零拷贝
- `AllocateMappedFileService` 后台预创建文件，避免写入时阻塞
- **TransientStorePool**：DirectByteBuffer 池，写 DirectBuffer → 异步刷盘，降低 P99 延迟

### 5.6 异步分发（ReputMessageService）

CommitLog 写入与 CQ/Index 构建完全解耦：

```java
// DefaultMessageStore.ReputMessageService — 后台 ServiceThread
doReput() {
    while (reputFromOffset < confirmOffset) {
        SelectMappedBufferResult result = commitLog.getData(reputFromOffset);
        DispatchRequest req = commitLog.checkMessageAndReturnSize(result.getByteBuffer());

        if (req.isSuccess() && size > 0) {
            doDispatch(req);  // 链式 Dispatcher
            notifyMessageArriveIfNecessary(req);  // 唤醒长轮询
            reputFromOffset += size;
        }
    }
}
```

**CommitLogDispatcher 链**：

| Dispatcher | 作用 |
|-----------|------|
| `CommitLogDispatcherBuildConsumeQueue` | 构建 ConsumeQueue 索引 |
| `CommitLogDispatcherBuildIndex` | 构建 IndexFile |
| `CommitLogDispatcherBuildTransIndex` | 事务消息索引 |
| `CommitLogDispatcherCalcBitMap` | SQL92 过滤位图 |
| `CommitLogDispatcherCompaction` | KV/Compaction Topic |

### 5.7 刷盘策略

| 模式 | 配置 | 行为 | 场景 |
|------|------|------|------|
| SYNC_FLUSH | `flushDiskType=SYNC_FLUSH` | 消息落盘后才返回 ACK | 金融等高可靠 |
| ASYNC_FLUSH | `flushDiskType=ASYNC_FLUSH` | 写入 PageCache 即返回，后台线程刷盘 | 高吞吐（默认） |

同步刷盘 + HA：`GroupTransferService` 等待 `push2SlaveMaxOffset >= 当前 offset`。

### 5.8 HA 主从复制（DefaultHAService）

```
Master (BrokerRole=SYNC_MASTER/ASYNC_MASTER)
  │
  ├─ AcceptSocketService (:HA端口 = listenPort + 1)
  │    └─ 接受 Slave 连接 → HAConnection
  │         └─ 推送 CommitLog 增量数据
  │
  ├─ GroupTransferService
  │    └─ 同步刷盘时等待 Slave 确认 offset
  │
  └─ push2SlaveMaxOffset 追踪已同步进度

Slave (BrokerRole=SLAVE)
  │
  └─ HAClient
       └─ 主动连接 Master HA 端口
       └─ 接收数据 → 写入本地 CommitLog
       └─ 本地 ReputMessageService 构建 CQ
```

**AutoSwitchHAService**（Controller 模式）：

- 配合 `ReplicasManager` 实现自动主从切换
- `changeToMaster()` / `changeToSlave()` 动态切换角色
- SyncStateSet 管理同步副本集

**DLedgerCommitLog**（Raft 多副本）：

- 替代传统 Master-Slave，基于 DLedger Raft 共识
- `dLedgerServer.handleAppend()` → Raft AppendEntries → 多数派确认
- `dividedCommitlogOffset` 分隔旧 CommitLog 与 DLedger CommitLog

---

## 六、Client 客户端原理

### 6.1 MQClientInstance — 共享运行时

同一 `clientId` 下所有 Producer/Consumer 共享一个实例：

```java
// client/impl/factory/MQClientInstance.java
public void start() {
    mQClientAPIImpl.start();           // Netty Client
    startScheduledTask();              // 路由/心跳/offset 定时任务
    pullMessageService.start();        // Pull 调度
    rebalanceService.start();          // Rebalance 定时
    defaultMQProducerImpl.start(false); // 内部 Producer (心跳/Admin)
}
```

**定时任务**：

| 任务 | 间隔 | 作用 |
|------|------|------|
| fetchNameServerAddr | 2min | 动态获取 NS 地址 |
| updateTopicRouteInfo | 30s | 刷新路由 |
| sendHeartbeat | heartbeatBrokerInterval | 向 Broker 上报订阅关系 |
| persistConsumerOffset | persistConsumerOffsetInterval | 持久化消费进度 |
| adjustThreadPool | 1min | 动态调整消费线程池 |

### 6.2 Producer 发送与容错

**发送链路**：

```
DefaultMQProducer.send(msg)
  → DefaultMQProducerImpl.sendDefaultImpl(SYNC/ASYNC/ONEWAY)
       → tryToFindTopicPublishInfo(topic)  // 本地缓存 → NS 拉取
       → MQFaultStrategy.selectOneMessageQueue()  // 队列选择
       → sendKernelImpl()
            → 消息预处理 (uniqId, 压缩, namespace)
            → MQClientAPIImpl.sendMessage() → Broker
```

**MQFaultStrategy 延迟隔离**：

```
sendLatencyFaultEnable=true 时:
  1. 过滤 not-available broker (延迟超阈值 → 退避 30s~10min)
  2. 过滤 not-reachable broker
  3. round-robin 选队列 (TopicPublishInfo.sendWhichQueue)
```

**Sync 重试策略**：

- 次数：`1 + retryTimesWhenSendFailed`
- 避开上次失败 broker（`lastBrokerName`）
- `MQBrokerException` 仅在 `retryResponseCodes` 内重试
- 超时预算递减，per-request 上限 `sendMsgMaxTimeoutPerRequest`

### 6.3 Consumer Rebalance

**RebalanceImpl.doRebalance(isOrder)**：

```
for each subscribed topic:
    if clientRebalance(topic):
        rebalanceByTopic()  // Client 端分配
    else:
        getRebalanceResultFromBroker()  // Broker 端 Pop 分配

rebalanceByTopic() [集群模式]:
    mqSet = topicSubscribeInfoTable[topic]           // 全部队列
    cidAll = findConsumerIdList(topic, group)        // 全部 Consumer
    allocateResult = strategy.allocate(group, cid, mqAll, cidAll)
    updateProcessQueueTableInRebalance()
        → 移除不再拥有的队列 (ProcessQueue.dropped=true)
        → 新增队列: computePullFromWhere() → 创建 PullRequest → dispatchPullRequest()
```

**分配策略（AllocateMessageQueueStrategy）**：

| 策略 | 类 | 算法 |
|------|-----|------|
| AVG（默认） | AllocateMessageQueueAveragely | 均匀分配，余数给前 N 个 Consumer |
| CIRCLE | AllocateMessageQueueAveragelyByCircle | 环形分配 |
| CONSISTENT_HASH | AllocateMessageQueueConsistentHash | 一致性 Hash，扩缩容最小变动 |
| MACHINE_ROOM | AllocateMessageQueueByMachineRoom | 同机房优先 |

**核心约束**：同一 MessageQueue 同一时刻只允许同一 ConsumerGroup 内一个 Consumer 消费。

### 6.4 Push vs Lite Pull vs Pop

| 维度 | Push Consumer | Lite Pull Consumer | Pop Consumer |
|------|--------------|-------------------|--------------|
| 驱动 | PullMessageService 单线程 | 每队列 PullTaskImpl | PopMessageProcessor |
| 投递 | Listener 回调 | 用户 poll() | Listener 回调 |
| Offset | 自动提交 | 手动 commit() | ACK/NACK |
| Rebalance | dispatchPullRequest | 更新 AssignedMessageQueue | Broker queryAssignment |
| ConsumeType | CONSUME_PASSIVELY | CONSUME_ACTIVELY | CONSUME_POP |

---

## 七、Remoting 通信层

### 7.1 协议格式（RemotingCommand）

```
┌────────────┬─────────────────────┬──────────────┬──────────────┐
│ Total Len  │ SerializeType+HdrLen│   Header     │    Body      │
│  (4 Bytes) │     (4 Bytes)       │  (JSON/Proto)│  (Binary)    │
└────────────┴─────────────────────┴──────────────┴──────────────┘
```

**Header 关键字段**：

| 字段 | 类型 | 说明 |
|------|------|------|
| code | int | RequestCode / ResponseCode |
| opaque | int | 请求 ID，关联请求响应 |
| flag | int | RPC 类型（普通/Oneway） |
| language | LanguageCode | 语言标识 |
| version | int | 协议版本 |
| remark | String | 自定义文本 |
| extFields | Map | 扩展字段 |

### 7.2 Netty Reactor 线程模型

```
NettyBoss (1)                    → Accept TCP 连接
NettyServerSelector (N, 默认3)   → IO 读写 (NIO/Epoll)
NettyServerCodec (M1, 默认8)     → SSL / 编解码 / 空闲检测
RemotingExecutor (M2, 按 Processor) → 业务处理
```

**三种通信模式**：

| 模式 | 说明 | 场景 |
|------|------|------|
| SYNC | 阻塞等待响应 | 发送消息、拉取消息 |
| ASYNC | 回调处理响应 | 异步发送 |
| ONEWAY | 不等待响应 | 心跳、ACK |

### 7.3 关键 RequestCode 速查

| Code | 名称 | 方向 | 说明 |
|------|------|------|------|
| 103 | REGISTER_BROKER | Broker→NS | Broker 注册 |
| 104 | UNREGISTER_BROKER | Broker→NS | Broker 注销 |
| 105 | GET_ROUTEINFO_BY_TOPIC | Client→NS | 路由查询 |
| 310/311 | SEND_MESSAGE / V2 | Client→Broker | 消息发送 |
| 11 | PULL_MESSAGE | Client→Broker | 消息拉取 |
| 200050 | POP_MESSAGE | Client→Broker | Pop 消费 |
| 200051 | ACK_MESSAGE | Client→Broker | Pop 确认 |
| 104 | END_TRANSACTION | Client→Broker | 事务提交/回滚 |
| 322 | QUERY_DATA_VERSION | Broker→NS | 配置版本检查 |
| 904 | BROKER_HEARTBEAT | Broker→NS | 心跳 |

---

## 八、各特性实现原理

### 8.1 延迟消息（ScheduleMessageService）

**固定 Level 延迟**，18 个预设级别（1s, 5s, 10s, 30s, 1m, 2m, 3m, 4m, 5m, 6m, 7m, 8m, 9m, 10m, 20m, 30m, 1h, 2h）。

```
Producer 设置 msg.setDelayTimeLevel(3)
  │
  ▼
Broker SendMessageProcessor:
  改写 Topic → SCHEDULE_TOPIC_XXXX
  改写 queueId → delayLevel2QueueId(3) = 2
  写入 CommitLog (正常流程)
  │
  ▼
ScheduleMessageService (后台定时扫描):
  offsetTable[delayLevel] 记录每个 level 的消费进度
  扫描到期消息 → 恢复原始 Topic/Queue → 重新 putMessage → 消费者可见
```

**核心数据结构**：

```java
ConcurrentSkipListMap<Integer/* level */, Long/* delayTimeMillis */> delayLevelTable;
ConcurrentMap<Integer/* level */, Long/* offset */> offsetTable;
```

### 8.2 定时消息（TimerMessageStore + TimerWheel）

**任意精度定时**，5.x 新增，区别于固定 Level 的延迟消息。

```
Producer 设置 msg property: TIMER_DELIVER_MS = 目标投递时间戳
  │
  ▼
Broker: 写入内部 Topic RMQ_SYS_WHEEL_TIMER
  │
  ▼
TimerMessageStore:
  ├─ enqueuePutQueue → TimerWheel 时间槽
  │    TimerWheel: Mmap 文件, slotsTotal × precisionMs
  │    每个 Slot 存储: commitLogOffset + size + magic
  ├─ TimerDequeueService 扫描到期 Slot
  └─ dequeuePutQueue → 投递到真实 Topic (普通过 putMessage)
```

**TimerWheel 结构**：

```java
// store/timer/TimerWheel.java
// 文件: $STORE/timerwheel
// 大小: slotsTotal × 2 × Slot.SIZE (precisionMs 精度, 默认 1000ms)
// TTL: 默认 7 天 (TIMER_WHEEL_TTL_DAY)
// Slot 内容: commitLogOffset(8) + size(4) + magic(4) + ...
```

**队列流水线**：

```
enqueuePutQueue → [TimerWheel 写入] → dequeueGetQueue → [到期检测] → dequeuePutQueue → [真实 Topic 投递]
```

支持 `enableTimerWheelEnable`、`timerRocksDBEnable`（RocksDB 持久化）。

### 8.3 事务消息（2PC + 补偿回查）

#### 8.3.1 正常流程

```
Phase 1 — 发送 Half 消息:
  Producer: msg.setTransactionPrepared(true)
  Broker: TransactionalMessageService.asyncPrepareMessage()
    → Topic 替换为 RMQ_SYS_TRANS_HALF_TOPIC
    → 原始 Topic/Queue 保存到消息 Properties
    → 写入 CommitLog (Consumer 不可见，因未订阅 Half Topic)

Phase 2 — 本地事务 + 提交/回滚:
  Producer: executeLocalTransaction(msg, arg)
    → COMMIT: endTransaction(COMMIT)
    → ROLLBACK: endTransaction(ROLLBACK)
    → UNKNOW: 等待 Broker 回查

  Broker EndTransactionProcessor:
    COMMIT:
      读取 Half 消息 → 恢复真实 Topic/Queue → 普通过 putMessage (用户可见)
      写入 Op 消息到 RMQ_SYS_TRANS_OP_HALF_TOPIC (标记 COMMIT)
    ROLLBACK:
      写入 Op 消息 (标记 ROLLBACK, Half 消息无需删除)
```

#### 8.3.2 补偿回查

```
Broker TransactionalMessageCheckService (定时):
  扫描 RMQ_SYS_TRANS_HALF_TOPIC
  对比 RMQ_SYS_TRANS_OP_HALF_TOPIC
  无对应 Op 的 Half 消息 → CHECK_TRANSACTION_STATE → Producer

Producer ClientRemotingProcessor.checkTransactionState():
  → checkExecutor 线程池
  → TransactionListener.checkLocalTransaction(msg)
  → endTransactionOneway(COMMIT/ROLLBACK)

默认最多回查 15 次，超时自动 ROLLBACK
```

#### 8.3.3 核心设计点

| 设计 | 原因 |
|------|------|
| Topic 替换 | 顺序写友好，无需真正删除消息 |
| Op 消息 | 标记事务最终状态，支持回查对比 |
| Half 索引延迟构建 | Commit 时才构建真实 Topic 的 CQ 索引 |
| 回查而非无限重试 | 15 次上限，防止悬挂事务 |

### 8.4 消息过滤

#### Tag 过滤

```
订阅: consumer.subscribe("TopicA", "Tag1 || Tag2")
  → SubscriptionData.subString = "Tag1 || Tag2"
  → SubscriptionData.codeSet = {hash(Tag1), hash(Tag2)}

Broker 端 (Store 层):
  ConsumeQueue 存储 tagsCode (8B Hash)
  → ExpressionMessageFilter.isMatchedByConsumeQueue(tagsCode)
  → codeSet.contains(tagsCode)

Client 端 (二次过滤):
  → 精确比对 Tag 字符串 (Hash 冲突时丢弃)
```

#### SQL92 过滤

```
订阅: consumer.subscribe("TopicA", "a > 10 AND b IS NOT NULL", SQL92)

Broker 端:
  1. ConsumerFilterManager 维护 BloomFilter
  2. CommitLogDispatcherCalcBitMap 构建过滤位图
  3. isMatchedByConsumeQueue(): BloomFilter 预过滤 (tagsCode + bitMap)
  4. isMatchedByCommitLog(): 读取消息 Properties → SQL 表达式精确匹配
     (rocketmq-filter 模块 Evaluator 执行)
```

**Filter 在 Store 层 CQ 读取时执行**，而非读完 CommitLog 后过滤，减少随机 IO。

### 8.5 顺序消息

**Producer 端**：

```java
producer.send(msg, new MessageQueueSelector() {
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        int index = arg.hashCode() % mqs.size();
        return mqs.get(index);
    }
}, orderId);  // 相同 orderId → 相同 Queue → 顺序保证
```

**Consumer 端**：

```java
// ConsumeMessageOrderlyService
// 1. Rebalance 时对队列加 Broker 锁
rebalanceImpl.lock(mq);  // lockBatchMQ RPC

// 2. 消费时 ProcessQueue 读锁
processQueue.getConsumeLock().readLock().lock();
try {
    listener.consumeMessage(msgs, context);  // 单线程顺序消费
} finally {
    processQueue.getConsumeLock().readLock().unlock();
}

// 3. Rebalance 移除队列时释放锁
rebalanceImpl.unlock(mq);
```

### 8.6 重试与死信队列

```
消费失败 (集群模式, RECONSUME_LATER):
  → DefaultMQPushConsumerImpl.sendMessageBack()
  → CONSUMER_SEND_MSG_BACK → Broker
  → 写入 %RETRY%{ConsumerGroup} Topic
  → reconsumeTimes++

超过 maxReconsumeTimes (默认 16):
  → 投递到 %DLQ%{ConsumerGroup} (死信队列)
  → 需人工介入处理
```

广播模式：消费失败直接丢弃，无重试 Topic。

### 8.7 Pop 消费（无状态消费）— 深度剖析

Pop 是 5.x 核心演进，解决 Classic Push Rebalance 时的**重复消费**和**消息丢失**问题。

#### 8.7.1 Pop 基本流程

```
Consumer → POP_MESSAGE (200050)
  │
  ▼
PopMessageProcessor:
  1. 从 ConsumeQueue 读取消息 (类似 Pull)
  2. 设置 invisibleTime (消息不可见窗口，默认 15s)
  3. 创建 PopCheckPoint 记录:
     - startOffset, popTime, invisibleTime
     - bitMap (标记哪些消息已 Pop)
     - queueOffsetDiff (消息 offset 映射)
  4. 返回消息 + extraInfo (checkPoint 信息)

Consumer 处理完成:
  → ACK_MESSAGE (200051) + extraInfo
  → AckMessageProcessor: 标记 PopCheckPoint bitMap 对应位

Consumer 处理失败 / 超时:
  → 不 ACK → PopReviveService 在 invisibleTime 到期后重新投递
```

#### 8.7.2 PopCheckPoint 结构

```java
// store/pop/PopCheckPoint.java
class PopCheckPoint {
    long startOffset;       // CQ 起始 offset
    long popTime;           // Pop 时间戳
    long invisibleTime;     // 不可见窗口 (ms)
    int bitMap;             // 已 Pop 消息的位图
    byte num;               // 本次 Pop 消息数
    int queueId;
    String topic;
    String cid;             // Consumer Group
    long reviveOffset;      // Revive Topic 中的 offset
    List<Integer> queueOffsetDiff;
    String brokerName;
}
```

#### 8.7.3 PopReviveService — 超时重投递

```java
// broker/processor/PopReviveService.java
// 后台 ServiceThread，扫描 PopCheckPoint 中未 ACK 的消息

run() {
    while (!stopped) {
        // 1. 从 Revive Topic (%REVIVE_LOG_{cluster}) 读取 AckMsg
        // 2. 合并 PopCheckPoint (PopBufferMergeService)
        // 3. invisibleTime 到期且未 ACK → 重新投递 (rePut)
        //    - 写入 %RETRY% Topic 或直接 rePut 到原 Topic
        //    - reconsumeTimes++
        // 4. ckRewriteIntervalsInSeconds 控制重试间隔
        //    [10, 20, 30, 60, 120, ..., 7200] 秒
    }
}
```

**Revive Topic**：`%REVIVE_LOG_{cluster}`，Pop ACK 消息写入此处，Master Broker 的 PopReviveService 消费。

#### 8.7.4 Pop vs Classic Push 对比

| 维度 | Classic Push | Pop |
|------|-------------|-----|
| 队列归属 | Consumer Rebalance 分配 | Broker Pop 时临时分配 |
| Rebalance 影响 | 可能重复/丢失 | 无影响（PopCheckPoint 保护） |
| Offset 管理 | Consumer 本地/Broker 远程 | PopCheckPoint + ACK |
| 可见性 | 立即可见 | invisibleTime 窗口 |
| Proxy 支持 | 需 Rebalance | 原生支持 |

### 8.8 Controller 自动选主 — 深度剖析

#### 8.8.1 架构

```
┌──────────────┐     心跳/注册      ┌──────────────────┐
│   Broker     │ ──────────────→  │   Controller      │
│              │ ←──────────────  │  (DLedger/JRaft)  │
│ ReplicasManager│  选主结果/通知   │  ControllerManager│
└──────────────┘                  └──────────────────┘
       │                                    │
       └──── AutoSwitchHAService ───────────┘
              主从角色切换 + 数据同步
```

#### 8.8.2 Controller 核心流程

```java
// controller/impl/DLedgerController.java
// 基于 DLedger Raft 共识

electMaster(ElectMasterRequestHeader):
  1. ReplicasInfoManager 获取 Broker 成员组
  2. DefaultElectPolicy 选择新 Master (最小 brokerId / 自定义策略)
  3. 更新 SyncStateSet (同步副本集)
  4. DLedger AppendEntries 持久化选主事件
  5. NotifyService → NOTIFY_BROKER_ROLE_CHANGED → 新 Master Broker
```

#### 8.8.3 Broker 端 ReplicasManager

```java
// broker/controller/ReplicasManager.java

// Broker 启动时 (enableControllerMode=true):
registerBrokerToController()  // 向 Controller 注册
startSyncStateSetChecker()    // 定期检查 SyncStateSet

// 收到选主通知:
changeToMaster(newMasterEpoch, syncStateSetEpoch, syncStateSet):
  haService.changeToMaster(newMasterEpoch)  // 切换为 Master
  setFenced(false)                           // 解除写入隔离

changeToSlave(newMasterAddr, newMasterEpoch):
  haService.changeToSlave(newMasterAddr, newMasterEpoch)
  setFenced(true)  // 隔离写入

// 写入检查 (CommitLog.asyncPutMessage):
if (enableControllerMode && inSyncReplicas < minInSyncReplicas)
    return IN_SYNC_REPLICAS_NOT_ENOUGH;  // 不满足最小同步副本数
```

### 8.9 消息查询

#### 按 MessageId 查询

```
MessageId = storeHost(8/20B) + commitLogOffset(8B)
  → 解析出 Broker 地址 + offset
  → VIEW_MESSAGE_BY_ID (33) → QueryMessageProcessor
  → commitLog.getMessage(offset)
```

#### 按 MessageKey 查询

```
Key = topic + "#" + key
  → IndexService.selectPhyOffset(key)
  → IndexFile Hash 查找 → commitLogOffset
  → commitLog.getMessage(offset)
```

---

## 九、5.x 新架构演进

### 9.1 Proxy 网关

**两种部署模式**：

| 模式 | 说明 | 适用 |
|------|------|------|
| Cluster | Proxy 独立集群，RPC 访问 Broker | 计算存储分离，Proxy 无限扩展 |
| Local | Proxy 与 Broker 同进程，IPC 通信 | 4.x 平滑升级 |

**Proxy 职责**：

- gRPC ↔ Remoting 协议转换
- SSL 终结、认证鉴权
- 连接管理、流量治理
- Pop 消费原生支持
- 多语言客户端（配合 rocketmq-clients）

### 9.2 分层存储（TieredStore）

```
热数据 (本地 SSD)  ←──定时迁移──→  冷数据 (POSIX/S3/OSS)
     ↑                                    ↑
  CommitLog/CQ                      TieredMessageStore
  (MessageStore 插件)              (FileSegmentProvider)
```

**配置**：

```properties
messageStorePlugIn=org.apache.rocketmq.tieredstore.TieredMessageStore
tieredBackendServiceProvider=org.apache.rocketmq.tieredstore.provider.PosixFileSegment
tieredStorageLevel=NOT_IN_DISK  # DISABLE / NOT_IN_DISK / NOT_IN_MEM / FORCE
tieredStoreFileReservedTime=72  # 小时
```

**迁移触发**：2500 条或 32MB 批量上传，Read-ahead Cache 优化冷读。

### 9.3 RocksDB 扩展

5.x 多个组件可选 RocksDB 后端：

| 组件 | 类 | 用途 |
|------|-----|------|
| Index | IndexRocksDBStore | 海量 MessageKey 索引 |
| Timer | TimerMessageRocksDBStore | 定时消息持久化 |
| Transaction | TransMessageRocksDBStore | 事务状态存储 |
| Message | MessageRocksDBStorage | 可选消息存储后端 |

### 9.4 Static Topic（逻辑队列）

```
逻辑 Topic (Global QueueId 0, 1, 2...)
  ↕ TopicQueueMappingDetail
物理 Topic (Broker A Queue 0, Broker B Queue 1...)
```

- 支持跨 Broker 队列迁移而不改变 Client 视角
- `TopicQueueMappingManager` 负责逻辑→物理映射
- Send/Pull 时 `rewriteRequestForStaticTopic()` / `rewriteResponseForStaticTopic()`

### 9.5 Broker Container

`BrokerContainerStartup` 支持单 JVM 多 Broker 实例：

- 共享 Netty Server 端口
- 独立 MessageStore / Topic 配置
- 降低部署资源消耗

---

## 十、技术亮点与设计亮点

### 10.1 技术亮点

| 亮点 | 实现 | 价值 |
|------|------|------|
| CommitLog 顺序写 | 所有 Topic 共享单日志 | 磁盘顺序 IO，吞吐极高 |
| Mmap + 预分配 | MappedFile + AllocateMappedFileService | 零拷贝，无写入阻塞 |
| CQ 异步构建 | ReputMessageService | 写入与索引解耦 |
| 长轮询 | PullRequestHoldService | 实时性与 Broker 压力平衡 |
| 延迟隔离容错 | MQFaultStrategy | Broker 故障自动退避恢复 |
| 两级写锁 | topicQueueLock + putMessageLock | offset 严格单调 |
| TransientStorePool | DirectByteBuffer 池 | P99 延迟优化 |
| Plugin MessageStore | TieredStore / RocksDB | 可插拔存储后端 |
| Fast Remoting Server | 独立端口 | 高频 Send/ACK 低延迟 |
| Pop 无状态消费 | PopCheckPoint + Revive | 消除 Rebalance 痛点 |
| Controller + AutoSwitchHA | Raft 选主 + 角色切换 | RPO≈0 自动故障转移 |
| BatchUnregistrationService | NS 批量注销 | 避免锁竞争 |
| BloomFilter + BitMap | SQL92 过滤 | 减少无效 IO |

### 10.2 设计亮点

| 设计 | 价值 |
|------|------|
| NameServer 无状态 | 极简、无单点、零节点通信 |
| Topic 替换套路 | 延迟/事务/定时统一实现，Consumer 无感知 |
| Client 端负载均衡 | Rebalance 策略可插拔，Broker 无 Consumer 拓扑感知 |
| 推拉结合 | Push = Pull + 长轮询 + 本地线程池 |
| Half + Op 事务模型 | 顺序写友好，补偿回查完备 |
| NS Client/Broker 线程池隔离 | 路由查询不被注册流量阻塞 |
| Acting Master | Master 宕机只读消费不中断 |
| Zone 路由 | 多机房同 Zone 优先 |
| Static Topic | 逻辑队列跨 Broker 迁移 |
| 分离 computing/storage (Proxy) | 5.x 云原生方向 |

---

## 十一、源码阅读导航

### 按角色

| 目标 | 入口类 | 路径 |
|------|--------|------|
| NameServer 启动 | NamesrvStartup | namesrv/ |
| 路由管理 | RouteInfoManager | namesrv/routeinfo/ |
| Broker 启动 | BrokerStartup | broker/ |
| Broker 核心 | BrokerController | broker/ |
| 存储引擎 | DefaultMessageStore | store/ |
| 客户端运行时 | MQClientInstance | client/impl/factory/ |
| Remoting 协议 | RemotingCommand | remoting/protocol/ |
| Controller | ControllerManager | controller/ |
| Proxy | ProxyStartup | proxy/ |

### 按功能

| 功能 | 核心类 |
|------|--------|
| 消息发送 | broker/processor/SendMessageProcessor |
| 消息拉取 | broker/processor/PullMessageProcessor |
| Pop 消费 | broker/processor/PopMessageProcessor |
| Pop ACK | broker/processor/AckMessageProcessor |
| Pop 重投递 | broker/processor/PopReviveService |
| CommitLog 写入 | store/CommitLog |
| CQ 索引 | store/ConsumeQueue |
| 异步分发 | store/DefaultMessageStore.ReputMessageService |
| HA 复制 | store/ha/DefaultHAService |
| DLedger | store/dledger/DLedgerCommitLog |
| 延迟消息 | broker/schedule/ScheduleMessageService |
| 定时消息 | store/timer/TimerMessageStore |
| 时间轮 | store/timer/TimerWheel |
| 事务消息 | broker/transaction/queue/TransactionalMessageServiceImpl |
| 事务提交 | broker/processor/EndTransactionProcessor |
| 消息过滤 | broker/filter/ExpressionMessageFilter |
| SQL 表达式 | filter/ |
| 长轮询 | broker/longpolling/PullRequestHoldService |
| Producer 发送 | client/impl/producer/DefaultMQProducerImpl |
| 延迟隔离 | client/latency/MQFaultStrategy |
| Rebalance | client/impl/consumer/RebalanceImpl |
| Push 消费 | client/impl/consumer/DefaultMQPushConsumerImpl |
| Lite Pull | client/impl/consumer/DefaultLitePullConsumerImpl |
| 顺序消费 | client/impl/consumer/ConsumeMessageOrderlyService |
| Controller 选主 | controller/impl/DLedgerController |
| Broker 选主响应 | broker/controller/ReplicasManager |
| 分层存储 | tieredstore/TieredMessageStore |
| 消费进度 | broker/offset/ConsumerOffsetManager |
| 启动恢复 | store/DefaultMessageStore.load/recover |
| 文件清理 | store/DefaultMessageStore.CleanCommitLogService |
| Pull 封装 | client/impl/consumer/PullAPIWrapper |
| Pop Buffer | broker/processor/PopBufferMergeService |
| 认证 Pipeline | broker/auth/pipeline/AuthenticationPipeline |
| 鉴权 Pipeline | broker/auth/pipeline/AuthorizationPipeline |
| Compaction | store/kv/CompactionStore |
| Broker 对外 API | broker/out/BrokerOuterAPI |
| Netty Server | remoting/netty/NettyRemotingServer |
| 消息压缩 | client/impl/producer/DefaultMQProducerImpl.tryToCompressMessage |
| 刷盘管理 | store/CommitLog.DefaultFlushManager |
| 堆外内存池 | store/TransientStorePool |
| MappedFile 预分配 | store/AllocateMappedFileService |
| Proxy gRPC | proxy/grpc/v2/GrpcMessagingApplication |
| Proxy 启动 | proxy/ProxyStartup |
| MessagingProcessor | proxy/processor/DefaultMessagingProcessor |
| 选主策略 | controller/elect/impl/DefaultElectPolicy |
| ReplicasManager | broker/controller/ReplicasManager |
| DLedger CommitLog | store/dledger/DLedgerCommitLog |
| 冷数据流控 | broker/coldctr/ColdDataPullRequestHoldService |
| 请求回复 | broker/processor/ReplyMessageProcessor |
| 消息召回 | broker/processor/RecallMessageProcessor |
| 消息轨迹 | client/trace/AsyncTraceDispatcher |
| Pop 通知 | broker/processor/NotificationProcessor |
| Broker Metrics | broker/metrics/BrokerMetricsManager |
| Store Metrics | store/metrics/DefaultStoreMetricsManager |
| Proxy Cluster 服务 | proxy/service/ClusterServiceManager |
| Proxy Local 服务 | proxy/service/LocalServiceManager |
| Proxy Remoting | proxy/remoting/RemotingProtocolServer |
| JRaft 状态机 | controller/impl/JRaftControllerStateMachine |
| RocksDB Store | store/RocksDBMessageStore |
| BrokerContainer | container/BrokerContainer |
| Static Topic | broker/topic/TopicQueueMappingManager |
| RunningFlags | store/RunningFlags |
| HA 传输 | store/ha/DefaultHAConnection |
| Namespace | remoting/protocol/NamespaceUtil |
| Pop 顺序消费 | broker/pop/orderly/ConsumerOrderInfoManager |
| Config v2 | broker/config/v2/ConfigStorage |
| OpenMessaging | openmessaging/MessagingAccessPointImpl |
| msgId 生成 | common/message/MessageClientIDSetter |
| mqadmin | tools/command/MQAdminStartup |
| TieredStore | tieredstore/TieredMessageStore |
| Tiered 迁移 | tieredstore/core/MessageStoreDispatcherImpl |
| gRPC Telemetry | proxy/grpc/v2/client/ClientActivity |
| ConsumeQueueExt | store/ConsumeQueueExt |
| QueueOffset | store/queue/QueueOffsetOperator |
| Store 插件 | store/plugin/AbstractPluginMessageStore |
| 异步背压 | client/impl/producer/DefaultMQProducerImpl |
| Peek 消息 | broker/processor/PeekMessageProcessor |
| Pop 消费者锁 | broker/pop/PopConsumerLockService |
| Broker 统计 | store/stats/BrokerStatsManager |
| Order Topic KV | namesrv/kvconfig/KVConfigManager |

### 按请求码追踪

```
发送: Client SEND_MESSAGE(310) → Broker SendMessageProcessor → CommitLog.putMessage
拉取: Client PULL_MESSAGE(11) → Broker PullMessageProcessor → Store.getMessage
Pop:  Client POP_MESSAGE(200050) → PopMessageProcessor → PopCheckPoint
ACK:  Client ACK_MESSAGE(200051) → AckMessageProcessor → PopReviveService
路由: Client GET_ROUTEINFO(105) → NS ClientRequestProcessor → RouteInfoManager
注册: Broker REGISTER_BROKER(103) → NS DefaultRequestProcessor → RouteInfoManager
事务: Client END_TRANSACTION → EndTransactionProcessor → TransactionalMessageService
```

---

## 附录 A：关键配置参数

| 配置 | 默认值 | 模块 | 说明 |
|------|--------|------|------|
| listenPort | 9876 | NameServer | NS 监听端口 |
| listenPort | 10911 | Broker | Broker 监听端口 |
| scanNotActiveBrokerInterval | 5s | NameServer | Broker 过期扫描 |
| brokerChannelExpiredTime | 2min | NameServer | Broker 心跳超时 |
| registerNameServerPeriod | 30s | Broker | 向 NS 注册周期 |
| pollNameServerInterval | 30s | Client | 路由刷新间隔 |
| rebalance.waitInterval | 20s | Client | Rebalance 间隔 |
| mappedFileSizeCommitLog | 1GB | Store | CommitLog 单文件大小 |
| flushDiskType | ASYNC_FLUSH | Store | 刷盘方式 |
| transferMsgByHeap | true | Remoting | 消息堆内传输 |
| enableControllerMode | false | Broker | Controller 自动选主 |
| minInSyncReplicas | 1 | Store | 最小同步副本数 |
| fileReservedTime | 72h | Store | CommitLog 保留时间 |
| pullThresholdForQueue | 1000 | Client | 本地缓存消息数上限 |
| compressMsgBodyOverHowmuch | 4096 | Client | 消息压缩阈值 |
| diskSpaceWarningLevelRatio | 90% | Store | 磁盘告警阈值 |
| diskMaxUsedSpaceRatio | 75% | Store | 磁盘写拒绝阈值 |
| consumeTimeout | 15min | Client | 消费超时清理 |
| invisibleTime | 15s | Pop | Pop 消息不可见窗口 |
| transactionCheckMax | 15 | Broker | 事务回查最大次数 |

---

## 附录 B：部署架构要点

```
                    ┌─── NameServer Cluster ───┐
                    │  NS1    NS2    NS3       │  (无状态, 互不通信)
                    └──────────┬───────────────┘
                               │ 注册/心跳
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
        ┌──────────┐    ┌──────────┐    ┌──────────┐
        │ Broker-A │    │ Broker-B │    │ Broker-C │
        │ Master   │    │ Slave    │    │ Slave    │
        │ :10911   │    │ :10911   │    │ :10911   │
        └────┬─────┘    └────┬─────┘    └────┬─────┘
             │    HA 同步     │               │
             └────────────────┘               │
                                               │
                    ┌─── Controller ───┐       │
                    │  (Raft 选主)      │←──────┘
                    └──────────────────┘

        ┌─── Proxy Cluster ───┐  (可选)
        │  Proxy1   Proxy2    │  gRPC → Remoting → Broker
        └─────────────────────┘
```

- 一个 Master 可对应多个 Slave（同 BrokerName，不同 BrokerId）
- BrokerId=0 为 Master，非 0 为 Slave
- Producer 只向 Master 发送
- Consumer 可从 Master 或 Slave 消费（Broker 建议）
- Controller 集群独立部署，Broker 通过 ReplicasManager 交互

---

## 附录 C：性能调优要点

| 维度 | 参数/手段 | 建议 |
|------|----------|------|
| 发送吞吐 | `flushDiskType=ASYNC_FLUSH` | 异步刷盘，吞吐优先 |
| 发送延迟 | TransientStorePool + Fast Remoting Server | 降低 P99 |
| 发送可靠 | `flushDiskType=SYNC_FLUSH` + `brokerRole=SYNC_MASTER` | 同步刷盘 + 同步复制 |
| 消费吞吐 | `consumeThreadMin/Max`、批量拉取 `pullBatchSize` | 按 CPU 核数调整 |
| 消费堆积 | 增加 Consumer 实例、扩大队列数 | Rebalance 自动分摊 |
| 磁盘 | `fileReservedTime`、监控 diskSpaceWarningLevelRatio | 预留足够保留时间 |
| 网络 | `sendMsgTimeout`、`clientCallbackExecutorThreads` | 避免超时堆积 |
| JVM | 堆外内存（Mmap/DirectBuffer）、G1GC | 避免 Full GC 停顿 |
| HA | Controller + minInSyncReplicas | 自动选主 + 最小同步副本 |

---

## 附录 D：MessageStore 状态机

Broker 启动时 `DefaultMessageStore.load()` 通过状态机追踪进度：

```
INITIAL
  → LOAD_COMMITLOG_OK
  → LOAD_CONSUME_QUEUE_OK
  → LOAD_COMPACTION_OK (可选)
  → LOAD_INDEX_OK
  → RECOVER_BEGIN
  → RECOVER_CONSUME_QUEUE_OK
  → RECOVER_COMMITLOG_OK
  → RECOVER_TOPIC_QUEUE_TABLE_OK
  → RUNNING
```

异常退出（`lastExitOK=false`）时走 `recoverAbnormally()`，从 CQ 最小 dispatch offset 重放 CommitLog。

---

## 十二、Broker 启动恢复机制

### 12.1 load() 加载阶段

```java
// store/DefaultMessageStore.java — load()
boolean lastExitOK = !isTempFileExist();  // 检测上次是否正常退出

1. commitLog.load()              // 加载 CommitLog MappedFile 列表
2. consumeQueueStore.load()      // 加载全部 ConsumeQueue 文件
3. compactionService.load()      // Compaction Topic (可选)
4. loadCheckPoint()              // 读取 StoreCheckpoint (confirmOffset/masterFlushedOffset)
5. indexService.load()            // 加载 IndexFile
6. recover(lastExitOK)           // 恢复阶段
```

**StoreCheckpoint**（`$STORE/checkpoint`）持久化：

| 字段 | 含义 |
|------|------|
| physicMsgTimestamp | 最后一条消息存储时间 |
| logicsMsgTimestamp | CQ 最后更新时间 |
| confirmPhyOffset | 已确认物理 offset（Reput 进度基准） |
| masterFlushedOffset | Master 刷盘 offset |

### 12.2 recover() 恢复阶段

```
Step 1: consumeQueueStore.recover(concurrently)
  → 每个 CQ 扫描 MappedFile，恢复 minLogicOffset / maxOffset

Step 2: 计算 dispatchFromPhyOffset
  → min(所有 CQ 的 dispatch offset, Index 的 dispatch offset)
  → 取最小值作为 Reput 起点，保证 CQ/Index 不丢数据

Step 3: CommitLog 恢复
  lastExitOK=true  → recoverNormally(dispatchFromPhyOffset)
  lastExitOK=false → recoverAbnormally(dispatchFromPhyOffset)
    → 扫描 CommitLog，找到第一个有效消息
    → 截断尾部脏数据（未完整写入的消息）

Step 4: recoverTopicQueueTable()
  → 恢复 Topic-Queue 的 maxOffset 映射表
```

### 12.3 start() 启动后台服务

```java
// recover 完成后 start()
haService.init()                          // HA 服务
allocateMappedFileService.start()         // 预分配 MappedFile
reputMessageService.setReputFromOffset(commitLog.getConfirmOffset())
reputMessageService.start()               // 开始异步构建 CQ/Index
flushConsumeQueueService.start()          // CQ 刷盘
indexService.start()                      // Index 刷盘
scheduleMessageService.start()            // 延迟消息投递
transactionalMessageService.start()       // 事务回查
```

**关键设计**：即使异常宕机，最多丢失 PageCache 中未刷盘的数据；CommitLog 尾部脏数据在 `recoverAbnormally` 时被截断，保证文件完整性。

---

## 十三、getMessage 读取路径详解

### 13.1 完整读取流程

```java
// DefaultMessageStore.getMessage(group, topic, queueId, offset, maxMsgNums, filter)
```

```
1. 前置检查
   shutdown / !runningFlags.isReadable() → 拒绝

2. Compaction Topic 分支
   cleanupPolicy=COMPACTION → compactionStore.getMessage()

3. 获取 ConsumeQueue
   findConsumeQueue(topic, queueId)
   minOffset = cq.getMinOffsetInQueue()
   maxOffset = cq.getMaxOffsetInQueue()

4. Offset 合法性检查
   offset < minOffset  → OFFSET_TOO_SMALL
   offset == maxOffset → OFFSET_OVERFLOW_ONE (无新消息)
   offset > maxOffset  → OFFSET_OVERFLOW_BADLY

5. CQ 迭代读取 (核心循环)
   while (bufferTotalSize <= 0 && nextBeginOffset < maxOffset) {
       bufferConsumeQueue = cq.iterateFrom(nextBeginOffset, maxMsgNums)
       for each CqUnit {
           offsetPy = cqUnit.getPos()      // CommitLog 物理 offset
           sizePy   = cqUnit.getSize()     // 消息大小
           tagsCode = cqUnit.getTagsCode() // Tag Hash

           // 5a. Filter 预过滤 (CQ 层，不读 CommitLog)
           if (!filter.isMatchedByConsumeQueue(tagsCode))
               continue;  // filterMessageCount++

           // 5b. 从 CommitLog 读取消息体
           selectResult = commitLog.getMessage(offsetPy, sizePy)
           if (!filter.isMatchedByCommitLog(selectResult.getByteBuffer()))
               continue;

           // 5c. 加入结果集
           getResult.addMessage(selectResult)
           nextBeginOffset = cqUnit.getQueueOffset() + cqUnit.getBatchNum()
       }
   }

6. 返回 GetMessageResult (status + messages + nextBeginOffset)
```

### 13.2 GetMessageStatus 枚举

| Status | 含义 | Client 行为 |
|--------|------|------------|
| FOUND | 读到消息 | 投递消费 |
| NO_MATCHED_MESSAGE | CQ 有数据但 Filter 全不匹配 | 更新 offset 继续拉 |
| NO_MESSAGE_IN_QUEUE | 队列空 | 长轮询挂起 |
| OFFSET_TOO_SMALL | offset 过小（消息已过期清理） | 修正 offset |
| OFFSET_OVERFLOW_ONE | offset == maxOffset | 长轮询挂起 |
| OFFSET_OVERFLOW_BADLY | offset 远超 maxOffset | rebalance |
| OFFSET_FOUND_NULL | CQ 文件损坏 | 修正 offset |

### 13.3 性能优化点

- **CQ 层 Filter**：Tag/SQL92 BloomFilter 在读 CommitLog 之前过滤，减少随机 IO
- **批量读取限制**：`maxPullSize`（默认 32KB）、`maxMsgNums`（默认 32条）
- **内存判定**：`estimateInMemByCommitOffset()` 判断消息是否在 PageCache 中，影响批量策略
- **TravelCqFileNumWhenGetMessage**：限制单次拉取跨越的 CQ 文件数

---

## 十四、文件清理与磁盘管理

### 14.1 CleanCommitLogService

后台定时任务（默认每 10s），负责过期 CommitLog 文件删除：

```java
// DefaultMessageStore.CleanCommitLogService
deleteExpiredFiles() {
    isTimeUp = isTimeToDelete()           // 凌晨 4~5 点删除窗口
    isUsageExceedsThreshold = isSpaceToDelete()  // 磁盘使用率超阈值
    isManualDelete = manualDeleteFileSeveralTimes > 0

    if (isTimeUp || isUsageExceedsThreshold || isManualDelete) {
        commitLog.deleteExpiredFile(
            fileReservedTime,              // 默认 72 小时
            deletePhysicFilesInterval,     // 删除间隔
            destroyMappedFileIntervalForcibly,
            cleanAtOnce,                   // 强制清理
            deleteFileBatchMax             // 单次最大删除文件数
        )
    }
}
```

**删除条件**：CommitLog 文件中最大消息存储时间 + `fileReservedTime` < 当前时间，且该文件对应的 CQ 索引已全部消费完毕。

### 14.2 磁盘空间保护

| 阈值 | 配置 | 行为 |
|------|------|------|
| 警告 | diskSpaceWarningLevelRatio (默认 90%) | 日志告警 |
| 强制清理 | diskSpaceCleanForciblyRatio (默认 85%) | 无视保留时间强制删除 |
| 拒绝写入 | diskMaxUsedSpaceRatio (默认 75%) | Broker 标记不可写 |

### 14.3 ConsumeQueue 清理

`CleanConsumeQueueService` 与 CommitLog 清理联动：

- CQ 文件对应的 CommitLog 已被删除 → CQ 文件可删除
- `minLogicOffset` 随 CommitLog 清理向前推进

### 14.4 IndexFile 清理

- 随 CommitLog 过期同步删除对应时间段的 IndexFile
- Index 文件以创建时间戳命名，按时间范围关联

---

## 十五、Client 消费流控与线程模型

### 15.1 消费线程池

```java
// ConsumeMessageConcurrentlyService
ThreadPoolExecutor consumeExecutor = new ThreadPoolExecutor(
    consumeThreadMin,    // 默认 20
    consumeThreadMax,    // 默认 20
    60, SECONDS,
    consumeRequestQueue  // 无界 LinkedBlockingQueue
);
```

每个 `ConsumeRequest` 是一个 Runnable，从 `ProcessQueue` 取一批消息调用 Listener。

### 15.2 拉取流控（DefaultMQPushConsumerImpl.pullMessage）

在发起 Pull 之前检查：

| 流控维度 | 配置 | 默认 | 说明 |
|---------|------|------|------|
| 本地缓存消息数 | pullThresholdForQueue | 1000 | ProcessQueue 消息数上限 |
| 本地缓存消息大小 | pullThresholdSizeForQueue | 100MB | ProcessQueue 字节上限 |
| 消息跨度 | consumeConcurrentlyMaxSpan | 2000 | maxOffset - minOffset 上限 |
| 消费间隔 | pullInterval | 0 | 两次 Pull 间隔 ms |
| 批量大小 | pullBatchSize | 32 | 单次拉取最大消息数 |

```java
// 流控触发 → 延迟 pullInterval 后再 Pull，不发起 RPC
if (processQueue.getMsgCount() > pullThresholdForQueue) {
    executePullRequestLater(pullRequest, pullInterval);
    return;
}
```

### 15.3 消费结果处理

```java
// ConsumeMessageConcurrentlyService.processConsumeResult()
switch (status) {
    case CONSUME_SUCCESS:
        // 集群: updateOffset → Broker
        // 广播: updateOffset → 本地文件
        break;
    case RECONSUME_LATER:
        // 集群: sendMessageBack() → %RETRY%Topic
        // 广播: 丢弃（无重试）
        break;
}
```

### 15.4 消费超时清理

`cleanExpireMsgExecutors` 定时扫描 `ProcessQueue`：

- 消息在本地缓存超过 `consumeTimeout`（默认 15min）未消费完
- 发送 `sendMessageBack()` 到重试队列
- 防止 Consumer 假死导致消息长期占用

---

## 十六、PullAPIWrapper 与读写分离

### 16.1 职责

`PullAPIWrapper` 是 Client 端 Pull/Pop 的统一封装：

```java
// client/impl/consumer/PullAPIWrapper.java
pullKernelImpl(mq, subData, offset, maxNums, sysFlag, timeout, mode, pullCallback)
popKernelImpl(mq, subData, offset, maxNums, invisibleTime, mode, popCallback)
processPullResult(mq, pullResult, subData)  // 解码 + Tag 过滤
updatePullFromWhichNode(mq, suggestWhichBrokerId)  // Master/Slave 切换
```

### 16.2 Master/Slave 读切换

```java
// pullFromWhichNodeTable: MessageQueue → brokerId
ConcurrentMap<MessageQueue, AtomicLong> pullFromWhichNodeTable;

updatePullFromWhichNode(mq, suggestWhichBrokerId) {
    // Broker 在 Pull 响应中建议下次从哪个 Broker 读
    pullFromWhichNodeTable.put(mq, suggestWhichBrokerId);
}

findBrokerResult(mq) {
    brokerId = pullFromWhichNodeTable.get(mq);  // 优先用建议的 Broker
    return mQClientFactory.findBrokerAddressInSubscribe(brokerName, brokerId);
}
```

**Broker 建议逻辑**（PullMessageProcessor）：

- Consumer offset 接近 maxOffset（读新消息）→ 建议 Master
- Consumer offset 远小于 maxOffset（读历史消息）→ 建议 Slave（若可读）
- Slave 不可用 → 回退 Master

### 16.3 客户端 Tag 二次过滤

Broker 端 Tag 过滤基于 HashCode，存在 Hash 冲突可能：

```java
// PullAPIWrapper.processPullResult()
msgList = MessageDecoder.decodesBatch(byteBuffer);
// 客户端精确 Tag 字符串匹配
for (MessageExt msg : msgList) {
    if (!subString.equals(SUB_ALL) && !tagsSet.contains(msg.getTags())) {
        continue;  // 丢弃 Hash 冲突的误匹配
    }
}
```

---

## 十七、Pop Buffer 合并与 ACK 机制

### 17.1 PopBufferMergeService

Pop 模式下，ACK 消息先写入 Revive Topic，由 `PopBufferMergeService` 合并处理：

```java
// broker/processor/PopBufferMergeService.java
ConcurrentHashMap<String/*mergeKey*/, PopCheckPointWrapper> buffer;
ConcurrentHashMap<String/*topic@cid@queueId*/, QueueWithTime<PopCheckPointWrapper>> commitOffsets;

// mergeKey = topic + cid + queueId + startOffset + popTime + invisibleTime
```

**工作流程**：

```
1. PopMessageProcessor.popMessage()
   → 创建 PopCheckPoint，设置 bitMap/invisibleTime
   → 返回消息 + extraInfo (Base64 编码的 checkpoint 信息)

2. Consumer 处理完成 → ACK_MESSAGE
   → AckMessageProcessor.processAck()
   → 构建 AckMsg → 写入 Revive Topic (%REVIVE_LOG_{cluster})

3. PopBufferMergeService (每 5ms 扫描)
   → 从 Revive Topic 读取 AckMsg
   → 合并到 buffer 中对应的 PopCheckPointWrapper
   → 更新 bitMap 标记已 ACK 位
   → 当 PopCheckPoint 全部 ACK → 提交 commitOffset
   → 更新 ConsumerOffsetManager

4. PopReviveService (Master 专属)
   → 扫描 invisibleTime 到期的 PopCheckPoint
   → 未 ACK 的消息 → rePut (重新投递)
   → 写入 %RETRY% Topic 或直接 rePut 到原 Topic
```

### 17.2 ACK 类型

| 类型 | RequestCode | 说明 |
|------|-------------|------|
| 单条 ACK | ACK_MESSAGE (200051) | 确认单条消息 |
| 批量 ACK | BATCH_ACK_MESSAGE | 确认多条消息 |
| 续期 | CHANGE_MESSAGE_INVISIBLETIME | 延长 invisibleTime |
| NACK | ACK with NACK flag | 立即重投递（不增加 reconsumeTimes） |

### 17.3 PopCheckPoint bitMap

```
一次 Pop 最多 32 条消息 (bitMap = 32 bit)
bitMap 每位对应一条消息的 ACK 状态:
  0 = 未 ACK (invisibleTime 到期后重投递)
  1 = 已 ACK (不再重投递)

queueOffsetDiff[] 记录每条消息相对于 startOffset 的偏移
```

### 17.4 Pop 与 Classic 消费对比（Rebalance 场景）

```
Classic Push Rebalance 问题:
  Consumer-A 持有 Queue-1, offset=100
  Rebalance 后 Queue-1 转给 Consumer-B
  → Consumer-B 从 Broker offset 继续 (可能 < 100)
  → 消息 100~BrokerOffset 重复消费
  → Consumer-A 本地未消费完的消息丢失

Pop 模式:
  消息 Pop 时设置 invisibleTime
  Rebalance 不影响 inflight 消息
  未 ACK → PopReviveService 重投递
  → 无重复、无丢失
```

---

## 十八、消息压缩与批量发送

### 18.1 消息压缩

```java
// DefaultMQProducerImpl.tryToCompressMessage()
if (body.length >= compressMsgBodyOverHowmuch) {  // 默认 4096 字节
    data = compressor.compress(body, compressLevel);  // 默认 level=5
    msg.setBody(data);
    // sendKernelImpl 中设置 COMPRESSED_FLAG sysFlag
}
```

| 配置 | 默认值 | 说明 |
|------|--------|------|
| compressMsgBodyOverHowmuch | 4096 | 超过此大小才压缩 |
| compressLevel | 5 | 压缩级别 (1~9) |
| compressType | ZLIB | 压缩算法 (ZLIB/LZ4/ZSTD) |

Consumer 端 `MessageDecoder.decodesBatch()` 根据 `COMPRESSED_FLAG` 自动解压。

### 18.2 批量发送

```java
// SendMessageProcessor.sendBatchMessage()
// RequestCode.SEND_BATCH_MESSAGE (312)

1. 解析 MessageExtBatch (多条消息合并为一个 Batch)
2. 每条消息独立编码，合并写入 CommitLog
3. 设置 INNER_BATCH_FLAG sysFlag
4. Consumer 端 PullAPIWrapper 检测 NEED_UNWRAP_FLAG → 拆包为单条消息
```

**限制**：Batch 不支持延迟消息、事务消息、定时消息。

### 18.3 批量 Pull

Consumer 端 `pullBatchSize`（默认 32）控制单次拉取条数，Broker 端 `getMessage` 循环读取 CQ 直到达到 `maxMsgNums` 或 `maxPullSize`。

---

## 十九、认证鉴权体系

### 19.1 架构（5.x auth 模块）

```
Remoting Request
  │
  ▼
AuthenticationPipeline (认证: 你是谁?)
  → AuthenticationEvaluator
  → AuthenticationStrategy (Stateless/Stateful)
  → AuthenticationMetadataProvider (Local/Remote)
  │
  ▼
AuthorizationPipeline (鉴权: 你能做什么?)
  → AuthorizationEvaluator
  → AuthorizationStrategy
  → PolicyEntry (Resource + Action + Decision)
  │
  ▼
Processor (业务处理)
```

### 19.2 核心类

| 类 | 模块 | 职责 |
|----|------|------|
| `AuthenticationPipeline` | broker/auth | 请求认证拦截 |
| `AuthorizationPipeline` | broker/auth | 请求鉴权拦截 |
| `AuthenticationEvaluator` | auth | 认证评估器 |
| `AuthorizationEvaluator` | auth | 鉴权评估器 |
| `Acl` / `PolicyEntry` | auth | ACL 策略模型 |
| `AuthConfig` | auth | 认证鉴权配置 |

### 19.3 认证流程

```java
// broker/auth/pipeline/AuthenticationPipeline.java
execute(ctx, request) {
    if (!authConfig.isAuthenticationEnabled()) return;

    AuthenticationContext context = newContext(ctx, request);
    // 从 request.extFields 提取凭证 (AccessKey/Signature)
    evaluator.evaluate(context);
    // 失败 → AbortProcessException(NO_PERMISSION)
}
```

### 19.4 鉴权模型

```java
// auth/authorization/model/
Acl {
    Subject subject;           // User / UserGroup
    PolicyType policyType;     // CUSTOM / DEFAULT
    List<PolicyEntry> entries; // 策略条目
}

PolicyEntry {
    Resource resource;         // Topic / Group / Cluster / *
    Decision decision;         // ALLOW / DENY
    List<String> actions;      // PUB / SUB / CREATE / UPDATE / DELETE / ANY
}
```

### 19.5 兼容旧 ACL

`auth/migration/v1/` 提供从 4.x PlainAccessConfig 到 5.x ACL 模型的迁移：

- `PlainAccessData` → 转换为新版 `Acl` + `PolicyEntry`
- `AuthMigrator` 自动迁移工具

Proxy 端独立实现：`ProxyAuthenticationMetadataProvider` / `ProxyAuthorizationMetadataProvider`。

---

## 二十、Hook 与 Pipeline 扩展机制

### 20.1 Client 端 Hook

| Hook 接口 | 触发时机 | 用途 |
|-----------|---------|------|
| `SendMessageHook` | 发送前/后 | 消息轨迹、审计 |
| `ConsumeMessageHook` | 消费前/后 | 消息轨迹、监控 |
| `FilterMessageHook` | Pull 结果过滤后 | 自定义过滤 |
| `CheckForbiddenHook` | 发送前检查 | 黑名单拦截 |

```java
// DefaultMQProducerImpl.sendKernelImpl()
executeCheckForbiddenHook(context);     // 禁止发送检查
executeSendMessageHookBefore(context);  // 发送前 Hook
// ... remoting send ...
executeSendMessageHookAfter(context);   // 发送后 Hook
```

### 20.2 Broker 端 Hook

| Hook 接口 | 触发时机 | 用途 |
|-----------|---------|------|
| `SendMessageHook` | 发送前/后 | 轨迹、延迟统计 |
| `ConsumeMessageHook` | 拉取后/消费前 | 轨迹 |
| `PutMessageHook` | 消息写入 Store 前 | 存储拦截 |
| `SendMessageBackHook` | 消息重试前 | 重试拦截 |

### 20.3 Remoting Pipeline

5.x 引入 `RequestPipeline` 链式拦截（替代旧 RPCHook）：

```java
// BrokerController.initialRequestPipeline()
requestPipeline.add(new AuthenticationPipeline(authConfig));
requestPipeline.add(new AuthorizationPipeline(authConfig));
// Processor 执行前依次经过 Pipeline
```

Pipeline 与 Processor 的关系：

```
Netty IO Thread → 解码 RemotingCommand
  → Remoting Executor Thread
    → RequestPipeline Chain (认证/鉴权/...)
    → NettyRequestProcessor.processRequest()
```

### 20.4 MessageStore 插件

```java
// store/plugin/MessageStorePlugin
// 通过 messageStorePlugIn 配置替换 DefaultMessageStore
// 实现: TieredMessageStore, RocksDBMessageStore, ...
MessageStore plugin = (MessageStore) Class.forName(messageStorePlugIn).newInstance();
plugin.load();
plugin.start();
```

---

## 二十一、Netty Pipeline 与连接管理

### 21.1 Server Pipeline 组成

```java
// remoting/netty/NettyRemotingServer.java — ChannelPipeline
pipeline.addLast("HAProxyDecoder", ...)       // 可选: 负载均衡器代理
pipeline.addLast("IdleStateHandler", ...)      // 空闲检测 (120s)
pipeline.addLast("ProtocolDetection", ...)     // TLS/Plain 协议探测
pipeline.addLast("NettyEncoder", ...)          // RemotingCommand 编码
pipeline.addLast("NettyDecoder", ...)          // RemotingCommand 解码
pipeline.addLast("NettyServerHandler", ...)    // 请求分发
```

### 21.2 Client Pipeline 组成

```java
// remoting/netty/NettyRemotingClient.java
pipeline.addLast("IdleStateHandler", ...)
pipeline.addLast("NettyEncoder", ...)
pipeline.addLast("NettyDecoder", ...)
pipeline.addLast("NettyClientHandler", ...)
```

### 21.3 请求分发（NettyRemotingAbstract）

```java
processRequestCommand(ctx, cmd) {
    // 1. 查找 Processor
    Pair<NettyRequestProcessor, ExecutorService> pair = processorTable.get(cmd.getCode());

    if (pair == null) {
        // 使用 defaultProcessor
    }

    // 2. 拒绝策略
    if (pair.getObject1().rejectRequest()) {
        // 快速失败 (Broker 繁忙/Slave)
        writeResponse(SYSTEM_BUSY);
        return;
    }

    // 3. 提交到业务线程池
    pair.getObject2().submit(new RequestTask(processor, ctx, cmd));
}
```

### 21.4 连接管理

| 组件 | 职责 |
|------|------|
| `ChannelEventListener` | CONNECT/CLOSE/IDLE/EXCEPTION 事件 |
| `BrokerHousekeepingService` | Broker/NS 端 Channel 断开 → 清理注册信息 |
| `ClientRemotingProcessor` | Client 端处理 Broker 回查请求 |
| `TableChannelEventListener` | Consumer/Producer 连接表维护 |

**Consumer 心跳数据结构**（Broker 端 `ConsumerManager`）：

```java
ConcurrentMap<String/* group */, ConsumerGroupInfo> consumerTable;

ConsumerGroupInfo {
    SubscriptionGroupConfig groupConfig;
    ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable;
    ConcurrentMap<String, SubscriptionData> subscribeTable;
}
```

Rebalance 时 `findConsumerIdList()` 从 `consumerTable` 获取 ConsumerGroup 全部 clientId。

---

## 二十二、BrokerOuterAPI 与外部交互

`BrokerOuterAPI` 是 Broker 对外通信的统一出口：

### 22.1 与 NameServer 交互

| 方法 | RequestCode | 说明 |
|------|-------------|------|
| `registerBrokerAll()` | REGISTER_BROKER (103) | 并行注册所有 NS |
| `unregisterBroker()` | UNREGISTER_BROKER (104) | 注销 |
| `sendHeartbeat()` | BROKER_HEARTBEAT (904) | 心跳 |
| `getBrokerMemberGroup()` | GET_BROKER_MEMBER_GROUP (901) | 获取 Broker 成员 |

```java
// registerBrokerAll 并行 fan-out
CountDownLatch latch = new CountDownLatch(nameServerAddressList.size());
for (String namesrvAddr : nameServerAddressList) {
    executor.submit(() -> {
        registerBroker(namesrvAddr, ...);
        latch.countDown();
    });
}
latch.await(timeout);
```

### 22.2 与 Controller 交互

| 方法 | 说明 |
|------|------|
| `registerBrokerToController()` | Broker 向 Controller 注册 |
| `electMaster()` | 请求选主 |
| `getReplicaInfo()` | 获取副本信息 |
| `alterSyncStateSet()` | 修改同步副本集 |

### 22.3 Broker 间交互

| 方法 | 说明 |
|------|------|
| `lockBatchMQ()` | 顺序消费队列锁 |
| `unlockBatchMQ()` | 释放队列锁 |
| `syncBrokerMetadata()` | 同步 Broker 元数据 |

---

## 二十三、Compaction Topic 与 KV 存储

### 23.1 Compaction Topic

适用于 Key-Value 语义场景（如 __change_message、配置同步）：

```
cleanupPolicy = COMPACTION
  → CommitLogDispatcherCompaction 构建 CompactionLog
  → CompactionStore 维护 Key → 最新 Value 映射
  → getMessage() 路由到 compactionStore.getMessage()
  → 相同 Key 只保留最新一条消息
```

### 23.2 Compaction 工作原理

```
写入:
  消息带 PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX (Key)
  → CommitLog 正常写入
  → CompactionStore 异步合并: Key → latestOffset

读取:
  getMessage(offset) → CompactionStore 按 Key 去重后返回
  每条 Key 只返回最新版本
```

### 23.3 KV Topic（RocksDB 后端）

5.x 支持 RocksDB 作为 ConsumeQueue 和 Message 的存储后端：

- `RocksDBConsumeQueue`：CQ 索引存 RocksDB
- `MessageRocksDBStorage`：消息体存 RocksDB
- 适用于超大规模 Topic/Queue 场景，减少文件句柄数

---

## 二十四、典型故障场景分析

### 24.1 消息丢失

| 场景 | 原因 | 源码保护 |
|------|------|---------|
| Producer 发送失败 | 网络/Broker 拒绝 | Sync 重试 + MQFaultStrategy |
| Broker 宕机 | 未刷盘消息 | SYNC_FLUSH + SYNC_MASTER |
| Consumer 消费失败 | 业务异常 | sendMessageBack → %RETRY% |
| Rebalance 丢消息 | 队列转移 | Pop 模式 PopCheckPoint 保护 |
| 事务悬挂 | 本地事务未提交 | Broker 回查 (15次) → Rollback |

### 24.2 消息重复

| 场景 | 原因 | 应对 |
|------|------|------|
| Producer 重试 | 网络超时但 Broker 已写入 | 业务幂等 / 唯一键去重 |
| Consumer Rebalance | 队列转移时 offset 不一致 | Pop 模式 / 业务幂等 |
| 消费失败重试 | sendMessageBack | 正常语义，业务幂等 |

### 24.3 消息堆积

```
排查路径:
  1. Consumer 消费速度 < Producer 发送速度
  2. 检查 consumeThreadMin/Max 是否足够
  3. 检查 Consumer 实例数 (Rebalance 分配)
  4. 检查是否有消费阻塞 (CONSUME_SUCCESS 但慢)
  5. Broker getMessage 是否有 Filter 大量不匹配
  6. 磁盘 IO 是否成为瓶颈 (CommitLog 读)
```

### 24.4 Broker 不可用

```
Producer 侧:
  MQFaultStrategy 延迟隔离故障 Broker
  自动选择其他 Broker 的队列

Consumer 侧:
  Rebalance 重新分配队列到存活 Consumer
  NameServer 路由刷新移除下线 Broker

HA 场景:
  Controller electMaster → ReplicasManager.changeToMaster
  AutoSwitchHAService 切换角色
  新 Master 继续服务
```

### 24.5 磁盘满

```
CleanCommitLogService:
  diskSpaceWarningLevelRatio → 日志告警
  diskSpaceCleanForciblyRatio → 强制删除过期文件
  diskMaxUsedSpaceRatio → 拒绝写入 (SendMessageProcessor.rejectRequest)

运维:
  tieredStorage  offload 冷数据
  扩容磁盘 / 减少 fileReservedTime
```

---

## 二十五、刷盘引擎与 TransientStorePool

### 25.1 DefaultFlushManager 架构

`CommitLog.DefaultFlushManager` 根据刷盘模式组合不同后台服务：

| 组件 | 同步刷盘 | 异步刷盘 | TransientStorePool 开启时 |
|------|---------|---------|--------------------------|
| flushCommitLogService | `GroupCommitService` | `FlushRealTimeService` | 同左 |
| commitRealTimeService | 不启动 | 不启动 | `CommitRealTimeService`（额外） |

```java
// CommitLog.DefaultFlushManager.handleDiskFlush()
if (flushDiskType == SYNC_FLUSH) {
    GroupCommitRequest request = new GroupCommitRequest(wroteOffset + wroteBytes, syncFlushTimeout);
    groupCommitService.putRequest(request);
    // 阻塞等待 flushOkFuture → PUT_OK / FLUSH_DISK_TIMEOUT
} else {
    // 异步: wakeup flushCommitLogService 或 commitRealTimeService
    if (transientStorePoolEnable)
        commitRealTimeService.wakeup();  // DirectBuffer → FileChannel
    else
        flushCommitLogService.wakeup();  // Mmap → 磁盘
}
```

### 25.2 三种写入-刷盘路径对比

```
路径 A: 普通 Mmap 异步刷盘 (默认)
  Producer → putMessageLock → MappedByteBuffer.append
  → PageCache → FlushRealTimeService 定时刷盘
  → 立即返回 ACK

路径 B: 同步刷盘
  Producer → MappedByteBuffer.append
  → GroupCommitService 等待 fsync 完成
  → GroupTransferService 等待 Slave 同步 (SYNC_MASTER)
  → 返回 ACK

路径 C: TransientStorePool 异步刷盘
  Producer → borrowBuffer(DirectByteBuffer) → 写入堆外内存
  → CommitRealTimeService: DirectBuffer → FileChannel.write
  → FlushRealTimeService: FileChannel → 磁盘
  → 立即返回 ACK (写入延迟最低)
```

### 25.3 TransientStorePool 原理

```java
// store/TransientStorePool.java
init() {
    for (i = 0; i < poolSize; i++) {
        ByteBuffer buf = ByteBuffer.allocateDirect(fileSize);  // 堆外内存
        LibC.mlock(address, fileSize);  // 锁定内存页，防止 swap
        availableBuffers.offer(buf);
    }
}

borrowBuffer() → poll from pool
returnBuffer() → 归还 pool
```

| 配置 | 默认值 | 说明 |
|------|--------|------|
| transientStorePoolSize | 5 | 堆外 Buffer 数量 |
| transientStorePoolEnable | false | 是否启用 |
| fastFailIfNoBufferInStorePool | false | Pool 耗尽时快速失败 |

**优势**：写入 DirectBuffer 不经过 PageCache，避免写入污染读缓存；`mlock` 防止 OS swap 导致延迟抖动。

**代价**：额外内存占用 = poolSize × mappedFileSizeCommitLog（约 5GB）。

### 25.4 GroupTransferService 与 HA 联动

同步刷盘 + 同步复制时的完整等待链：

```
putMessage → append CommitLog
  → GroupCommitService (等待磁盘 fsync)
  → GroupTransferService (等待 push2SlaveMaxOffset >= 当前 offset)
  → 返回 Producer ACK

任一环节超时:
  FLUSH_DISK_TIMEOUT (10) 或 FLUSH_SLAVE_TIMEOUT (12)
  Producer 可配置 retryAnotherBrokerWhenNotStoreOK 切换 Broker 重试
```

---

## 二十六、MappedFile 预分配与文件生命周期

### 26.1 AllocateMappedFileService

后台 `ServiceThread` 提前创建 MappedFile，避免写入时同步创建文件的延迟：

```java
// store/AllocateMappedFileService.java
putRequestAndWaitMappedFile(nextFilePath, fileSize) {
    requestTable.put(path, AllocateRequest);
    requestQueue.offer(request);  // PriorityBlockingQueue
    // 等待 mappedFile 创建完成 (最多 5s)
    return request.getMappedFile();
}

run() {
    while (!stopped) {
        request = requestQueue.take();
        mappedFile = new DefaultMappedFile(path, fileSize);
        mappedFile.warmMappedFile(...);  // 预热: 逐页写入触发 page fault
        request.mappedFile = mappedFile;
        request.countDownLatch.countDown();
    }
}
```

**预热（warmMappedFile）**：对新 MappedFile 逐页写入，提前触发 Page Fault，避免首次写入时的延迟尖刺。

### 26.2 MappedFile 生命周期

```
创建 → 写入 (wrotePosition 递增) → isFull() → rollNextFile
  → 只读等待过期 → CleanCommitLogService 删除
  → unmap / destroy
```

| 阶段 | 关键字段 | 说明 |
|------|---------|------|
| 写入中 | wrotePosition | 当前写入位置 |
| 写满 | isFull() = true | wrotePosition >= fileSize - blankSize |
| 可读 | getReadPosition() | 供 ReputMessageService / getMessage 读取 |
| 过期 | storeTimestamp + fileReservedTime | CleanCommitLogService 判定 |

### 26.3 多路径存储（MultiPath）

CommitLog 支持多磁盘路径（`storePathCommitLog` 用 `:` 分隔）：

```java
if (storePath.contains(MultiPathSplit)) {
    mappedFileQueue = new MultiPathMappedFileQueue(...);
    // 轮询写入不同磁盘，分散 IO 压力
}
```

---

## 二十七、Proxy gRPC 全链路剖析

### 27.1 启动与模式选择

```java
// proxy/ProxyStartup.java
createMessagingProcessor() {
    if (ProxyMode.isClusterMode())
        return DefaultMessagingProcessor.createForClusterMode();
        // ServiceManagerFactory → 远程 RPC 访问 Broker

    if (ProxyMode.isLocalMode())
        brokerController = BrokerStartup.createBrokerController(...);
        brokerController.start();
        return DefaultMessagingProcessor.createForLocalMode(brokerController);
        // 同进程，直接调用 BrokerController
}
```

### 27.2 gRPC 服务层

```java
// proxy/grpc/v2/GrpcMessagingApplication.java
extends MessagingServiceGrpc.MessagingServiceImplBase

// gRPC 方法 → MessagingProcessor 映射:
sendMessage(SendMessageRequest)     → messagingProcessor.sendMessage()
receiveMessage(ReceiveMessageRequest) → popMessage() / pullMessage()
ackMessage(AckMessageRequest)       → ackMessage()
queryRoute(QueryRouteRequest)       → getTopicRouteDataForProxy()
endTransaction(EndTransactionRequest) → endTransaction()
queryAssignment(QueryAssignmentRequest) → queryAssignment()
heartbeat(HeartbeatRequest)         → heartbeat()
```

**线程池隔离**（Proxy 端）：

| 线程池 | 用途 |
|--------|------|
| routeThreadPoolExecutor | 路由查询 |
| producerThreadPoolExecutor | 消息发送 |
| consumerThreadPoolExecutor | 消息接收/ACK |
| clientManagerThreadPoolExecutor | 心跳/客户端管理 |
| transactionThreadPoolExecutor | 事务 |

### 27.3 协议转换链路

```
Client (gRPC/Protobuf)
  │
  ▼
GrpcMessagingApplication
  → RequestPipeline (ContextInit → Authentication → Authorization)
  → GrpcMessagingActivity
  → MessagingProcessor (DefaultMessagingProcessor)
  │
  ├─ Cluster 模式:
  │    → ProxyRelayService / MessageService
  │    → MQClientAPIImpl (Remoting) → Broker
  │
  └─ Local 模式:
       → 直接调用 BrokerController 的 Processor 逻辑
       → 无网络开销
```

### 27.4 Pop 消费在 Proxy 中的实现

Proxy 原生基于 Pop 模式，`ReceiveMessageRequest` 默认走 Pop：

```java
// MessagingProcessor.popMessage()
→ ReceiptHandle 封装 (messageId + offset + invisibleTime + ...)
→ Client ACK 时携带 ReceiptHandle
→ Proxy 转发 ACK_MESSAGE 到 Broker
```

**ReceiptHandle**：Proxy 层对 Pop extraInfo 的封装，屏蔽底层 Broker 协议细节，提供统一的多语言 API。

### 27.5 Proxy 双协议支持

| 协议 | 端口 | 说明 |
|------|------|------|
| gRPC | grpcServerPort (默认 8081) | 新版多语言客户端 |
| Remoting | remotingListenPort | 兼容旧版 Remoting 客户端 |
| HTTP | 规划中 | RIP 支持 |

---

## 二十八、Controller 选主与 ReplicasManager

### 28.1 Controller 双实现

| 实现 | 类 | 共识算法 |
|------|-----|---------|
| DLedger | `DLedgerController` | DLedger Raft |
| JRaft | `JRaftController` | SOFAJRaft |

通过 `controllerConfig.controllerType` 选择，选主事件通过 Raft AppendEntries 持久化。

### 28.2 DefaultElectPolicy 选主策略

```java
// controller/elect/impl/DefaultElectPolicy.java
elect(clusterName, brokerName, syncStateSet, allReplicas, oldMaster, preferBrokerId) {

    // 1. 优先在 syncStateSet 中选
    newMaster = tryElect(syncStateSet);

    // 2. syncStateSet 无可用 → 扩大到 allReplicas
    if (newMaster == null)
        newMaster = tryElect(allReplicas);

    // tryElect 内部:
    //   a. validPredicate 过滤存活 Broker
    //   b. oldMaster 仍有效 → 继续担任
    //   c. preferBrokerId 有效 → 优先选择
    //   d. 按 (epoch DESC, maxOffset DESC, electionPriority ASC) 排序选最优
}
```

**排序规则**：epoch 最大者优先 → epoch 相同则 CommitLog offset 最大者优先 → 仍相同则 electionPriority 最小者优先。

### 28.3 ReplicasManager 角色切换

#### changeToMaster 流程

```
Controller 通知: NOTIFY_BROKER_ROLE_CHANGED (newMasterBrokerId=self)
  │
  ▼
ReplicasManager.changeToMaster(newMasterEpoch, syncStateSetEpoch, syncStateSet)
  ├─ changeSyncStateSet(newSyncStateSet)       // 更新同步副本集
  ├─ handleSlaveSynchronize(BrokerRole.SYNC_MASTER)
  ├─ haService.changeToMaster(newMasterEpoch)  // HA 层切 Master
  ├─ brokerConfig.setBrokerId(MASTER_ID=0)
  ├─ messageStoreConfig.setBrokerRole(SYNC_MASTER)
  ├─ changeSpecialServiceStatus(true)          // 启动 PopRevive/Schedule 等
  ├─ setFenced(false)                          // 允许写入
  └─ registerBrokerWhenRoleChange()            // 重新注册 NS
```

#### changeToSlave 流程

```
ReplicasManager.changeToSlave(newMasterAddress, newMasterEpoch, newMasterBrokerId)
  ├─ stopCheckSyncStateSet()
  ├─ messageStoreConfig.setBrokerRole(SLAVE)
  ├─ changeSpecialServiceStatus(false)          // 停止 Master 专属服务
  ├─ haService.changeToSlave(newMasterAddress, newMasterEpoch)
  ├─ setFenced(true)                           // 禁止写入
  └─ registerBrokerWhenRoleChange()
```

### 28.4 SyncStateSet 管理

```
Master 定期 checkSyncStateSet():
  检查各副本同步进度 (haConnectionRuntimeInfo)
  滞后过多 → 移出 SyncStateSet
  新同步完成 → 加入 SyncStateSet
  变更 → alterSyncStateSet() → Controller 持久化

写入约束:
  inSyncReplicas >= minInSyncReplicas 才允许写入
  否则返回 IN_SYNC_REPLICAS_NOT_ENOUGH
```

### 28.5 完整选主时序

```
1. Master Broker 宕机
2. Controller 检测心跳超时 (RaftBrokerHeartBeatManager)
3. electMaster(brokerName) → DefaultElectPolicy 选新 Master
4. Raft 持久化选主事件
5. NotifyService → NOTIFY_BROKER_ROLE_CHANGED → 所有副本
6. 新 Master: ReplicasManager.changeToMaster()
7. 旧 Slave: ReplicasManager.changeToSlave()
8. 新 Master 重新 registerBrokerAll() → NS 路由更新
9. Client 下次路由刷新 → 感知新 Master
```

---

## 二十九、冷数据流控 / Reply / Recall

### 29.1 冷数据流控（ColdDataPullRequestHoldService）

TieredStore 或历史消息读取时，冷数据从远程存储加载耗时较长：

```java
// broker/coldctr/ColdDataPullRequestHoldService.java
suspendColdDataReadRequest(pullRequest) {
    // 首次读冷数据 → 放入 coldHoldQueue
    pullRequestColdHoldQueue.offer(pullRequest);
}

run() {
    // 每 5s 扫描
    if (now - pullRequest.suspendTimestamp >= coldHoldTimeoutMillis) {  // 默认 3s
        // 冷数据已加载到 PageCache → 重新执行 Pull
        executeRequestPullLater(pullRequest, 0);
    }
}
```

**作用**：避免冷数据读取阻塞 Pull 线程池，首次请求挂起等待数据预热，后续读取走 PageCache。

### 29.2 请求-回复模式（ReplyMessageProcessor）

RocketMQ 支持类 RPC 的请求-回复语义：

```
Consumer 设置 Reply 属性:
  msg.putUserProperty(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, true)
  msg.putUserProperty(MessageConst.PROPERTY_MESSAGE_REPLY_TO_TOPIC, replyTopic)
  msg.putUserProperty(MessageConst.PROPERTY_MESSAGE_REPLY_TO, replyAddress)

Consumer 消费后:
  → SEND_REPLY_MESSAGE (324) → ReplyMessageProcessor
  → 读取原消息中的 replyTopic / replyAddress
  → 构建回复消息 → 发送到指定 Topic
  → Producer 端 DefaultMQProducerImpl 监听 replyTopic 接收回复
```

**应用场景**：同步请求-响应模式，如微服务 RPC 替代方案。

### 29.3 消息召回（RecallMessageProcessor）

定时/延迟消息的"撤回"能力：

```java
// broker/processor/RecallMessageProcessor.java
// RequestCode.RECALL_MESSAGE

processRequest() {
    1. 解析 RecallMessageRequestHeader (topic, queueId, timestamp, offset)
    2. 从 TimerMessageStore / ScheduleMessageService 定位目标消息
    3. 写入召回标记消息 (RECALL_MESSAGE_TAG)
    4. TimerMessageStore 扫描时跳过已召回消息
}
```

**约束**：仅支持尚未投递的定时/延迟消息，已投递的消息无法召回。

---

## 三十、消息轨迹 Trace

### 30.1 架构

```
Producer/Consumer
  → SendMessageTraceHookImpl / ConsumeMessageTraceHookImpl
  → AsyncTraceDispatcher (异步队列 + 批量发送)
  → 内部 Trace Producer
  → Topic: rmq_sys_TRACE_DATA_{group}
  → Trace Consumer (mqadmin trace query)
```

### 30.2 AsyncTraceDispatcher

```java
// client/trace/AsyncTraceDispatcher.java
// 独立线程池 + 有界队列，不阻塞业务线程

traceContextQueue (ArrayBlockingQueue)
  → TraceDispatcherThread 批量取出
  → 封装 TraceTransferBean (Pub/Sub/EndTransaction 等)
  → 内部 DefaultMQProducer 发送到 Trace Topic
```

**Trace 数据类型**：

| 类型 | 触发点 | 内容 |
|------|--------|------|
| Pub | 发送前/后 | topic, msgId, offset, storeHost, bodyLength |
| SubBefore | 消费前 | topic, msgId, group, retryTimes |
| SubAfter | 消费后 | 消费结果, costTime |
| EndTransaction | 事务结束 | 事务状态 |

### 30.3 启用方式

```java
DefaultMQProducer producer = new DefaultMQProducer("group");
producer.setEnableTrace(true);  // 开启 Trace

DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group");
consumer.setEnableTrace(true);
```

Trace Topic 默认 1 队列，Trace 消息不影响业务 Topic 性能（独立异步发送）。

---

## 三十一、ResponseCode 与异常语义

### 31.1 发送相关

| Code | 常量 | 含义 | Producer 行为 |
|------|------|------|--------------|
| 0 | SUCCESS | 成功 | 返回 SendResult |
| 10 | FLUSH_DISK_TIMEOUT | 刷盘超时 | 可重试 (retryResponseCodes) |
| 11 | SLAVE_NOT_AVAILABLE | Slave 不可用 | 可重试 |
| 12 | FLUSH_SLAVE_TIMEOUT | Slave 同步超时 | 可重试 |
| 13 | MESSAGE_ILLEGAL | 消息非法 | 不重试 |
| 14 | SERVICE_NOT_AVAILABLE | 服务不可用 | 重试其他 Broker |
| 16 | NO_PERMISSION | 无权限 | 不重试 |
| 17 | TOPIC_NOT_EXIST | Topic 不存在 | 自动创建 (isAutoCreateTopicEnable) |
| 29 | INVALID_PARAMETER | 参数无效 | 不重试 |

### 31.2 拉取相关

| Code | 常量 | 含义 | Consumer 行为 |
|------|------|------|--------------|
| 19 | PULL_NOT_FOUND | 无消息 | 长轮询挂起 |
| 20 | PULL_RETRY_IMMEDIATELY | 立即重试 | 立即重新 Pull |
| 21 | PULL_OFFSET_MOVED | Offset 已过期 | 修正 offset / rebalance |
| 206 | CONSUMER_NOT_ONLINE | Consumer 不在线 | rebalance |
| 208 | NO_MESSAGE | Pop 无消息 | 长轮询 |
| 209 | POLLING_FULL | 长轮询队列满 | 短轮询 fallback |

### 31.3 事务相关

| Code | 常量 | 含义 |
|------|------|------|
| 200 | TRANSACTION_SHOULD_COMMIT | 应提交 |
| 201 | TRANSACTION_SHOULD_ROLLBACK | 应回滚 |
| 202 | TRANSACTION_STATE_UNKNOW | 状态未知 → 触发回查 |

### 31.4 系统相关

| Code | 常量 | 含义 |
|------|------|------|
| 1 | SYSTEM_ERROR | 系统错误 |
| 2 | SYSTEM_BUSY | 系统繁忙 (rejectRequest) |
| 3 | REQUEST_CODE_NOT_SUPPORTED | 不支持的操作码 |
| 205 | NOT_IN_CURRENT_UNIT | 单元化路由不匹配 |

---

## 三十二、可观测性 OpenTelemetry Metrics

### 32.1 三层 Metrics 体系

RocketMQ 5.x 基于 OpenTelemetry 统一指标：

| 层级 | 类 | 关键指标 |
|------|-----|---------|
| Store | `DefaultStoreMetricsManager` | commitlog_put_latency, dispatch_behind, flush_latency |
| Broker | `BrokerMetricsManager` | send_msg_latency, pull_msg_latency, pop_msg_latency |
| Proxy | `ProxyMetricsManager` | grpc_request_latency, relay_latency |
| Controller | `ControllerMetricsManager` | elect_master_count, sync_state_set_size |

### 32.2 Store 层指标

```
Histogram: rocketmq_store_commitlog_put_latency (ms)
Histogram: rocketmq_store_flush_latency (ms)
Gauge:     rocketmq_store_dispatch_behind (条)
Gauge:     rocketmq_store_commitlog_max_offset
Counter:   rocketmq_store_messages_in_total
Counter:   rocketmq_store_messages_out_total
```

### 32.3 Broker 层指标

```
Histogram: rocketmq_broker_send_msg_latency (ms)
Histogram: rocketmq_broker_pull_msg_latency (ms)
Histogram: rocketmq_broker_pop_msg_latency (ms)
Counter:   rocketmq_broker_send_msg_total
Counter:   rocketmq_broker_pull_msg_total
Counter:   rocketmq_broker_pop_msg_total
Counter:   rocketmq_broker_send_back_msg_total
Gauge:     rocketmq_broker_consumer_connection_count
```

### 32.4 TieredStore 指标

```
Histogram: rocketmq_tiered_store_api_latency
Histogram: rocketmq_tiered_store_provider_upload_bytes
Gauge:     rocketmq_tiered_store_dispatch_behind
Counter:   rocketmq_tiered_store_get_message_fallback_total
Gauge:     rocketmq_tiered_store_read_ahead_cache_bytes
```

### 32.5 启用方式

```properties
# broker.conf
metricsExporterType=OTLP                    # 导出类型
otlpEndpoint=http://collector:4317          # OTLP Collector 地址
metricsGrpcExporterInterval=60              # 导出间隔 (秒)
enableMetricsPush=true
```

---

## 三十三、单元化部署（Unit Mode）

### 33.1 概念

单元化 = 多机房/多单元独立部署，每个单元处理特定用户/数据分片：

```
Unit A (北京)          Unit B (上海)
  NS-A + Broker-A        NS-B + Broker-B
  Topic 同名但数据隔离    Topic 同名但数据隔离
```

### 33.2 实现机制

| 机制 | 说明 |
|------|------|
| UnitName | Broker/Client 配置所属单元 |
| `NOT_IN_CURRENT_UNIT (205)` | 跨单元请求被拒绝 |
| UnitTopic / UnitSubTopic | 单元内 Topic 命名空间 |
| `AllocateMessageQueueByMachineRoom` | 同机房/单元优先分配 |

### 33.3 Client 端

```java
clientConfig.setUnitMode(true);
clientConfig.setUnitName("UnitA");
// MQClientAPIImpl 发送请求时携带 UNIT_MODE=true + UNIT_NAME
// NameServer ZoneRouteRPCHook 过滤路由
```

---

## 三十四、Lite Topic 与通知机制

### 34.1 Lite Topic

5.x 轻量级 Topic，适用于 IoT/MoM 海量小 Topic 场景：

- 动态创建/销毁，无需预注册
- 独立的 `LitePullMessageProcessor` / `PopLiteMessageProcessor`
- `LiteSubscriptionDTO` 管理订阅关系
- `LiteUtil` 工具类处理 Lite Topic 命名规范

### 34.2 NotificationProcessor（Pop 通知）

Pop 模式下 Consumer 长轮询等待新消息：

```java
// broker/processor/NotificationProcessor.java
// RequestCode.NOTIFICATION

Consumer 发送 NOTIFICATION 请求 (长轮询)
  → PopLongPollingService.suspendPopRequest()
  → 新消息 Pop 时 notifyMessageArriving() 唤醒
  → 返回通知 (无需 Client 主动 Pop)
```

与 Classic Pull 的 `PullRequestHoldService` 类似，但面向 Pop 消费模型。

---

## 三十五、DLedger 多副本 CommitLog

### 35.1 与传统 Master-Slave 的区别

| 维度 | Master-Slave | DLedger |
|------|-------------|---------|
| 共识 | 主从异步/同步复制 | Raft 多数派确认 |
| 选主 | 手动 / Controller | DLedger 内置 Raft 选主 |
| CommitLog | 单节点写入 | 多节点 Raft AppendEntries |
| 数据一致性 | 可能丢最后几条 | Raft 保证不丢已确认数据 |

### 35.2 DLedgerCommitLog 写入

```java
// store/dledger/DLedgerCommitLog.java
asyncPutMessage(msg) {
    AppendEntryRequest request = buildAppendEntryRequest(msg);
    AppendFuture<AppendEntryResponse> future = dLedgerServer.handleAppend(request);
    // Raft 多数派确认后返回
    return future.thenApply(response → PutMessageResult);
}

// dividedCommitlogOffset 分隔旧 CommitLog 与 DLedger CommitLog
// 升级 DLedger 时保留旧数据
```

### 35.3 DLedger 配置

```properties
enableDLegerCommitLog=true
dLegerGroup=broker-g0
dLegerSelfId=n0
dLegerPeers=n0-127.0.0.1:40911;n1-127.0.0.1:40912;n2-127.0.0.1:40913
```

---

## 附录 E：核心类继承/实现关系

```
RemotingServer
  └── NettyRemotingServer
RemotingClient
  └── NettyRemotingClient

MessageStore (interface)
  └── DefaultMessageStore
        └── TieredMessageStore (plugin)
        └── RocksDBMessageStore (plugin)

CommitLog
  └── DLedgerCommitLog

NettyRequestProcessor (interface)
  ├── SendMessageProcessor extends AbstractSendMessageProcessor
  ├── PullMessageProcessor
  ├── PopMessageProcessor
  ├── AckMessageProcessor
  ├── EndTransactionProcessor
  └── AdminBrokerProcessor

MQConsumerInner (interface)
  ├── DefaultMQPushConsumerImpl
  ├── DefaultLitePullConsumerImpl
  └── DefaultMQPullConsumerImpl (deprecated)

Controller (interface)
  ├── DLedgerController
  └── JRaftController

MessagingProcessor (interface)
  └── DefaultMessagingProcessor
        ├── createForClusterMode()
        └── createForLocalMode(BrokerController)
```

---

## 附录 F：Broker 定时任务一览

| 任务 | 间隔 | 类/方法 | 作用 |
|------|------|---------|------|
| 注册 NameServer | 10~60s | BrokerController.registerBrokerAll | 路由注册 |
| 发送心跳 NS | brokerHeartbeatInterval | scheduleSendHeartbeat | 保活 |
| 扫描过期 Broker | 5s | RouteInfoManager.scanNotActiveBroker | NS 清理 |
| 清理 CommitLog | 10s | CleanCommitLogService | 过期文件删除 |
| 清理 ConsumeQueue | 10s | CleanConsumeQueueService | CQ 文件删除 |
| 持久化 ConsumerOffset | 5s | ConsumerOffsetManager | offset 刷盘 |
| 事务回查 | 60s | TransactionalMessageCheckService | 悬挂事务 |
| 延迟消息投递 | 100ms | ScheduleMessageService | 扫描到期消息 |
| Timer 时间轮 | 精度 ms | TimerMessageStore | 定时消息 |
| Rebalance | 20s | RebalanceService | 消费队列分配 |
| 路由刷新 | 30s | MQClientInstance | Client 路由 |
| PopRevive 扫描 | 持续 | PopReviveService | 超时重投递 |
| SyncStateSet 检查 | 可配 | ReplicasManager | Controller 副本集 |
| 打印水位 | 1s | NamesrvController | 队列积压监控 |

---

## 三十六、Proxy ServiceManager 服务分层

Proxy 通过 `ServiceManagerFactory` 按部署模式注入不同的服务实现，是计算存储分离的核心抽象。

### 36.1 工厂与两种实现

```java
// proxy/service/ServiceManagerFactory.java
createForClusterMode()  → ClusterServiceManager  // 远程 RPC
createForLocalMode(brokerController) → LocalServiceManager  // 同进程
```

### 36.2 ClusterServiceManager 组件

| 接口 | 实现 | 职责 |
|------|------|------|
| `TopicRouteService` | ClusterTopicRouteService | 从 NS 拉路由 |
| `MessageService` | ClusterMessageService | Remoting 发/收消息 |
| `ProxyRelayService` | ClusterProxyRelayService | 转发到 Broker |
| `MetadataService` | ClusterMetadataService | Topic/Group 元数据 |
| `TransactionService` | ClusterTransactionService | 事务 RPC |
| `AdminService` | DefaultAdminService | 管理操作 |
| `ProducerManager` | (内置) | Proxy 侧 Producer 连接表 |
| `ClusterConsumerManager` | (内置) | Proxy 侧 Consumer 连接表 |

Cluster 模式维护 **3~4 个 MQClientAPIFactory**（messaging / operation / transaction / liteSubscription），分别对应不同 RPC 场景的长连接池。

### 36.3 LocalServiceManager 组件

| 接口 | 实现 | 职责 |
|------|------|------|
| `TopicRouteService` | LocalTopicRouteService | 读 Broker 本地 TopicConfig |
| `MessageService` | LocalMessageService | 直接调 Broker Processor |
| `ProxyRelayService` | LocalProxyRelayService | 进程内 relay |
| `MetadataService` | LocalMetadataService | Broker 本地元数据 |
| `TransactionService` | LocalTransactionService | Broker 本地事务 |

Local 模式复用 `BrokerController` 的 `ProducerManager` / `ConsumerManager`，无额外网络 hop。

### 36.4 MessagingProcessor 统一入口

```java
// DefaultMessagingProcessor — 对 gRPC 和 Remoting 提供统一 API
sendMessage(ctx, queueSelector, group, sysFlag, msgs)
receiveMessage / popMessage(ctx, ...)
ackMessage(ctx, receiptHandle)
queryRoute(ctx, topic)
endTransaction(ctx, ...)
queryAssignment(ctx, group, topic)  // Pop Rebalance
```

无论 Cluster/Local，上层 `GrpcMessagingApplication` 和 `RemotingProtocolServer` 都只依赖 `MessagingProcessor` 接口。

---

## 三十七、Proxy Remoting 协议层

5.x Proxy 除 gRPC 外还提供 **Remoting 协议兼容层**，使旧版 Client 可通过 Proxy 访问集群。

### 37.1 RemotingProtocolServer

```java
// proxy/remoting/RemotingProtocolServer.java
// 独立 NettyRemotingServer，注册 Activity 处理器:

GetTopicRouteActivity      → GET_ROUTEINFO_BY_TOPIC
SendMessageActivity        → SEND_MESSAGE / V2 / BATCH
PullMessageActivity        → PULL_MESSAGE
PopMessageActivity         → POP_MESSAGE
AckMessageActivity         → ACK_MESSAGE / BATCH_ACK
TransactionActivity        → END_TRANSACTION / CHECK_TRANSACTION
ConsumerManagerActivity    → GET_CONSUMER_LIST_BY_GROUP / heartbeat
RecallMessageActivity      → RECALL_MESSAGE
ChangeInvisibleTimeActivity → CHANGE_MESSAGE_INVISIBLETIME
```

每个 Activity 将 Remoting 请求转换为 `MessagingProcessor` 调用，响应再编码为 `RemotingCommand`。

### 37.2 Activity 模式

```
RemotingCommand 入站
  → RequestPipeline (ContextInit → Auth → Authz)
  → XxxActivity.handle(ctx, request)
  → MessagingProcessor.xxx()
  → CompletableFuture<Response>
  → 编码 RemotingCommand 出站
```

与 Broker 端 Processor 模式对称，但多了一层 Proxy 上下文（`ProxyContext`：clientId、channel、namespace）。

### 37.3 RemotingChannelManager

Proxy 维护 Client ↔ Proxy 的 Channel 映射，Cluster 模式下 Proxy 作为 Client 连接 Broker，Local 模式下直接委托 Broker Channel。

---

## 三十八、JRaft Controller 状态机

除 DLedger 实现外，RocketMQ 5.x 提供基于 **SOFAJRaft** 的 Controller 实现。

### 38.1 JRaftControllerStateMachine

```java
// controller/impl/JRaftControllerStateMachine.java
implements StateMachine {

onApply(Iterator iter) {
    // 从 Raft Log 顺序应用事件
    while (iter.hasNext()) {
        ControllerClosure closure = iter.done();
        RemotingCommand command = closure.getCommand();
        switch (command.getCode()) {
            case CONTROLLER_ELECT_MASTER:
            case CONTROLLER_ALTER_SYNC_STATE_SET:
            case REGISTER_BROKER_TO_CONTROLLER:
            case APPLY_BROKER_ID:
            // ...
        }
        RaftReplicasInfoManager.apply(event);
    }
}

onLeaderStart(term) {
    // 成为 Leader → 触发选主回调 / 扫描非活跃 Broker
}

onSnapshotSave / onSnapshotLoad {
    // 快照持久化 ReplicasInfoManager 状态
}
```

### 38.2 DLedger vs JRaft 对比

| 维度 | DLedgerController | JRaftController |
|------|-------------------|-----------------|
| Raft 实现 | OpenMessaging DLedger | SOFAJRaft |
| 状态机 | DLedger 内置 + EventMessage | JRaftControllerStateMachine |
| 快照 | DLedger Snapshot | JRaft Snapshot |
| 生态 | RocketMQ 原生 | Ant Group 生产级 Raft |
| 配置 | controllerType=DLedger | controllerType=JRaft |

两者共享 `DefaultElectPolicy`、`NotifyService`、`RaftBrokerHeartBeatManager` 等上层逻辑。

### 38.3 Controller 事件类型

| 事件 | 触发 | 效果 |
|------|------|------|
| RegisterBroker | Broker 注册 | 记录副本信息 |
| ElectMaster | Master 宕机 | 选新 Master + 通知 |
| AlterSyncStateSet | Master 上报 | 更新同步副本集 |
| BrokerHeartbeat | 定时心跳 | 更新 BrokerLiveInfo |
| CleanBrokerData | Admin 清理 | 移除过期副本数据 |

---

## 三十九、RocksDB 存储后端

### 39.1 RocksDBMessageStore

```java
// store/RocksDBMessageStore.java
extends DefaultMessageStore {
    createConsumeQueueStore() {
        return new RocksDBConsumeQueueStore(this);  // CQ 存 RocksDB
    }
}
```

启用方式：

```properties
messageStorePlugIn=org.apache.rocketmq.store.RocksDBMessageStore
```

### 39.2 RocksDB 组件矩阵

| 组件 | 类 | 存储内容 |
|------|-----|---------|
| ConsumeQueue | `RocksDBConsumeQueueStore` | CQ 索引 (topic+queueId+offset → phyOffset) |
| Index | `IndexRocksDBStore` | MessageKey 索引 |
| Timer | `TimerMessageRocksDBStore` | 定时消息状态 |
| Transaction | `TransMessageRocksDBStore` | 事务状态 |
| Message | `MessageRocksDBStorage` | 消息体 (可选) |
| Broker Config | `ConfigStorage` (v2) | Topic/Subscription/Offset 配置 |

### 39.3 适用场景

| 场景 | 传统 MappedFile | RocksDB |
|------|----------------|---------|
| Topic/Queue 数量 | 千级 | 十万级+ |
| 文件句柄 | 每 CQ 一个文件 | 统一 RocksDB |
| 随机读 | Mmap 顺序读优 | LSM-Tree 随机读优 |
| 运维复杂度 | 低 | 中（需 RocksDB 调优） |

---

## 四十、BrokerContainer 多 Broker 容器

### 40.1 动机

云原生场景下，单 JVM 运行多个轻量 Broker 实例，共享 Netty 端口和 OS 资源：

```java
// container/BrokerContainer.java
ConcurrentMap<BrokerIdentity, InnerBrokerController> masterBrokerControllers;
ConcurrentMap<BrokerIdentity, InnerBrokerController> slaveBrokerControllers;
ConcurrentMap<BrokerIdentity, InnerBrokerController> dLedgerBrokerControllers;

// 共享:
RemotingServer remotingServer;       // 统一监听端口
RemotingServer fastRemotingServer;
BrokerOuterAPI brokerOuterAPI;       // 共享 NS 连接
```

### 40.2 请求路由

```
Client 请求 → BrokerContainer RemotingServer
  → BrokerContainerProcessor.processRequest()
  → 根据 BrokerIdentity (cluster+brokerName+brokerId) 路由
  → 对应 InnerBrokerController 的 Processor 处理
```

### 40.3 与独立 Broker 的区别

| 维度 | 独立 Broker | BrokerContainer |
|------|------------|-----------------|
| 进程数 | 1 Broker = 1 JVM | N Broker = 1 JVM |
| 端口 | 每 Broker 独立 | 共享端口 |
| 资源 | 独立堆/线程池 | 共享 Netty/部分线程池 |
| 隔离 | 完全 | BrokerIdentity 逻辑隔离 |
| 部署 | 传统 | K8s Sidecar / 高密度 |

---

## 四十一、Static Topic 深度剖析

### 41.1 问题背景

Classic Topic 的 Queue 与 Broker 物理绑定，扩缩容 Broker 会导致 Queue 迁移困难。Static Topic 引入 **逻辑队列** 与 **物理队列** 解耦。

### 41.2 核心数据结构

```java
// remoting/protocol/statictopic/
TopicQueueMappingDetail {
    String topic;
    String scope;           // 映射范围
    List<LogicQueueMappingItem> mappingItemList;
}

LogicQueueMappingItem {
    int globalId;           // 逻辑 QueueId (Client 视角)
    String bname;           // 物理 BrokerName
    int queueId;            // 物理 QueueId
    long logicOffset;       // 逻辑 offset 起始
    long startOffset;       // 物理 offset 起始
    long endOffset;         // 物理 offset 结束 (-1=活跃)
}
```

### 41.3 读写转换

**发送时**（SendMessageProcessor）：

```
Client 发送到 logicQueueId=2
  → TopicQueueMappingManager.buildContext()
  → 找到 leader LogicQueueMappingItem
  → rewriteRequest: logicQueueId → physicalQueueId, physicalBroker
  → 写入物理 Broker 的 CommitLog
  → rewriteResponse: physicalOffset → staticLogicOffset
```

**拉取时**（PullMessageProcessor）：

```
Client pull logicQueueId=2, globalOffset=1000
  → findLogicQueueMappingItem(mappingList, globalOffset)
  → physicalOffset = mappingItem.computePhysicalQueueOffset(1000)
  → 从物理 Broker 读取
  → 响应中转换回 logicOffset
```

### 41.4 TopicQueueMappingManager

```java
// broker/topic/TopicQueueMappingManager.java
ConcurrentMap<String, TopicQueueMappingDetail> topicQueueMappingTable;

updateTopicQueueMapping(newDetail, force, isClean, flush)
pickupTopicRoute() → 附加 mapping 到 TopicRouteData
```

---

## 四十二、RunningFlags 与读写状态控制

`RunningFlags` 是 Store 层的**位图状态机**，控制 Broker 是否可读/可写：

```java
// store/RunningFlags.java
NOT_READABLE_BIT          (bit 0)  → getMessage 拒绝
NOT_WRITEABLE_BIT         (bit 1)  → putMessage 拒绝
WRITE_LOGICS_QUEUE_ERROR  (bit 2)  → CQ 写入异常
WRITE_INDEX_FILE_ERROR    (bit 3)  → Index 写入异常
DISK_FULL_BIT             (bit 4)  → CommitLog 磁盘满
FENCED_BIT                (bit 5)  → Controller 模式写入隔离
LOGIC_DISK_FULL_BIT       (bit 6)  → CQ 磁盘满
```

**典型触发**：

| 事件 | 标志变化 | 效果 |
|------|---------|------|
| 磁盘使用率 > 75% | DISK_FULL → NOT_WRITEABLE | 拒绝写入 |
| CQ 磁盘满 | LOGIC_DISK_FULL | 拒绝写入 |
| ReplicasManager.setFenced(true) | FENCED | Controller 选主期间拒绝写入 |
| CQ 写入失败 | WRITE_LOGICS_QUEUE_ERROR → NOT_READABLE | 拒绝读写 |
| 磁盘恢复 | makeWriteable() | 恢复写入 |

`SendMessageProcessor.rejectRequest()` 和 `getMessage()` 均检查 `runningFlags`。

---

## 四十三、HAConnection 传输协议

### 43.1 连接建立

```
Slave (HAClient)                    Master (AcceptSocketService)
  │                                      │
  │────── TCP Connect (:HA端口) ────────→│
  │                                      │ new DefaultHAConnection
  │←───── 传输 CommitLog 数据 ──────────│ WriteSocketService
  │────── ACK (slaveAckOffset) ────────→│ ReadSocketService
```

### 43.2 传输包格式

```
┌──────────────────┬──────────────┬──────────────────────┐
│   physicOffset   │   bodySize   │   body (CommitLog)   │
│    (8 bytes)     │  (4 bytes)   │   (bodySize bytes)   │
└──────────────────┴──────────────┴──────────────────────┘
         TRANSFER_HEADER_SIZE = 12
```

### 43.3 双工通信

| 服务 | 方向 | 职责 |
|------|------|------|
| WriteSocketService | Master → Slave | 推送 CommitLog 增量 |
| ReadSocketService | Slave → Master | 接收 Slave ACK offset |
| HAClient (Slave) | 双向 | 主动连接 Master，接收数据 + 发送 ACK |

### 43.4 流控

`FlowMonitor` 监控传输速率，Slave 落后过多时 Master 全速推送，Slave 接近时减速，避免网络拥塞。

### 43.5 AutoSwitchHAConnection

Controller 模式下使用 `AutoSwitchHAConnection`，额外传输：

- BrokerEpoch / MasterEpoch
- SyncStateSet 变更通知
- 角色切换信号

---

## 四十四、Namespace 多租户隔离

### 44.1 机制

Namespace 在 Topic、ConsumerGroup 等资源名前加前缀，实现逻辑隔离：

```java
// remoting/protocol/NamespaceUtil.java
wrapNamespace("tenantA", "OrderTopic")  → "tenantA%OrderTopic"
stripNamespace("tenantA%OrderTopic")    → "OrderTopic"
wrapNamespaceAndRetry("tenantA", "Group") → "tenantA%%Group"  // 重试 Topic
```

### 44.2 Client 配置

```java
producer.setNamespace("tenantA");
consumer.setNamespace("tenantA");
// DefaultMQProducerImpl.sendKernelImpl() 自动 wrapNamespace
// 消费/订阅时自动 stripNamespace 做匹配
```

### 44.3 Broker 端

- TopicConfig / SubscriptionGroupConfig 存储带 Namespace 前缀的完整名
- `NamespaceRPCHook` 在 RPC 层自动添加/剥离 Namespace
- 不同 Namespace 的 Topic 互不可见，共享同一 Broker 进程

---

## 四十五、Pop 顺序消费

Classic Push 顺序消费依赖 Broker 队列锁；Pop 模式通过 `ConsumerOrderInfoManager` 实现。

### 45.1 接口设计

```java
// broker/pop/orderly/ConsumerOrderInfoManager.java
updateReceivedMessages(...)     // Pop 时记录消息状态
updateOrderInfoByAck(...)       // ACK 时更新顺序信息
isLockFree(...)                 // 检查是否可以 Pop 下一批
getNextVisibleOffset(...)       // 获取下一个可见 offset
```

### 45.2 顺序级别

```java
// common/OrderedConsumptionLevel.java
QUEUE_LEVEL       // 队列级顺序 (默认，同 Classic)
MESSAGE_GROUP_LEVEL  // 消息组级顺序 (同 ShardingKey，提高并发)
```

**队列级**：同一 Queue 同时只有一个 Pop 请求在处理，前一批全部 ACK 后才 Pop 下一批。

**消息组级**：同一 MessageGroup 顺序，不同 MessageGroup 可并行 Pop。

### 45.3 Pop 顺序 vs Classic 顺序

| 维度 | Classic Orderly | Pop Orderly |
|------|----------------|-------------|
| 锁机制 | Broker lockBatchMQ | ConsumerOrderInfoManager 状态机 |
| Rebalance | 需抢锁 | 无影响 |
| 并发度 | 队列级 | 可配置 MESSAGE_GROUP_LEVEL |
| 超时 | 锁超时 30s | invisibleTime |

---

## 四十六、Broker Config v2 与 OpenMessaging

### 46.1 Config v2（RocksDB 配置存储）

5.x Broker 元数据可选 RocksDB 后端，替代 JSON 文件：

```java
// broker/config/v2/ConfigStorage.java
extends AbstractRocksDBStorage {
    // Column Family:
    //   topic_config
    //   subscription_group
    //   consumer_offset
    //   topic_queue_mapping
    //   ...
}
```

**优势**：原子批量写入、支持大量 Topic/Group、启动加载更快。

配置：`brokerConfigStorageEnable=true` + RocksDB 路径。

### 46.2 OpenMessaging 适配层

```java
// openmessaging/MessagingAccessPointImpl.java
implements MessagingAccessPoint {
    createProducer()  → ProducerImpl (封装 DefaultMQProducer)
    createPushConsumer() → PushConsumerImpl (封装 DefaultMQPushConsumer)
    createPullConsumer() → PullConsumerImpl
}
```

OpenMessaging 标准 API → RocketMQ 原生 Client 的适配桥，支持 OMS 0.3.0 规范。

| OMS 接口 | RocketMQ 实现 |
|----------|----------------|
| Producer.send | DefaultMQProducer.send |
| PushConsumer.subscribe | DefaultMQPushConsumer.subscribe |
| PullConsumer.pull | DefaultMQPullConsumer.pull |

---

## 四十七、msgId 生成与构建部署

### 47.1 MessageClientIDSetter

```java
// common/message/MessageClientIDSetter.java
// uniqId 结构 (LEN = ip + pid + classloaderHash + timestamp + counter):
createUniqID() {
    FIX_STRING = ip(4/16B) + pid(2B) + classloaderHash(4B)  // 固定前缀
    + datePrefix(8B)   // 月初 timestamp 编码
    + counter(2B)      // 原子递增
}
```

**MessageId**（Broker 端）= `storeHost(ip+port)` + `commitLogOffset(8B)` → 16/28 字节 Hex 字符串。

**uniqId**（Client 端）= `MessageClientIDSetter.createUniqID()` → 32 字符，用于 Client 端去重和事务关联。

### 47.2 构建体系

| 工具 | 文件 | 说明 |
|------|------|------|
| Maven | `pom.xml` (revision=5.5.0) | 主构建，Java 8 |
| Bazel | `WORKSPACE` + `BUILD.bazel` | 可选，rules_jvm_external 拉 Maven 依赖 |
| 打包 | `distribution/` | 组装 bin/conf/lib |

### 47.3 启动脚本

```
distribution/bin/
  mqnamesrv / mqnamesrv.cmd     → NamesrvStartup
  mqbroker / mqbroker.cmd       → BrokerStartup (或 -pm local → ProxyStartup)
  mqproxy / mqproxy.cmd         → ProxyStartup
  mqcontroller / mqcontroller.cmd → ControllerStartup
  mqbrokercontainer             → BrokerContainerStartup
  mqadmin / mqadmin.cmd         → MQAdminStartup
  runserver.sh / runbroker.sh   → 通用 JVM 启动器 (ROCKETMQ_HOME)
```

JVM 参数通过 `runserver.sh` 统一设置堆大小、GC、堆外内存等。

### 47.4 mqadmin 工具体系

```
tools/MQAdminStartup
  → MQAdminStartup.initCommand() 注册 SubCommand
  → 常用命令:
    topicRoute / topicList / topicStatus
    consumerProgress / consumerConnection
    sendMessage / consumeMessage
    updateBrokerConfig / updateTopic
    resetOffsetByTime / cleanExpiredCQ
    exportMetrics / clusterList
    migrateTopic / rebalance
```

每个 SubCommand 实现 `execute(CommandLine, Options, RPCHook)` 通过 Remoting 调用 Broker/NS。

---

## 附录 G：核心 RequestCode 速查（扩展）

| Code | 名称 | 方向 | 处理方 |
|------|------|------|--------|
| 10 | CHECK_TRANSACTION_STATE | Broker→Client | ClientRemotingProcessor |
| 11 | PULL_MESSAGE | Client→Broker | PullMessageProcessor |
| 103 | REGISTER_BROKER | Broker→NS | DefaultRequestProcessor |
| 105 | GET_ROUTEINFO_BY_TOPIC | Client→NS | ClientRequestProcessor |
| 200 | GET_CONSUMER_LIST_BY_GROUP | Client→Broker | AdminBrokerProcessor |
| 206 | GET_CONSUMER_CONNECTION_LIST | Admin→Broker | AdminBrokerProcessor |
| 310 | SEND_MESSAGE | Client→Broker | SendMessageProcessor |
| 311 | SEND_MESSAGE_V2 | Client→Broker | SendMessageProcessor |
| 312 | SEND_BATCH_MESSAGE | Client→Broker | SendMessageProcessor |
| 324 | SEND_REPLY_MESSAGE | Client→Broker | ReplyMessageProcessor |
| 401 | SEND_TRANSACTION_MESSAGE | Client→Broker | SendMessageProcessor |
| 405 | END_TRANSACTION | Client→Broker | EndTransactionProcessor |
| 901 | GET_BROKER_MEMBER_GROUP | Broker→NS | DefaultRequestProcessor |
| 904 | BROKER_HEARTBEAT | Broker→NS | DefaultRequestProcessor |
| 200050 | POP_MESSAGE | Client→Broker/Proxy | PopMessageProcessor |
| 200051 | ACK_MESSAGE | Client→Broker/Proxy | AckMessageProcessor |
| 200052 | BATCH_ACK_MESSAGE | Client→Broker/Proxy | AckMessageProcessor |
| 200053 | CHANGE_MESSAGE_INVISIBLETIME | Client→Broker | ChangeInvisibleTimeProcessor |
| 200054 | NOTIFICATION | Client→Broker | NotificationProcessor |
| 200086 | RECALL_MESSAGE | Client→Broker | RecallMessageProcessor |

---

## 附录 H：源码目录树（精简）

```
rocketmq/
├── auth/                 # 认证鉴权框架
├── broker/               # Broker 服务端
│   ├── processor/        # 请求处理器 (Send/Pull/Pop/Ack/Admin...)
│   ├── topic/            # Topic/StaticTopic 管理
│   ├── offset/           # 消费进度
│   ├── transaction/      # 事务消息
│   ├── schedule/         # 延迟消息
│   ├── pop/              # Pop 顺序消费
│   ├── controller/       # ReplicasManager
│   ├── auth/             # Auth Pipeline
│   ├── config/v1|v2/     # 配置存储
│   └── out/              # BrokerOuterAPI
├── client/               # Java SDK
│   ├── producer/         # Producer API
│   ├── consumer/         # Consumer API
│   ├── impl/             # 内部实现
│   ├── trace/            # 消息轨迹
│   └── latency/          # 延迟隔离
├── common/               # 公共模块
├── container/            # BrokerContainer
├── controller/           # Controller 选主
├── distribution/         # 打包部署
├── docs/                 # 文档
├── example/              # 示例
├── filter/               # SQL92 过滤
├── namesrv/              # NameServer
├── openmessaging/        # OMS 适配
├── proxy/                # Proxy 网关
│   ├── grpc/             # gRPC 服务
│   ├── remoting/         # Remoting 兼容
│   ├── processor/        # MessagingProcessor
│   └── service/          # ServiceManager
├── remoting/             # RPC 协议
│   ├── netty/            # Netty 实现
│   └── protocol/         # 协议定义
├── store/                # 存储引擎
│   ├── ha/               # 主从复制
│   ├── dledger/          # DLedger CommitLog
│   ├── timer/            # 定时消息
│   ├── index/            # 索引
│   ├── queue/            # ConsumeQueue
│   ├── pop/              # Pop 状态
│   ├── rocksdb/          # RocksDB 后端
│   └── metrics/          # Store 指标
├── tieredstore/          # 分层存储
├── tools/                # mqadmin CLI
├── pom.xml               # Maven 根 POM
└── WORKSPACE             # Bazel 构建
```

---

## 四十八、TieredStore 冷热分层深度

### 48.1 插件架构

`TieredMessageStore` 继承 `AbstractPluginMessageStore`，包装底层 `DefaultMessageStore`：

```
Producer → TieredMessageStore.putMessage()
              ├─ defaultStore.putMessage()     // 热数据写本地 CommitLog
              └─ dispatcher.dispatch()         // 异步迁移到冷存储

Consumer → TieredMessageStore.getMessage()
              ├─ fetchFromCurrentStore()? 
              │    YES → fetcher.getMessage()  // 从冷存储读
              │    NO  → defaultStore.getMessage()  // 从热存储读
              └─ 合并结果
```

### 48.2 核心组件

| 组件 | 类 | 职责 |
|------|-----|------|
| 元数据 | `MetadataStore` | Topic/Queue/Segment 元信息 |
| 冷文件 | `FlatFileStore` | 按 Segment 组织的冷数据文件 |
| 迁移调度 | `MessageStoreDispatcherImpl` | CommitLog → FlatFile 异步上传 |
| 冷读 | `MessageStoreFetcherImpl` | 从 FlatFile 读取消息 |
| Topic 过滤 | `MessageStoreTopicFilter` | 系统 Topic 不走分层 |
| 索引 | `IndexStoreService` | 冷数据 MessageKey 索引 |
| 后端 | `PosixFileSegment` / 自定义 | 实际 IO 提供者 |

### 48.3 迁移流程（MessageStoreDispatcherImpl）

```java
// tieredstore/core/MessageStoreDispatcherImpl.java
run() {
    while (!stopped) {
        for each FlatFile in flatFileStore {
            dispatchWithSemaphore(flatFile) {
                // 1. 从 defaultStore CQ 读取待迁移消息
                // 2. 读取 CommitLog 消息体
                // 3. 写入 FlatFile (FileSegment)
                // 4. 更新 metadataStore commitOffset
                // 5. GroupCommit 批量提交
            }
        }
    }
}
```

**批量触发条件**（满足任一）：
- 累积 `tieredStoreGroupCommitCount` 条（默认 2500）
- 累积 `tieredStoreGroupCommitSize` 字节（默认 32MB）

**并发控制**：`Semaphore(tieredStoreMaxPendingLimit / 4)` 限制并行迁移任务数。

### 48.4 读取路由决策（fetchFromCurrentStore）

```java
TieredStorageLevel 枚举:
  DISABLE       → 不启用分层
  NOT_IN_DISK   → CommitLog 中已无该 offset → 读冷存储
  NOT_IN_MEM    → 不在 PageCache 中 → 读冷存储
  FORCE         → 强制读冷存储

判断逻辑:
  offset >= flatFile.getConsumeQueueCommitOffset() → 读热存储 (数据尚未迁移)
  !next.checkInStoreByConsumeOffset(topic, queueId, offset) → 读冷存储
  !next.checkInMemByConsumeOffset(topic, queueId, offset, batchSize) → 读冷存储
```

### 48.5 Read-Ahead Cache

冷数据读取带预读缓存，避免每次读冷存储都走远程 IO：

- `readAheadCacheSizeThresholdRate`：堆空间占用上限（默认 30%）
- `readAheadCacheExpireDuration`：缓存过期时间（默认 1s）
- 指标：`rocketmq_tiered_store_read_ahead_cache_hit_total`

### 48.6 配置示例

```properties
messageStorePlugIn=org.apache.rocketmq.tieredstore.TieredMessageStore
tieredBackendServiceProvider=org.apache.rocketmq.tieredstore.provider.PosixFileSegment
tieredStoreFilePath=/mnt/cold-storage
tieredStorageLevel=NOT_IN_DISK
tieredStoreFileReservedTime=72
tieredStoreGroupCommitCount=2500
tieredStoreGroupCommitSize=33554432
```

---

## 四十九、gRPC Telemetry 双向流

5.x 新版 Client 通过 gRPC **双向流** 与 Proxy/Broker 交换控制面信息，替代旧版定时心跳的部分职责。

### 49.1 流模型

```
Client                                    Proxy (ClientActivity)
  │                                            │
  │──── TelemetryCommand (SETTINGS) ──────────→│ processAndWriteClientSettings()
  │←─── TelemetryCommand (SETTINGS) ──────────│ 返回服务端配置
  │                                            │
  │──── TelemetryCommand (THREAD_STACK) ──────→│ reportThreadStackTrace()
  │──── TelemetryCommand (VERIFY_MSG) ────────→│ reportVerifyMessageResult()
  │                                            │
  │←─── RECOVER_ORPHANED_TRANSACTION ──────────│ 事务恢复指令
  │←─── PRINT_THREAD_STACK_TRACE ──────────────│ 诊断指令
  │                                            │
  │═══════ 长连接保持 ═══════════════════════│
```

入口：`GrpcMessagingApplication.telemetry()` → `ClientActivity.telemetry()`。

### 49.2 SETTINGS 交换

Client 首次连接发送 `Settings`（clientId、endpoints、请求超时、消费线程数等），Proxy 返回：

- 服务端限流配置
- 路由缓存策略
- Pop invisibleTime 默认值
- Lite Subscription 同步策略

`GrpcClientSettingsManager` 维护 clientId → Settings 映射。

### 49.3 连接断开处理

```java
// ClientActivity.handleGrpcCancel()
if (Status.CANCELLED || Status.UNAVAILABLE) {
    grpcClientSettingsManager.offlineClientLiteSubscription(ctx, clientId, null);
    // 清理 Proxy 侧 Consumer 注册信息 → 触发 Rebalance
}
```

gRPC 连接断开等价于 Consumer 下线，Proxy 会清理该 clientId 的全部订阅状态。

### 49.4 与 Remoting 心跳的关系

| 维度 | Remoting 心跳 | gRPC Telemetry |
|------|----------------|----------------|
| 协议 | 定时 Oneway RPC | gRPC 双向流 |
| 频率 | 30s | 长连接 + 事件驱动 |
| 携带信息 | 订阅关系 + 消费模式 | Settings + 诊断 + 事务恢复 |
| 使用方 | 旧版 Java Client | rocketmq-clients (5.x) |

---

## 五十、ConsumeQueueExt 与过滤位图

### 50.1 设计目的

`ConsumeQueueExt` 是 CQ 的扩展文件，存储 CQ 主文件不便于存放的附加信息：

```
ConsumeQueue (20B/条):  offset + size + tagsCode
ConsumeQueueExt:       storeTime + bitMap (SQL92 过滤) + ...
```

特点（源码注释）：
- 仅 `ConsumeQueue` 内部使用
- **弱可靠**（丢失不影响消息正确性，只影响过滤精度）
- 地址始终 < 0（与 CQ 正地址区分）

### 50.2 CqExtUnit 结构

```
┌──────────────────┬──────────────────┬──────────────────┐
│   Tag HashCode   │   Store Time     │    BitMap        │
│    (8 Bytes)     │   (8 Bytes)      │   (N Bytes)      │
└──────────────────┴──────────────────┴──────────────────┘
```

- **Store Time**：消息存储时间戳，用于按时间过滤
- **BitMap**：SQL92 表达式 BloomFilter 位图，加速过滤

### 50.3 过滤链路中的角色

```
getMessage 读取 CQ 索引
  → isMatchedByConsumeQueue(tagsCode, cqExtUnit)
       Tag 模式: codeSet.contains(tagsCode)
       SQL92 模式: BloomFilter.isValid(cqExtUnit.bitMap)
  → 通过预过滤后才读 CommitLog
  → isMatchedByCommitLog() 精确 SQL 匹配
```

**设计亮点**：BloomFilter 在 CQ 层拦截，避免对不匹配消息读 CommitLog 随机 IO。

---

## 五十一、QueueOffset 分配机制

### 51.1 QueueOffsetOperator

```java
// store/queue/QueueOffsetOperator.java
ConcurrentMap<String, Long> topicQueueTable;       // topic-queueKey → nextOffset
ConcurrentMap<String, Long> batchTopicQueueTable;  // Batch 消息独立计数
ConcurrentMap<String, Long> lmqTopicQueueTable;  // Lite Message Queue
```

### 51.2 分配时机

```java
// CommitLog.asyncPutMessage() 写入前:
topicQueueLock.lock(topicQueueKey);
assignOffset(msg);          // 读取 nextOffset 赋给 msg.queueOffset
// ... appendMessage ...
increaseOffset(msg, batchNum);  // nextOffset += batchNum
topicQueueLock.unlock();
```

```java
// DefaultMessageStore.assignOffset()
if (tranType == NOT_TYPE || COMMIT_TYPE)
    consumeQueueStore.assignQueueOffset(msg);
// Half 消息 (PREPARED) 不分配 offset（尚未进入真实 Topic CQ）
```

### 51.3 三层 offset 体系

| offset 类型 | 含义 | 存储位置 |
|------------|------|---------|
| **QueueOffset** | 逻辑消费队列 offset | QueueOffsetOperator 内存 + CQ 文件 |
| **CommitLog Offset** | 物理文件绝对位置 | CommitLog MappedFile |
| **Consumer Offset** | 消费组进度 | ConsumerOffsetManager (Broker) / 本地文件 (Client) |

关系：`CQ[queueOffset] → {commitLogOffset, size, tagsCode}` → `CommitLog[commitLogOffset] → 消息体`

---

## 五十二、MessageStore 插件链

### 52.1 插件模式

```java
// store/plugin/AbstractPluginMessageStore.java
public abstract class AbstractPluginMessageStore implements MessageStore {
    protected MessageStore next;  // 委托链

    public PutMessageResult putMessage(msg) {
        // 前置逻辑
        return next.putMessage(msg);  // 委托给下一层
    }
}
```

启用方式：

```properties
messageStorePlugIn=org.apache.rocketmq.tieredstore.TieredMessageStore
# 或
messageStorePlugIn=org.apache.rocketmq.store.RocksDBMessageStore
```

### 52.2 初始化链

```
BrokerController.initializeMessageStore()
  → MessageStorePluginContext 构建
  → Class.forName(messageStorePlugIn).newInstance(context, defaultMessageStore)
  → plugin.load() → plugin.start()
  → 实际 MessageStore 引用指向 plugin（外层）
```

### 52.3 已有插件实现

| 插件 | 类 | 增强 |
|------|-----|------|
| TieredStore | `TieredMessageStore` | 冷热分层 |
| RocksDB | `RocksDBMessageStore` | CQ 存 RocksDB |

插件可覆写 `putMessage`、`getMessage`、`load`、`start` 等方法，未覆写的自动委托 `next`。

---

## 五十三、Producer 异步背压

### 53.1 双 Semaphore 限流

```java
// DefaultMQProducerImpl 构造时:
semaphoreAsyncSendNum  = new Semaphore(backPressureForAsyncSendNum, true);   // 默认 10000
semaphoreAsyncSendSize = new Semaphore(backPressureForAsyncSendSize, true); // 默认 100MB
```

### 53.2 异步发送流程

```java
executeAsyncMessageSend(msg, callback, timeout) {
    // 1. 尝试获取 num 信号量 (1个)
    boolean acquired = semaphoreAsyncSendNum.tryAcquire(timeout, MILLISECONDS);
    if (!acquired) throw RemotingTooMuchRequestException("send async semaphore num");

    // 2. 尝试获取 size 信号量 (msgBodySize)
    acquired = semaphoreAsyncSendSize.tryAcquire(msgBodySize, timeout, MILLISECONDS);
    if (!acquired) {
        semaphoreAsyncSendNum.release();
        throw RemotingTooMuchRequestException("send async semaphore size");
    }

    // 3. 发送
    sendDefaultImpl(ASYNC, ...);

    // 4. 回调/onException 中 release 两个信号量
}
```

### 53.3 背压效果

| 场景 | 行为 |
|------|------|
| 正常 | 异步发送不阻塞业务线程 |
| 发送速度 > Broker 处理速度 | Semaphore 耗尽 → `RemotingTooMuchRequestException` |
| 回调未及时执行 | size 信号量占用 → 新发送被限流 |

**配置调优**：

```java
producer.setBackPressureForAsyncSendNum(50000);
producer.setBackPressureForAsyncSendSize(200 * 1024 * 1024);
```

### 53.4 Netty Client 层信号量

除 Producer 层外，Remoting Client 也有独立信号量：

```java
// NettyRemotingClient 构造:
super(clientOnewaySemaphoreValue, clientAsyncSemaphoreValue);
// 限制全局 Oneway / Async 请求并发数
```

---

## 五十四、Peek 消息与 Pop 消费者锁

### 54.1 PeekMessageProcessor

**Peek** = 读取消息但**不推进消费进度、不触发 Pop 状态变更**：

```java
// broker/processor/PeekMessageProcessor.java
// RequestCode.PEEK_MESSAGE

processRequest() {
    // 类似 PopMessageProcessor，但从 Store 读取后不创建 PopCheckPoint
    // 不设置 invisibleTime
    // 不写入 Revive Topic
    // Consumer offset 不变
}
```

适用场景：消息预览、调试、监控消费进度而不影响消费位点。

### 54.2 PopConsumerLockService

Pop 模式下控制 Consumer Group 对 Topic 的并发 Pop：

```java
// broker/pop/PopConsumerLockService.java
ConcurrentMap<String, TimedLock> lockTable;  // "groupId@topicId" → lock

tryLock(groupId, topicId) {
    TimedLock lock = lockTable.computeIfAbsent(key, ...);
    return lock.tryLock();  // CAS + 超时自动释放
}
```

**作用**：
- 防止同一 Group+Topic 并发 Pop 导致消息重复分配
- 超时自动 unlock（默认 `popConsumerLockTimeout`）
- 与 `ConsumerOrderInfoManager` 配合实现 Pop 顺序消费

---

## 五十五、Broker 运行时统计体系

### 55.1 BrokerStatsManager

Broker 内置轻量级统计（非 OpenTelemetry），用于 mqadmin 和运维监控：

| 统计项 | 常量 | 含义 |
|--------|------|------|
| Topic 发送数 | TOPIC_PUT_NUMS | 每 Topic 发送消息数 |
| Topic 发送量 | TOPIC_PUT_SIZE | 每 Topic 发送字节数 |
| Group 消费数 | GROUP_GET_NUMS | 每 Group 消费消息数 |
| 发送回退 | SNDBCK_PUT_NUMS | sendMessageBack 次数 |
| 死信 | DLQ_PUT_NUMS | 死信队列消息数 |
| Pop ACK | BROKER_ACK_NUMS | Pop ACK 次数 |
| Pop CK | BROKER_CK_NUMS | PopCheckPoint 数 |
| 发送延迟 | TOPIC_PUT_LATENCY | 发送耗时分布 |

### 55.2 统计架构

```
StatisticsManager (common)
  → StatisticsItem (原子计数)
  → StatisticsItemScheduledPrinter (定时打印, 默认 60s)
  → StatsItemSet / MomentStatsItemSet (分维度聚合)

StoreStatsService (store)
  → 存储层 TPS / 耗时统计
  → singlePutMessageTopicTimesTotal
  → getMessageTransferedMsgCount
```

### 55.3 与 OpenTelemetry 的关系

| 维度 | BrokerStatsManager | OpenTelemetry Metrics |
|------|-------------------|----------------------|
| 输出 | 日志 / mqadmin | OTLP Exporter |
| 粒度 | Topic/Group 级 | 可配置 Label |
| 性能 | 极低开销 | 中等 |
| 用途 | 运维排查 | 监控告警 / Grafana |

5.x 推荐使用 OpenTelemetry，BrokerStatsManager 仍保留用于向后兼容。

---

## 五十六、全局顺序消息 Order Topic

### 56.1 概念

Order Topic 是 RocketMQ 4.x 引入的**全局严格顺序**方案，通过 NameServer KV 配置实现 Broker 间的顺序路由。

### 56.2 KV 配置

```java
// namesrv/kvconfig/KVConfigManager.java
namespace = ORDER_TOPIC_CONFIG  (NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG)

PUT_KV_CONFIG(100) → 存储 OrderTopic 配置
GET_KV_CONFIG(101) → Broker 启动时拉取

// Broker 注册时 NS 返回 OrderTopic KV:
registerBroker() → response.body = kvTable (orderTopicConf)
```

### 56.3 路由规则

Order Topic 的所有队列映射到**同一 Broker** 的**同一队列**：

```
普通 Topic:  Queue-0 → Broker-A, Queue-1 → Broker-B (并行)
Order Topic: Queue-0 → Broker-A:0, Queue-1 → Broker-A:0 (全局串行)
```

Producer 发送 Order Topic 消息时，所有消息路由到同一物理队列，保证全局顺序。

### 56.4 与局部顺序的对比

| 维度 | 局部顺序 (MessageQueueSelector) | 全局顺序 (Order Topic) |
|------|-------------------------------|----------------------|
| 范围 | 单 Queue 内有序 | 全 Topic 有序 |
| 实现 | Client Hash 选 Queue | NS KV 配置强制路由 |
| 并发度 | 多 Queue 并行 | 单 Queue 串行 |
| 性能 | 高 | 低（瓶颈在单 Queue） |
| 使用场景 | 订单 ID 级顺序 | 数据库 Binlog 级顺序 |

---

## 附录 I：源码学习路径建议

### 入门级（理解基本流程）

```
1. example/quickstart/Producer.java + Consumer.java
2. namesrv/NamesrvStartup → RouteInfoManager
3. broker/BrokerStartup → BrokerController
4. broker/processor/SendMessageProcessor
5. broker/processor/PullMessageProcessor
6. client/DefaultMQProducerImpl.sendDefaultImpl()
7. client/RebalanceImpl.doRebalance()
8. store/CommitLog.asyncPutMessage()
9. store/DefaultMessageStore.ReputMessageService
```

### 进阶级（存储与 HA）

```
1. store/ConsumeQueue + ConsumeQueueExt
2. store/index/IndexService
3. store/ha/DefaultHAService + DefaultHAConnection
4. store/dledger/DLedgerCommitLog
5. broker/controller/ReplicasManager
6. controller/impl/DLedgerController
7. store/DefaultMessageStore.load/recover
8. store/CommitLog.DefaultFlushManager
```

### 高级（5.x 新特性）

```
1. broker/processor/PopMessageProcessor
2. broker/processor/AckMessageProcessor + PopReviveService
3. broker/processor/PopBufferMergeService
4. proxy/ProxyStartup → GrpcMessagingApplication
5. proxy/processor/DefaultMessagingProcessor
6. tieredstore/TieredMessageStore
7. store/timer/TimerMessageStore + TimerWheel
8. auth/AuthenticationEvaluator + AuthorizationEvaluator
```

### 专家级（设计模式与扩展）

```
1. store/plugin/AbstractPluginMessageStore
2. broker/topic/TopicQueueMappingManager (Static Topic)
3. remoting/netty/NettyRemotingServer (Reactor 模型)
4. client/latency/MQFaultStrategy (延迟隔离)
5. filter/ SQL92 Evaluator + BloomFilter
6. container/BrokerContainer
```

---

## 附录 J：核心配置参数完整清单

### NameServer

| 参数 | 默认 | 说明 |
|------|------|------|
| listenPort | 9876 | 监听端口 |
| scanNotActiveBrokerInterval | 5000ms | Broker 过期扫描 |
| brokerChannelExpiredTime | 120000ms | 心跳超时 |
| supportActingMaster | false | Acting Master |
| needWaitForService | false | 启动保护期 |

### Broker

| 参数 | 默认 | 说明 |
|------|------|------|
| listenPort | 10911 | 监听端口 |
| brokerRole | ASYNC_MASTER | SYNC_MASTER/ASYNC_MASTER/SLAVE |
| flushDiskType | ASYNC_FLUSH | SYNC_FLUSH/ASYNC_FLUSH |
| registerNameServerPeriod | 30000ms | NS 注册周期 |
| deleteWhen | 04 | 文件清理时间点 |
| fileReservedTime | 72 | CommitLog 保留小时 |
| transferMsgByHeap | true | 消息堆内传输 |
| enableControllerMode | false | Controller 模式 |
| enableSlaveActingMaster | false | Slave 冒充 Master |
| messageStorePlugIn | — | Store 插件类名 |

### Store

| 参数 | 默认 | 说明 |
|------|------|------|
| mappedFileSizeCommitLog | 1GB | CommitLog 文件大小 |
| mappedFileSizeConsumeQueue | 6000000 | CQ 文件大小 |
| transientStorePoolEnable | false | 堆外内存池 |
| transientStorePoolSize | 5 | 池大小 |
| useReentrantLockWhenPutMessage | true | 写锁类型 |
| enableDLegerCommitLog | false | DLedger 模式 |
| minInSyncReplicas | 1 | 最小同步副本 |
| diskMaxUsedSpaceRatio | 75 | 写拒绝阈值 |
| readUncommitted | false | 读未确认消息 |

### Client

| 参数 | 默认 | 说明 |
|------|------|------|
| pollNameServerInterval | 30000ms | 路由刷新 |
| heartbeatBrokerInterval | 30000ms | 心跳间隔 |
| persistConsumerOffsetInterval | 5000ms | offset 持久化 |
| pullBatchSize | 32 | 批量拉取 |
| consumeThreadMin/Max | 20 | 消费线程 |
| pullThresholdForQueue | 1000 | 本地缓存上限 |
| sendMsgTimeout | 3000ms | 发送超时 |
| retryTimesWhenSendFailed | 2 | 同步重试 |
| compressMsgBodyOverHowmuch | 4096 | 压缩阈值 |

### Proxy

| 参数 | 默认 | 说明 |
|------|------|------|
| proxyMode | cluster | cluster/local |
| grpcServerPort | 8081 | gRPC 端口 |
| grpcRouteThreadPoolNums | 16 | 路由线程池 |
| grpcProducerThreadPoolNums | 16 | 发送线程池 |
| grpcConsumerThreadPoolNums | 16 | 消费线程池 |

### TieredStore

| 参数 | 默认 | 说明 |
|------|------|------|
| tieredStorageLevel | NOT_IN_DISK | 分层级别 |
| tieredStoreFileReservedTime | 72h | 冷数据 TTL |
| tieredStoreGroupCommitCount | 2500 | 批量迁移条数 |
| tieredStoreGroupCommitSize | 32MB | 批量迁移大小 |
| tieredStoreMaxPendingLimit | 10000 | 最大 pending 数 |

---

*本文档基于 RocketMQ 5.5.0 源码分析生成，如需追踪特定链路的逐行源码，请参考第十一节导航表。*
