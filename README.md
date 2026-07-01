# 🏪 内容社区事件驱动型实时数仓

![Scala](https://img.shields.io/badge/Scala-2.12.17-DC322F?logo=scala)
![Spark](https://img.shields.io/badge/Spark-3.0.0-E25A1C?logo=apachespark)
![Kafka](https://img.shields.io/badge/Kafka-2.x-231F20?logo=apachekafka)
![Redis](https://img.shields.io/badge/Redis-Stack-DC382D?logo=redis)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-0.7.0-003366)
![License](https://img.shields.io/badge/license-MIT-green)

> 📚 一个用于**学习与验证** Spark Structured Streaming 实时数仓学习效果的个人项目。

---

## 📖 目录

- [项目动机](#项目动机)
- [业务场景](#业务场景)
- [快速开始](#快速开始)
- [核心特性](#核心特性)
- [指标体系](#指标体系)
- [已知局限](#已知局限)
- [许可证](#许可证)

---

## 🎯 项目动机

本项目是一名数据科学与大数据技术专业本科生的**自学验证项目**，目标是通过动手搭建一个完整的实时数据仓库，深入理解以下技术概念：

- **事件时间 vs 处理时间**：Watermark 推进机制、迟到数据修正对下游的影响
- **有状态流计算**：`flatMapGroupsWithState` 的状态机设计、Checkpoint 恢复、RocksDB vs 堆内存
- **外部状态管理**：RedisBloom 替代 Spark State 做新用户判别的 O(1) 空间优势
- **冷热存储分离**：Redis 多聚合指标秒级查询 + Delta Lake 全量明细归档
- **CEP 复杂事件处理**：多条件滑动窗口判定刷屏/刷赞等异常行为
- **at-least-once + 幂等下游**：分布式一致性在实习项目中的务实取舍

> **本项目不面向生产环境。** 所有设计决策均以"理解原理"为优先，部分实现（如 Driver 端 collect 查询）刻意保留了简化的痕迹，以便后续对比优化前后的差异。

---

## 🏪 业务场景

模拟一个"小红书式"内容社区的用户行为数据流：

- **13 种事件类型**：`app_open`、`enter_item`、`leave_item`、`like`、`fav`、`share`、`comment`、`follow`、`unfollow`、`dm`、`enter_profile`、`click_tag`、`dislike`
- **250 名模拟用户** × **100 篇内容笔记** — 5 个类目（身心健康、知识成长、时尚美妆、家庭生活、生活记录）
- **30-50 EPS** 持续产出，含 10% 人为注入的乱序（30-90s）和 6 种异常行为模式

---

## ⚡ 快速开始

### 环境要求

- JDK 1.8
- Scala 2.12.17
- SBT 1.x
- Kafka（本地或远程，Topic: `content_events`）
- Redis Stack（含 RedisBloom 模块，端口 6379）
- 运行 Mock Generator 的 Java 运行环境

### 启动步骤

```bash
# 1. 启动 Mock Generator（在您指定的 hadoop 虚拟机）
java -jar mock-generator-1.0-SNAPSHOT.jar

# 2. 编译项目
sbt compile

# 3. 运行测试模式（3 分钟后自动输出 CSV 并退出）
sbt "run --test --duration=180"

# 4. 验证 Redis 指标
redis-cli KEYS 'dws:*'
redis-cli --raw HGETALL 'dws:user_activity:1781092200_1781092500:家庭生活'
```

### 运行模式

| 参数 | 行为 |
|---|---|
| 无参数 | 持续运行，2 分钟后打印一次 MemorySink 结果 |
| `--test` | 测试模式，3 分钟后输出 CSV 到 `您指定的路径` 并停止 |
| `--test --duration=300` | 自定义测试时长（秒） |

---

## ✨ 核心特性

### 📊 28 项实时指标

覆盖四大主题域，按 6 类目下钻，5 分钟/30 分钟滚动窗口聚合：

| 域 | 指标数 | 典型指标 |
|---|---|---|
| 用户活跃与消费 (U) | 7 | DAU, 新用户数, PV, UV, 类目PV/UV, 平均停留, 跳出率 |
| 互动行为 (I) | 11 | 点赞, 收藏, 分享, 评论, 互动率, 转化率, 关注/取关 |
| 会话与路径 (S) | 3 | 会话总数, 平均时长, 平均事件数 |
| 数据质量与异常 (Q) | 5 | A1刷屏, A2狂赞, D1秒退, E1超速, C极长驻留 |

### 🔍 CEP 作弊检测

基于 `flatMapGroupsWithState` 的自定义状态机：

| 异常类型 | 判定规则 |
|---|---|
| **A1 刷屏评论** | 连续 ≥5 条 + 相邻间隔 ≤10s + (60s 窗口 ≥5 条 ∨ ≥4 条相同文本) |
| **A2 疯狂点赞** | 5s 内对 15 篇不同笔记点赞 |

### 📈 其他亮点

- **RedisBloom 新用户判别**：内存固定 ~1.5MB（vs Set 的 O(n) 增长）
- **Missing-to-Unknown**：维表关联失败统一填充，**零数据丢弃**
- **多级 Watermark**：CEP 30min / 聚合 5min，覆盖 30~90s 乱序
- **维表热更新**：每 micro-batch 重读 CSV，无需重启
- **降级设计**：Redis 不可用 → 自动回退本地 Guava BloomFilter

---

## ⚠️ 已知局限

> 这些局限是**有意保留**的 — 作为学习项目，它们帮助理解"简化的代价"和"生产化的方向"。

| 局限 | 说明 | 生产化方向 |
|---|---|---|
| Driver 端 `collect()` 查询 Bloom | 新用户判别中 `collect()` 到 Driver 串行查 Redis，单点瓶颈 | `foreachPartition` + `Accumulator` 分布式查询 |
| Delta 无 Auto Compaction | 长期运行小文件堆积 | 开启 `autoOptimize.optimizeWrite` + 定时 `OPTIMIZE` |
| CheatDetector / NewUserAggregator 仅 MemorySink | 作弊标记和新用户指标无生产级 Sink | 补 RedisSink 写入 |
| Bloom 全域单 Key 无 TTL | 超过 capacity 后误判率上升 | 按日分片或定时重建 |
| 未做压测 | "80% 内存节省""万级吞吐"均为理论估算 | 生产环境压测验证 |
| `complete` 模式的聚合流 | 每批全量重写 Redis，造成窗口修正波动 | 改为 `update` 模式 |

---

## 📄 许可证

MIT © 2026 HenRY2048-arch — 详见 [LICENSE](LICENSE)

---
