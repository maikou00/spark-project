# POM 统一版本管理设计

@author maikou @since 2026-06-09

## 目标

将项目中散落在各 POM 的硬编码版本号统一收纳到根 POM 的 `<properties>` 中，通过 `<dependencyManagement>` 和 `<pluginManagement>` 集中管控，子模块不再写版本号。

## 现状问题

| 问题 | 位置 |
|---|---|
| 4 个数据源连接器版本硬编码 | `spark-common/pom.xml` — ClickHouse、ES、MongoDB、Redis |
| 5 个 Maven 插件版本散落 | 根 `pom.xml` — compiler、surefire、jacoco、shade、enforcer |
| `cluster` profile 重复声明依赖 | 根 `pom.xml` — 与 `<dependencyManagement>` 重复 |
| `<properties>` 无分组注释 | 根 `pom.xml` |

## 方案

### 1. 版本属性整理

根 POM `<properties>` 按功能分组：

- 构建（maven.compiler.*）
- 核心引擎（scala、spark、hadoop、hive）
- 工具库（lombok、commons-lang3、mysql）
- 数据源连接器（clickhouse、elasticsearch、mongodb、redis）**新增**
- 测试（junit、mockito、assertj）
- Maven 插件（compiler、surefire、shade、enforcer、jacoco）**新增**

命名延续简洁风格：`<clickhouse.version>`、`<elasticsearch.version>` 等。

### 2. dependencyManagement 扩展

根 POM `<dependencyManagement>` 新增 4 个连接器声明。`spark-common` 中对应依赖删除 `<version>`。

### 3. pluginManagement 统一

新增 `<pluginManagement>` 收纳所有插件，`<build>` 和 profile 中的插件去掉 `<version>`，引用 `${属性}`。

### 4. Profile 去重

`cluster` profile 移除与 `<dependencyManagement>` 重复的依赖声明。`spark-hive` 的 exclusion 移到根 `<dependencyManagement>` 统一处理。

## 受影响文件

- `pom.xml` — 主要修改
- `spark-common/pom.xml` — 删除硬编码版本
- `spark-order-statistics/pom.xml` — 无需修改
