# Agent Instructions

## Git 工作流

- **每次改代码前先建分支**，禁止在 main 上直接开发；合入 main 后删除远端分支
- 分支名：`{type}/{描述}`，如 `fix/ci-workflow`、`refactor/pom-cleanup`
- `git add & commit` 自动执行；**禁止自动 push**，需人工确认
- 提交格式：`<type>: <subject>`，type 取值 `feat`/`fix`/`refactor`/`test`/`docs`/`perf`/`chore`
- 原则：原子提交、subject 极简，能准确说明就行（一般现在时）、拼写修正用 `amend`

## 代码审查

- 提交前自查 diff 中的调试代码、TODO、死代码；复杂改动附说明

## 代码规范

### 命名
- 类名 UpperCamelCase（`SparkEnvUtils`），方法 lowerCamelCase（`getDatabaseList`），常量 UPPER_SNAKE（`DB_ODS`）
- 抽象类以 `Abstract`/`Base` 开头，异常类以 `Exception` 结尾，测试类以 `Test` 结尾
- POJO 禁止 `Map`/`List` 接收参数，必须定义具体类；包名全小写单数

### 常量与枚举
- 禁止魔法值，必须定义为常量；常量类 `final class` + `private` 构造器
- ≥5 个固定值优先用 `enum`

### 代码结构
- 工具类：`final class` + `private` 构造器 + 全部 `static` 方法
- 方法 ≤ 80 行；成员顺序：常量 → 静态变量 → 实例变量 → 构造器 → public 方法 → private 方法
- 所有 `public` 类加 `@Slf4j`；尽可能用 Lombok

### 集合
- 数组转 List：`new ArrayList<>(Arrays.asList(...))`（`Arrays.asList()` 不可变）
- 判空用 `CollectionUtils.isEmpty()` 或 `list.isEmpty()`，禁止 `list.size() == 0`
- `equals` 常量放左侧：`"OK".equals(status)`
- Stream `forEach` 禁止修改外部变量

### 并发
- 线程池禁止 `Executors`，用 `ThreadPoolExecutor` 显式创建
- 日期格式化用 `FastDateFormat`（线程安全），禁止 `SimpleDateFormat`

### 异常
- 优先精确捕获；工具类多 checked 异常允许 `catch(Exception)`，禁止吞异常
- 可恢复异常：记录+返回默认值；系统异常：抛 `WarehouseException`
- `finally` 必须释放资源（SparkSession、FileSystem、Connection 等）
- 日志与抛异常二选一，禁止重复；禁止空 catch

### 日志
- 统一 `@Slf4j`，禁止 `System.out`/`System.err`
- 必须用 `{}` 占位符，禁止字符串拼接
- 级别：ERROR(功能异常) / WARN(可恢复) / INFO(关键步骤) / DEBUG(调试)

### 注释
- 所有 `public` 方法写 Javadoc（参数、返回值、异常）
- 类注释含功能说明 + `@author` + `@since`
- TODO 标注责任人：`// TODO(maikou): 2026-06-08 补充空值校验`
- 禁止注释代码块

### SQL
- 表名小写下划线（`ods_users`）；`SELECT` 禁止 `*`，必须显式列名
- `count(*)` 优先于 `count(1)`/`count(列名)`
- 分区字段：`dt`(天)、`hour`(小时)、`month`(月)

## 测试

- 工具类覆盖率 > 90%；断言用 AssertJ `assertThat(...)`，禁止 JUnit 原生断言
- Mock 用 Mockito；命名：`方法名_场景_期望结果`

## 构建

- 测试完成后 `mvn clean package` 保证正确构建

## IDE 与工程环境

- 禁止在已有项目内执行 `mvn archetype:generate`
