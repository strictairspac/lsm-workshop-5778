# 边缘遥测 LSM 合并工坊

北岭边缘计算小组的有序键值写入与合并演练：记录写入批次、配置层级策略、后台执行刷盘与合并，用最后写入字典 Oracle 核查读结果，防止墓碑误删导致旧值复活。

**这是 Python 确定性教学 LSM 模型。MySQL 8.0 仅保存业务元数据，不是对 MySQL/InnoDB 存储内核的实现或性能测试。** 页面与导出结果均保留此边界。

## 场景与模块

北岭边缘计算小组有 7 人，负责低内存遥测站点的存储方案验证。过去在电子表格记录写入序列、手工画 SSTable、用本地脚本核对合并结果，三个痛点是配置与操作序列混在一起、删除键的历史难复查、读写放大缺少一致口径。上游为站点模拟写入序列；本站替代输入归档、配置组合与可重现演练；下游是边缘存储评审人与值班交接。

- 写入批次：有序 put/delete，支持重复键、墓碑、完整操作预览、输入校验。
- 层级配置：memtable 不同键容量、SSTable 合并触发数、层级上限、最终完整合并开关。
- 合并队列：创建试验时持久化输入及配置快照，Redis 分发，独立 worker 执行并记录状态。
- 读放大证据：显示合并前后层级、读写放大、真实存活键、Oracle 摘要、刷盘/合并轨迹、JSON 导出。
- 在线迁移：从批次建立可写分片，选择键区间迁往新分片；迁移期间可覆盖、删键、范围查询；复制完成后再切换路由。
- 管理员审计：记录写操作账号、路由与 HTTP 结果，包含权限拒绝。

| 角色 | 页面与权限 | 典型动作 |
|---|---|---|
| admin 管理员 | 全业务页面与审计 | 登记批次/配置、执行、迁移、导出、查看审计 |
| operator 操作员 | 五业务页面，不可看管理员审计 | 新建输入与配置、提交 worker、推进迁移、检查与下载结果 |
| viewer 观察员 | 五业务页面只读与导出 | 阅读层级、Oracle 与归属证据，写请求在 API 层403 |

## 启动与账号

```bash
cp .env.example .env
docker compose config --quiet
docker compose up -d --build
# 默认启动自动执行；手动重跑不覆盖已存在的输入、配置、账号
docker compose exec -T backend python -m scripts.seed_demo
```

[前端入口](http://localhost:18245) · [后端接口文档](http://localhost:19245/docs)，数据库映射端口17745。

三个本地演示账号是 `admin@beiling.test` / `operator@beiling.test` / `viewer@beiling.test`，统一密码 `EdgeStore!17`。`.env.example` 提供固定本地数据库与JWT演示凭据，上线前替换。

```bash
# 只清理本项目与其演示卷
docker compose down -v --remove-orphans
```

## 09:00—10:00 的工作剧本

1. 09:00 operator 登录，打开“北岭站点-早班写入”：12条操作，8个不同键。
2. 09:05 查看 sensor-01 的18.2→18.8→19.0覆盖记录，sensor-02和sensor-03有删除墓碑。
3. 09:10 新建 `shift-20261117` 输入，或复用“旧值复活回归集”。
4. 09:15 选择“小内存-完整合并”：memtable键容量2，表触发数2，3层，最后完整合并。
5. 09:20 创建试验并提交worker，在队列刷新至“Oracle一致”。
6. 09:25 打开读放大证据，确认早班输入最终6个存活键，sensor-02/03未复活。
7. 09:30 下载 `lsm-trial-<id>.json`，包含操作/配置快照、层级、轨迹与校验和。
8. 09:40 viewer复核，创建试验被403拒绝；10:00 admin检查操作审计。

### 站点扩容：热点键区间在线搬迁（同一上午，10:10—10:40）

1. 10:10 operator 打开“在线迁移”，选“北岭站点-分片搬迁演练”批次，初始 2 片，区间 `edge-k06 … edge-k09`。
2. 10:12 建立后分片立即接受覆盖与删键；先删 `edge-k06`，确认写入口与路由区间。
3. 10:15 点“推进一个复制批次”两次，观察复制中状态、游标与目标分片暂存版本。
4. 10:20 复制前/中写入 `edge-k07`，再“续迁至复制完成”，验证重复副本只保留最高序号。
5. 10:25 切换路由；页面归属列从 S2 变为目标分片，区间外键仍归原分片。
6. 10:30 切换后再覆盖 `edge-k07`、删 `edge-k09`；范围查询全区间，结果按键升序、不重不漏、墓碑不复活。
7. 10:35 下载 `migration-<id>.json`，包含分片、路由、实测快照与校验和。
8. 10:40 重启 backend 后重放“续迁/切换”只返回 noop；迁移历史记录保留复制与切换步骤。

`backend/tests/` 下的 pytest 覆盖复制中删键、跨分片查询、切换与写入相撞、重启续迁与幂等重试，原批次/试验流程另有回归测试。

Seed另含“旧值复活回归集”和“全部删除边界集”；配置另含“保留层级观测”和“宽缓冲-完整合并”。账户、批次、配置以自然键查缺插入，重复seed不改变已存在字段。

## 核心算法与指标口径

输入为有序操作数组和参数配置：

- 操作数1–2000；键长1–64；put的value必须是1个不超过200字符的字符串（允许空字符串）；delete的value忽略。
- memtable_limit范围2–64，按不同键数量计容量；l0_trigger范围2–8；max_levels范围2–6。
- 每个用户操作分配单调序号。memtable同键覆盖；达到容量后按键排序刷成L0 SSTable。末尾不足容量也刷盘。
- 某层表数达到触发值时，合并该层所有表到下一层；相同键按最高序号保留记录。最深层在原层合并。**层级合并始终保留墓碑**，不会提早清理。
- 若打开最终完整合并，收集全部层级所有表，按序号合并完整历史后才移除墓碑，存放在最深层。不开启时保留中间层级与墓碑。
- 点查遍历范围覆盖键的SSTable，在候选记录中选择最高序号；带墓碑则不返回。固定探测集合为全部输入键加一个不存在的探针键。没有Bloom过滤器模型。
- 读放大 = 固定点查集合的范围命中SSTable读取总数 / 探测键数；合并前后使用相同集合。写放大 = flush/各次compaction真实输出条目总数 / 用户操作数，**不是物理字节或磁盘I/O**。
- Python另从原始操作顺序构造最后写入字典Oracle；合并前、合并后均必须与Oracle一致，否则试验failed。结果与Oracle各自对有序JSON求SHA-256；同输入/配置结果确定，无随机时钟项。

输出包括真实计算的层级表数/键范围/墓碑数、每步刷盘合并轨迹、输入键数、存活键、写出条目、读写放大、Oracle及实际键值、摘要。空输入和超限在提交时422，引用不存在404，重名409，排队/运行总数超过20时429；worker失败保存error和failed状态。

## 在线键区间迁移口径

- 批次的不同键按字节序切成 2–4 个可写来源分片，另建一个目标分片。搬迁区间必须完整落在一个来源分片内；区间用端点值记录，语义是闭区间 `[起点, 终点]`。路由保存显式的开闭标记，切换后目标分片精确持有 `[起点, 终点]`，两侧余下的区间仍归来源分片，因此不会为 64 字符键构造不存在的后继键。
- 分片内每个键保存 `(key, seq, value, deleted)` 版本；覆盖和删键都追加版本，墓碑不会被复制或查询省略。写入序号全局单调，`migration_writes` 记录每次已确认写入及其发生阶段（批次初始 / 切换前 / 切换后）。
- 复制按有序键批次推进，最多 3 个键一批，游标 `copy_cursor` 与状态持久化在 MySQL。“续迁”从游标继续，只补 `(shard,key,seq)` 尚不存在的版本，因此中断重试不会重复搬迁；已切换后推进返回 noop。
- 复制完成后才允许切换。切换先在同一事务里把来源分片中区间内的全部版本补齐到目标分片，再整体替换路由；它与写入口共用迁移行锁，所以切换提交前的写入落在来源并会被补齐，切换提交后的写入直接落目标，两者都保留。路由与目标数据同事务提交。
- 查询在发起时先取 `watermark = next_seq`，再读取所有分片里 `seq ≤ watermark` 的版本，按键取最高序号并合并重复副本；墓碑版本不返回，因此旧来源副本上的 put 不会让删键复活。返回按键升序、不重不漏，并与 `migration_writes` 的 Oracle 逐键核对，不一致直接 500 而不是给出可疑结果。
- 归属查询返回当前路由区间、区间所有者与目标分片暂存版本；导出 JSON 含迁移详情、归属与一次实测快照。
- 边界：教学模型，不是真实分片集群。没有网络复制延迟、双写协议、路由版本号发布、重新分片回滚或跨迁移链式搬迁；单个迁移只搬一个连续区间，来源与目标都保留历史版本作为证据。

## 技术栈与代码组织

| 服务 | 实现与作用 |
|---|---|
| frontend | 原生HTML/CSS/JS ES模块，Nginx1.27，土橙材料样本册视觉 |
| backend | Python3.11、FastAPI、SQLAlchemy、PyMySQL、JWT与PBKDF2密码 |
| worker | 独立Python进程，Redis BLPOP，LSM演算，数据库恢复队列，心跳健康检查 |
| db | MySQL8.0，持久化3个业务实体、用户与审计 |
| redis | Redis7，异步队列与持久卷 |

`backend/src/`分为config/models/schemas/services/controllers/routes/middlewares/utils/constants；`backend/scripts/seed_demo.py`种子与初始化；`backend/migrations/README.md`为schema v1/v2说明。3实体dataset/config/trial各自贯穿model/schema/service/route、前端api/store/page；在线迁移另加migration/shard/version/write 4张表，贯穿model/schema/service/route与前端api/store/page，详情页为独立业务页面。试验结果保存在Trial JSON字段中，迁移状态与路由保存在Migration JSON字段中。

`frontend/src/`包含api/stores/types/components/common/hooks/pages/router/utils/constants，表格/状态/页头跨页共享，HTML转义与反馈工具复用。角色/任务状态/操作类型枚举在`backend/src/constants/enums.py`和`frontend/src/constants/enums.js`。模型默认任务状态使用后端枚举，前端状态组件使用前端枚举。

Docker恰好5服务，全部healthcheck；backend依赖db/redis healthy，worker还等待backend完成seed；frontend等待backend healthy。命名卷保存MySQL、Redis及worker证据。Nginx `/api/`剥前缀转发后端。

环境变量：COMPOSE_PROJECT_NAME、FRONTEND_PORT、BACKEND_PORT、DB_PORT、MYSQL_PASSWORD、MYSQL_ROOT_PASSWORD、JWT_SECRET、SEED_DEMO；容器连接还配置DATABASE_URL/REDIS_URL。端口可在.env整体调整避免冲突。

本地开发推荐修改代码后`docker compose up -d --build`。也可用虚拟环境安装backend/requirements.txt，启动Compose db/redis，设置可达DATABASE_URL/REDIS_URL/JWT_SECRET，进入backend执行`uvicorn src.main:app --reload`；worker须有可写`/artifacts`并执行`python -m src.worker`。

```bash
# 迁移验收测试：使用临时 SQLite，不触碰 Compose 中的 MySQL/Redis
cd backend && pip install -r requirements-dev.txt && python -m pytest tests -q
```

## 已知边界

- 存储过程为内存教学模型，忽略磁盘块、压缩、Bloom过滤器、WAL持久化、并行compaction与真实故障恢复。MySQL负责保存网页业务输入和输出。
- 在线迁移同样不是生产级分片迁移：单 backend 进程靠进程内锁，多副本部署时只依赖 MySQL 行锁，未实现路由版本广播、复制流或回滚。
- 前端创建独立批次/配置，不提供原记录编辑删除；新版本新建记录，试验总是固定输入快照。
- 最终全历史合并允许去掉墓碑；没有模拟跨副本历史/快照读，因此不能将该规则直接用于生产数据库。迁移的跨副本读只按键取最高序号，不提供时间旅行快照。
- 单worker与按钮刷新；不提供多worker分布式锁、SSE。迁移复制也由按钮逐批推进，没有后台自动搬运。
- Web双字体来自Google Fonts，离线回退到serif/monospace。

MIT License，见LICENSE。实际测试与截图位于批次的`test-artifacts/gb-4106/`，源码内不保存测试证据。
