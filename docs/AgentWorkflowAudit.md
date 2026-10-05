# 仓库指令与工作流审计

原始审计日期：2026-09-05；本次复审：2026-10-05。范围：当前 `project-iot-benchmark` 仓库中的指令入口、技能和相关文档。现有 GitHub Actions 改动属于并行工作，本次未修改。未修改用户全局技能、模型设置、权限配置或其他仓库。

## 官方依据

- [GPT-6 Astra 模型指导](https://developers.openai.com/api/docs/guides/latest-model?model=gpt-6-astra)：关注授权范围内的持续执行、技能指令冲突、适量验证和按场景决定委派。仓库规则据此减少无条件流程，保留具体正确性约束。
- [GPT-6 Astra 技能与提示词复审](https://developers.openai.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra)：强调短入口、渐进披露、按任务读取文档、避免无条件重复测试，并明确完成边界。仓库规则保持模型无关，不把这些原则写成固定模型或推理档位要求。
- [AGENTS.md 加载机制](https://learn.chatgpt.com/docs/agent-configuration/agents-md)：根目录提供共享指令；不能假定 `CLAUDE.md` 在未配置 fallback 时自动成为 Codex 指令。
- [技能构建与发现](https://learn.chatgpt.com/docs/build-skills)：仓库技能放在 `.agents/skills/<name>/SKILL.md`，使用明确的 name/description 和按需加载。
- [Codex 最佳实践](https://learn.chatgpt.com/guides/best-practices)：指令保持简短、具体；复杂任务按需要规划，推理强度按任务选择。

以上是设计依据；本次没有进行模型性能对照实验，也没有证据证明某个固定推理档位能在所有仓库任务中取得最优表现。

## 发现与处理

### 本次复审

- `AGENTS.md` 改为按任务读取上下文：小型文档改动不再要求完整仓库阅读；保留状态、写集、未授权外部动作和历史计划边界。
- `CLAUDE.md` 明确 `AGENTS.md` 是唯一共享规则入口，避免双份规则漂移。
- `.agents/skills/iot-benchmark-development/SKILL.md` 保留测试、配置、适配器和 benchmark 运行的正确性陷阱，只压缩重复措辞，并把根规则链接作为按需入口。
- 本次只做静态文档优化；没有测量模型速度、质量、成功率、token 或费用变化。

| 原状态 / 问题 | 处理 |
| --- | --- |
| 无仓库 `AGENTS.md`、`SKILL.md`；规则集中在 7,299 字节的 `CLAUDE.md` | 原始审计新建公共 `AGENTS.md` 并让 `CLAUDE.md` 引用入口；本次复审继续按任务裁剪，当前两者合计 3,238 字节（字节比较，不是 token 或耗时测量） |
| 架构、启动步骤和配置列表重复 README / DeveloperGuide | 用任务相关链接替代复制的说明；保留 Java 17、模块分工、验证入口 |
| 默认全仓 clean/package、全仓格式化，易扩大小任务范围 | 按文档、核心行为、适配器和格式化选择检查；避免重复 validate、无理由 clean 和反复扩大测试 |
| 声称 ConfigDescriptor 通过反射加载配置，与 `loadProps()` 显式 setter 不符 | 技能明确字段、解析、默认配置三处联动，并验证配置实际生效 |
| 配置初始化导致测试 JVM 退出的陷阱容易在精简时丢失 | 保留 BenchmarkTestBase、静态初始化时机、共享状态恢复和实际测试执行数量检查 |
| 旧适配器说明遗漏部分枚举与构造器约束 | 路由到 DeveloperGuide，并保留 DBConfig 构造器、枚举/工厂/assembly 连接点 |
| 忽略目录中的旧计划要求特定 superpowers 技能、固定分步执行及提交 | 公共指令明确历史计划是资料；未改写或重新纳入版本控制 |
| core-test / spotless 无依赖缓存、无过时运行取消 | 增加 Maven 缓存及按工作流/ref 的并发组；保持触发分支、事件、任务名及检查范围；检查任务使用 contents: read |
| main.yml 去重条件写成 `steps.bm-info.last_commit`，且无需发布时 exit 1 | 用具名 should_release 输出决定是否执行 compile；相同提交或机器人提交正常跳过，下游发布历史写入随依赖跳过 |
| main.yml 多次运行可能并发写 release_history；保留废弃 IoTDB 源码构建注释 | 串行执行该发布工作流，不取消正在发布的任务；删除废弃注释，用 git 格式化输出读取作者名 |

## 保留项与范围边界

- `release_history_commit.yml` 是手动触发、固定旧 SHA 的历史发布流程，其中旧模块名称属于历史目标；没有把它改成当前模块列表或自动发布。已检查 YAML 与 shell 语法，未验证旧提交能否在当前 runner 上构建。
- `.claude/settings.local.json` 只有本机权限配置（146 个 allow 条目），无 hook 或模型配置；它被 Git 忽略，且不会成为 Codex 的仓库技能。未合并成通配授权，避免把去重变成扩大执行权限。
- 没有新增强制子代理、自动评审循环、定时任务、模型调用或固定最高推理强度。需要委派时仍遵守当前会话授权；模型选择由宿主设置控制。
- 发布流程仍使用原仓库、master、发布矩阵和历史提交目标。本次没有验证远端分支保护、令牌权限、发布服务或触发实际发布。

## 原始审计验证结果

- 官方 Skill Creator 的 `quick_validate.py`：通过。
- 四个 workflow：YAML 可解析；全部 run 脚本以表达式占位后通过 `bash -n`。
- 直接执行修改后的发布判断脚本：相同提交 false、机器人提交 false、新提交 true，三种情况通过。
- 对比修改前后：core-test / spotless 的触发事件、分支和任务名保留；指令链接解析通过；`git diff --check` 通过。
- 本次无 Java 源码变更，未运行 Maven 全套测试。没有安装 actionlint 或调用额外模型做评测；本地结构与分支验证不能替代 GitHub 运行结果。

## 本次复审验证

- 四个文档中的本地 Markdown 链接均解析到现有文件。
- `git diff --cached --check` 通过；只暂存了本次四个文档，现有 `.github/workflows/` 改动保持未暂存。
- 本次仍是静态文档审阅，没有运行 Maven，也没有测量模型速度、质量、成功率、token 或费用。

新任务会按官方发现机制读取仓库指令。若当前会话未显示新技能，可重新打开任务。后续可在相同任务、模型和环境下比较人工介入次数、无效工具调用、测试重复次数、完成时间及正确性，再决定是否继续调整。
