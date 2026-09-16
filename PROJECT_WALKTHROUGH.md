# 🏥 MediCareAI-Agent · 项目详细讲解文档

> 逐方法 / 逐字段级实现剖析 · 配套既有 `docs/*.mdx` 使用
>
> 编写时间：2026-09-16 · 基于仓库当前源码（非 `PROPOSAL.md` 设计稿）

本文档对 MediCareAI-Agent 后端与前端做一次**精确到方法 / 字段**的实现讲解。所有内容直接来自源码精读；凡发现与 `docs/` 既有文档不一致之处，均在对应小节与文末第 16 章统一标注，请以本文档（源码事实）为准。

---

## 目录

1. [系统总览](#1-系统总览)
2. [目录结构与模块职责](#2-目录结构与模块职责)
3. [基础设施层（逐方法）](#3-基础设施层逐方法)
4. [数据模型与 Schema（逐字段）](#4-数据模型与-schema逐字段)
5. [Agent 与 LLM 引擎（逐方法）](#5-agent-与-llm-引擎逐方法)
6. [RAG 与文档管线（逐方法）](#6-rag-与文档管线逐方法)
7. [API 路由层（逐端点）](#7-api-路由层逐端点)
8. [Celery 任务与工具层](#8-celery-任务与工具层)
9. [服务层（audit / config / email）](#9-服务层audit--config--email)
10. [前端概览](#10-前端概览)
11. [关键数据流与调用链](#11-关键数据流与调用链)
12. [配置与环境变量](#12-配置与环境变量)
13. [部署](#13-部署)
14. [已实现 vs 未实现（TODO）](#14-已实现-vs-未实现todo)
15. [快速上手](#15-快速上手)
16. [源码与既有 docs 不一致清单（重要）](#16-源码与既有-docs-不一致清单重要)

---

## 第二部分 · 代码级深读（真实代码 + 分支走查）

17. [认证、权限与配置链（代码级）](#17-认证权限与配置链代码级)
18. [LLM 服务（代码级）](#18-llm-服务代码级)
19. [Agent 编排与诊断引擎（代码级 · 系统大脑）](#19-agent-编排与诊断引擎代码级-系统大脑)
20. [诊断 SSE 流水线（代码级）](#20-诊断-sse-流水线代码级)
21. [RAG 与工具（代码级）](#21-rag-与工具代码级)
22. [关键降级与容错总表（代码级）](#22-关键降级与容错总表代码级)

---

## 1. 系统总览

MediCareAI-Agent 是一个**多 Agent 自主医疗协作系统**：患者用自然语言描述症状、上传化验单，系统通过多 Agent 编排完成「采集病史 → 检索医学证据 → 鉴别诊断 → 结构化诊断报告 → 制定方案 → 随访监测」，并在必要时转交真人医生。

**技术栈（实际源码）**

| 层级 | 技术 |
|------|------|
| 后端 | Python 3.12 + FastAPI + SQLAlchemy 2.0（async）+ Alembic |
| 任务队列 | Celery + Redis（broker db1 / backend db2；应用缓存用 db0） |
| 数据库 | PostgreSQL 17 + pgvector（注：pgvector 列**未启用**，见 16.5） |
| AI/LLM | OpenAI 兼容多提供商（kimi / GLM / DeepSeek / Qwen），配置存加密 DB 表 |
| 前端 | React 19 + TypeScript + Vite + MUI（见第 10 章） |
| 部署 | Docker Compose（6~8 服务） |

**核心架构：三轨问诊 + 诊断后对话**（详见 `docs/architecture.mdx` 与第 11 章）

```
患者主诉/图片
   → route_stream (SSE 诊断管道)
   → MasterAgent 意图识别 → diagnosis
   → DiagnosisAgent.interview() 生成 InterviewState
   ┌────────────┬─────────────────┬─────────────────┐
   │ Track1 病史 │ Track2 搜索增强  │ Track3 多模态解析 │
   └─────┬──────┴────────┬────────┴────────┬────────┘
         └──── 三层去重 → decide_next() 判 synthesize
   → run_full_diagnosis_workflow() 输出 DiagnosisReport
   → （注册用户）自动建 MedicalCase
   → POST /sessions/{id}/chat 进入诊断后对话（方案 B+C）
```

设计铁律（来自 `architecture.mdx`）：`dedup 空 + pending 空 + lab 完整 → synthesize`（唯一出口）。已取消 `escalation`、`_assess_sufficiency`、`MIN_QUESTIONS` 等旧逻辑。

---

## 2. 目录结构与模块职责

```
MediCareAI-Agent/
├── backend/app/
│   ├── main.py                 # FastAPI 入口、路由/中间件挂载、lifespan、健康检查
│   ├── core/                   # config(Settings) / security(JWT+bcrypt) / encryption(Fernet) / logging
│   ├── db/                     # session(async engine) / redis_client / migrations(Alembic)
│   ├── api/v1/                 # 16 个路由文件（auth/users/patient/doctor/admin/.../agents/llm/rag/...）
│   ├── models/                 # 14 个 ORM 模型（user/agent/interview/medical_case/rag/config/notification/audit/email）
│   ├── schemas/                # 7 个 Pydantic schema 包（请求/响应）
│   ├── services/               # 业务核心：agents / orchestrator / llm / rag / embedding / reranker /
│   │                           #   external_search / document_parser / document_router /
│   │                           #   multimodal_parser / audit / config / email_service
│   ├── tasks/                  # Celery 任务：agent/audit/health/monitoring/planning/celery_app
│   ├── tools/                  # Agent 工具：base / registry / medical（search_medical_knowledge 等）
│   └── tests/                  # pytest 用例
├── frontend/                   # React 前端（源在 frontend/src）
├── docs/                       # 既有架构/前端/数据库/部署/待办文档
├── nginx/ searxng/             # 反向代理 / SearXNG 配置
├── Dockerfile docker-compose.yml docker-compose.prod.yml
└── README.md PROPOSAL.md
```

> 关键事实：`app/agent/__init__.py` 只是包标记，**真正的 Agent 实现在 `services/agents.py`**；`services/orchestrator.py` 是「三轨问诊决策引擎」（Track1/Track2 + `decide_next`），**不负责**意图路由（意图路由在 `AgentOrchestrator.route`，同样在 `services/agents.py`）。不要用文件名望文生义。

---

## 3. 基础设施层（逐方法）

### 3.1 `main.py` — 应用入口
- `settings = get_settings()`：进程级缓存的 `Settings` 单例。
- `configure_logging(debug)`：导入即配置结构化日志。
- `sentry_sdk.init(...)`：仅 `is_production and sentry_dsn` 时初始化（采样 `traces 0.1 / profiles 0.05`）。
- `_ensure_default_admin()` → `None`：无 ADMIN 时建默认管理员（`password_change_required=True`）；无 `admin_password` 则跳过。
- `lifespan(app)`（`@asynccontextmanager`）：进入时 `_ensure_default_admin()` + `ensure_default_templates()`（监控模板），退出无逻辑。
- `health_check()` `GET /health`：存活探针 `{status,version,env}`，不连外部依赖。
- `readiness_check()` `GET /ready`：分别探 DB（`SELECT 1`）与 Redis（`ping()`），单个不可用不整体 500。
- `app = FastAPI(lifespan=...)`：仅开发环境暴露 `/docs` `/redoc`；挂载 `CORSMiddleware`（生产禁用 `*`）、`ProxyHeadersMiddleware`（信任 Nginx `X-Forwarded-Proto`）；`include_router(v1_router, prefix="/api/v1")`。**未注册自定义异常处理器**。

### 3.2 `core/config.py` — 配置
- `class Settings(BaseSettings)`：从 `.env` 读取（`extra="ignore"`）。敏感值全部来自环境变量，无硬编码。关键字段：`secret_key`(必填)、`database_url`(PostgresDsn)、`redis_url`、`celery_broker_url/result_backend`、`default_admin_email/password`、`api_key_master_key`、`cors_origins_raw`(别名 `CORS_ORIGINS`)、`rag_chunk_size/overlap` 等。
- `@property cors_origins`：逗号切分；**生产含 `*` 则降级为 `cors_fallback_origin`**。
- `@property async_database_url` / `is_production` / `is_development`。
- `get_settings()` `@lru_cache`：单例。

### 3.3 `core/security.py` — 密码与 JWT
- `verify_password(plain, hashed)` / `get_password_hash(pwd)`：bcrypt 校验/哈希（`rounds=12`）。
- `create_access_token(subject, platform, expires_delta)`：HS256，默认 7 天，`type:"access"`。
- `create_guest_token(guest_session_id, fingerprint, platform)`：访客 token，`type:"guest"`，1 天。
- `decode_token(token)`：解码；过期 `ExpiredSignatureError`、非法 `InvalidTokenError`。

### 3.4 `core/encryption.py` — 静态加密
- `derive_fernet_key(master)`：sha256 → base64 得 32 字节 Fernet key。
- `_get_cipher()`：从环境变量 `API_KEY_MASTER_KEY` 取主密钥；缺失抛 `RuntimeError`。
- `encrypt_value(plain)` / `decrypt_value(cipher)`：Fernet 加/解密；**解密异常吞掉返回 `None`**（安全降级）。
- `mask_api_key(key)`：脱敏 `sk-****xxxx`。

### 3.5 `core/logging.py`
- `configure_logging(debug)`：dev 用 `ConsoleRenderer`，prod 用 `JSONRenderer`；抑制 `openai/httpx/httpcore` 噪声。
- `get_logger(name)`：返回 structlog `BoundLogger`。

### 3.6 `db/session.py`
- `async_engine`：`create_async_engine(settings.async_database_url, echo=dev, pool_pre_ping=True, pool_size=10, max_overflow=20)`。
- `AsyncSessionLocal = async_sessionmaker(..., expire_on_commit=False, autoflush=False)`。
- `Base = declarative_base()`。
- `get_db()`：FastAPI 依赖，异步生成器，请求结束 `session.close()`。

### 3.7 `db/redis_client.py`
- `get_redis()`：懒初始化单例，`redis.from_url(..., decode_responses=True)`。用途：token 黑名单、监控 `diag_lock` 等。

### 3.8 `api/deps.py` — 认证依赖（核心链路）
- `oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login", auto_error=False)`。
- `class UserContext`（`@dataclass`）：`user/platform/is_guest/guest_id`。
- `_resolve_token(token, db)`：解析核心——黑名单校验（sha256 → Redis `token_blacklist:{hash}`）→ `decode_token` → 按 `type` 分支（guest / access 查库，非 active 抛 403；过期/非法按无 token 处理，因 SSE 无法刷 token）。
- `get_current_user_or_guest(...)`：**严格版**，token 优先级 `Bearer > X-Guest-Token > URL query > Cookie`（query 优先于 cookie 是 SSE 刻意设计）。
- `get_current_user(...)`：仅已认证用户。
- `get_current_platform(...)` / `require_platform(*allowed)`（工厂，403）/ `require_role(*roles)`（工厂，403）/ `get_current_user_or_guest_lenient(...)`（SSE 宽松版，失败不抛）。

### 3.9 `tasks/celery_app.py` — Celery 应用
- `celery_app = Celery("medicareai_agent", broker=..., backend=..., include=[...])`：显式 `include` 5 个任务模块。
- `conf.update`：`task_serializer/json`、`timezone=Asia/Shanghai`、`task_time_limit=300`、`worker_prefetch_multiplier=1`、`visibility_timeout=2592000`（支持数周 ETA）。
- `beat_schedule`：`cleanup-audit-logs-daily`（每日 03:00）、`scan-pending-events`（每 30 分钟兜底扫描错过的 ETA 提醒）。

---

## 4. 数据模型与 Schema（逐字段）

> 完整字段表见子代理精读（已含全部列名/类型/约束/关系）。此处给出**结构与关键关联**，并标注与 `docs/database.mdx` 的差异（差异汇总见第 16 章）。

### 4.1 枚举全集
`UserRole`(patient/doctor/admin)、`UserStatus`(active/inactive/pending)、`AgentSessionStatus`(active/completed/escalated/failed/timeout)、`AgentSessionType`(diagnosis/planning/monitoring/consultation/conversation)、`CaseStatus`(pending_review/in_progress/awaiting_patient/resolved/archived)、`DocumentType`(report/prescription/image/lab_result/discharge_summary/other)、`DocType`(platform_guideline/case_report/drug_reference)、`ReviewStatus`(pending/agent_reviewed/doctor_reviewed/approved/rejected/revision_requested)、`NotificationType`(system/announcement/direct/reminder)、`NotificationPriority`(high/medium/low)、`AuditActionType`(21 个)、`AuditResourceType`、`SmtpSecurity`(starttls/ssl/none)、`EmailSendStatus`、`InterviewPhase`(23 个临床维度 + complete，用于 `collected_info` 的 key)。

### 4.2 核心模型
- **`User`**(`users`)：双身份（同 email+不同 role 唯一）；字段含凭据、角色、医生资质(`license_number/hospital/department/title`)、注册资料、各类令牌(`verification_token/doctor_confirmation_token`)、`password_change_required/email_verified`。
- **`GuestSession`**(`guest_sessions`)：`session_token/message_count/max_messages(默认 999)/expires_at`。
- **`UserAttachment`** / **`RoleSwitchLog`**：资质附件（管理员逐文件审核）/ 角色切换审计。
- **`AgentSession`**(`agent_sessions`)：Agent Memory Layer 2。`context`(JSONB，存 `InterviewState` 等)、`tool_calls`(JSONB)、`structured_output`(JSONB 诊断报告)、`parent_session_id`(自引用，诊断后对话)、`escalated_to`。
- **`AgentTask`** / **`PatientHealthProfile`**(一对一，Layer 3 长期档案) / **`CarePlan`** / **`MonitoringEvent`**：会话内任务 / 患者健康档案 / 随访计划(JSONB 任务 DAG) / 定时提醒事件。
- **`MedicalCase`**(`medical_cases`) + **`MedicalDocument`**：方案 C 分层模型，`source_session_id` 回指诊断会话，冗余 `chief_complaint/ai_diagnosis_summary/severity/is_emergency`。
- **`Document`**(`documents`) + **`DocumentChunk`** + **`DocumentReview`**：知识库文档（embedding 存 `embedding_json` JSONB）、分块、审核流水。
- **`LLMProviderConfig`**(`llm_provider_configs`)：加密 `api_key_encrypted`；`(provider,platform,model_type)` 唯一。`platform=NULL` 表示全局。
- **`SystemSetting`** / **`Notification`** / **`AuditLog`** / **`EmailConfiguration`** / **`EmailTemplate`** / **`EmailLog`**：键值设置 / 站内信(双方软删+广播) / 审计日志(不含密钥明文) / SMTP 加密配置 / 邮件模板 / 发送日志。

### 4.3 关键关联链路
```
User ─< AgentSession (user_id)
        ├─< AgentTask (session_id)
        ├─< AgentSession (self: parent_session_id)  // 诊断 → 诊断后对话
        └─< MedicalCase (source_session_id)
User ─< MedicalCase (patient_id) ─< MedicalDocument (case_id)
User ─< PatientHealthProfile (一对一) ─< CarePlan ─< MonitoringEvent
Document ─< DocumentChunk / DocumentReview
```

### 4.4 Schema 约定
- `Response` 变体带 `model_config=ConfigDict(from_attributes=True)` 用于 ORM 序列化。
- **密钥永不返回**：`LLMProviderConfigResponse.api_key_masked="***"`、`EmailConfigResponse` 不返回密码。
- `Update` 变体字段基本全可选。

---

## 5. Agent 与 LLM 引擎（逐方法）

> 设计：中心辐射式 `AgentOrchestrator`（总调度）持有 5 个平级专科 Agent，**无子 Agent 递归树**，路由时串行调用。

### 5.1 `services/agents.py`
**结构化输出 Schema**：`DifferentialDiagnosis`、`DiagnosisReport`(主产物：primary_diagnosis / differential / confidence / severity / red_flags / knowledge_sources / …)、`TreatmentPlan`、`MonitoringAssessment`、`ResearchResult`、`AgentResult`(统一返回)。

- **`MasterAgent`**：`classify_intent(user_input, session_context)` — LLM 把输入分为 6 类意图（diagnosis/planning/monitoring/consultation/research/general）；带上下文时动态拼接诊断摘要。
- **`DiagnosisAgent`**：
  - `analyze(symptoms, patient_id?, patient_history?, test_results?, session_id?, knowledge_context?)` — 主诊断：多轮 Tool Use（≤5 轮，第 4 轮 `tool_choice="none"`）→ 三重兜底结构化解析 `DiagnosisReport` → 持久化。
  - `_generate_search_query(symptoms)` — 口语主诉转 SearXNG 检索词。
  - `_update_session(...)` — **只写 tool_calls/structured_output/status**，刻意不碰 `context`（防竞态）。
  - `interview(session_id, collected_info?, ...)` / `interview_answer(session_id, question_id, answer, ...)` — 问诊初始化/作答，内部用 `DynamicInterviewEngine.decide_next`。
  - `run_full_diagnosis_workflow(...)` — 先 SearXNG 搜知识（60s 超时降级）再 `analyze`；搜索工具调用插入 `tool_calls_used` 供前端可见。
  - `_update_interview_state(session_id, state)` — **相位降级守卫**：DB 已 `completed` 则拒绝写入；整字典重赋值触发 JSONB 变更检测。
- **`PlanningAgent`**：`plan(diagnosis, patient_profile?, ...)`（生成 `TreatmentPlan`）；`generate_health_profile(patient_id)`（查 MedicalCase → LLM 摘要 → 写 `PatientHealthProfile`）；`generate_care_plan(session_id, patient_id)`（建 `CarePlan` + 拆 `MonitoringEvent` → `send_reminder.apply_async(eta=...)` 入 Celery）。
- **`MonitoringAgent`**：`check(patient_updates, ...)` → `MonitoringAssessment`。
- **`ResearchAgent`**：`research(query, patient_context?)` — `ExternalSearchAgent` 搜指南/药物/论文 → `_format_results`（带可信标记）→ `ResearchResult`；`_detect_search_type`(关键词) / `_format_results`。
- **`AgentOrchestrator`**（**真正的意图路由+会话创建**）：
  - `route(user_input, patient_id?, patient_history?)` — `classify_intent` → `_create_session` → 按 intent 调对应 Agent（diagnosis 建 task 记录；general/consultation 串行 diagnose→plan→monitor）。
  - `_create_session(user_id, session_type, intent?)` — 建 `AgentSession`（`context={"messages":[],"collected_info":{}}`，JSONB 源头）。
  - `_create_task(...)` — `AgentTask` 审计记录。
  - `_escalate_session(...)` — 标记 ESCALATED（当前路由已不调用）。

### 5.2 `services/orchestrator.py` — 三轨问诊决策引擎
- **`Track1Agent`**：`generate(state, patient_history?)` — 病史采集，LLM 决定下一轮基础问题；`_to_templates`(去重/补选项)。
- **`Track2Agent`**：`generate(state, search_results, diffs)` — 搜索增强的靶向问题（结果过短直接返回 `[]`）。
- **`InterviewOrchestrator`**：
  - `decide_next(state, patient_history?, knowledge_context="")` — **唯一合成出口**：Phase1 并行 `track1 + search`；`track2` 增强；三层去重（同轮互斥 / `phase_key` 静态 / LLM 语义）；选项补全；`deduped` 空 + pending 空 + lab 完整 → `action="synthesize"`；否则 `action="ask"`。
  - `_complete_options(q)`（LLM 生成 3-6 选项）、`_run_search`（30s 超时）、`_semantic_dedup`（LLM 判语义重复）、`_is_diagnosis_active`（Redis `diag_lock`）、`_deduplicate`（按 phase_key 最多 2 条）。

### 5.3 `services/llm.py` — 统一 LLM 客户端（多提供商解耦）
- `_get_provider_config(db, provider, platform?, model_type="diagnosis")` — 4 级优先级解析（精确→全局+类型→兼容→全局兜底），`decrypt_value` 解密 key；无配置抛 `ValueError`。
- `_get_default_provider(db, ...)` — 按 `is_default` 选 provider。
- `LLMService`：
  - `_get_client()` — `AsyncOpenAI(base_url, api_key, timeout=120, max_retries=2)`（**OpenAI 兼容**，故 kimi/GLM/DeepSeek/Qwen 皆可用）。
  - `chat(messages, ..., disable_thinking=True)` — **非流式**（chat 端点与诊断后对话实际用此）。
  - `chat_stream(...)` — 流式（基本弃用，仅 `route_stream` 其他意图分支）。
  - `chat_with_tools(messages, tools, tool_choice="auto")` — function calling。
  - `generate_structured(messages, output_schema, ...)` — JSON Schema 结构化（`DiagnosisReport` 等用）。
  - `chat_vision(text_prompt, image_bytes, ...)` — 多模态（base64，适配 Kimi 视觉 API）。
  - `health_check()` — `client.models.list()`（即 provider 测试接口）。
  - `get_llm_service(db, platform?, model_type?)` — 工厂。
- **无 provider 降级策略**（TODO，见 14）。

### 5.4 `api/v1/agents.py` — SSE / 会话端点
模块辅助：`_session_lab_bridge`(内存桥)、`_inject_lab_context`、`_load_health_profile_context`、`_build_conversation_context`(方案 B 会话内上下文)、`_build_chat_context`(方案 C)、`_diagnosis_report_to_markdown`、`_chunk_text`。

端点：
| 方法+路径 | 行为 |
|---|---|
| `POST /route` | `AgentOrchestrator.route`（非流式） |
| `POST /diagnose` `/plan` `/monitor` `/consult` | 各 Agent 直接调用（非流式） |
| `GET /sessions` `GET /sessions/{id}` | 管理员/医生列会话、取全量 |
| `POST /sessions/{id}/lab-reports` | 化验单桥（UUID→`_frontend_sid` 归一化，单写入者） |
| `GET /route/stream` | **SSE 主诊断流**：thinking→intent→agent_switch→interview/diagnose→tool_call/tool_result→structured→text→complete；访客自动建 GuestSession |
| `GET/POST /route/stream/continue` | **SSE 续问诊**（答案取 `X-Answer` 头/base64） |
| `POST /sessions/{id}/chat` | **SSE 诊断后对话（方案 C）**：建 `CONVERSATION` 子会话(`parent_session_id`)→`_build_chat_context`→`llm.chat`→分块 yield |

### 5.5 `api/v1/llm.py`
`POST /chat`(非流式 `ChatResponse`)、`POST /chat/stream`(SSE)、`GET /health`(provider 测试 = `LLMService.health_check`)。

---

## 6. RAG 与文档管线（逐方法）

### 6.1 整体管线（以源码为准）
- **入库** `RAGService.create_document`：写 `Document` → 生成 `search_vector`(tsvector，但**未在检索使用**，见 16.4) → `_chunk_text` 分块 → 批量 `EmbeddingService.embed` → 结果以 `str(list)` 写 `embedding_json`(JSONB) → 置 `vectorized_at/chunk_count`。
- **检索** `RAGService.search` 三阶段：① ILIKE 粗排 → ② Python 余弦相似度向量重排(`cosine_similarity`) → ③ `RerankerService.rerank` 精排；每阶段 provider 未配置即降级为上一阶段。
- **生成** `generate_answer`(LLM 合成) / `query`(search+generate，返回 answer+sources+chunks)。

> **embedding 真相**：存 `DocumentChunk.embedding_json`(JSONB)，`Vector(1024)` 仅注释；相似度是纯 Python 循环，**非 pgvector 索引检索**（16.5）。

### 6.2 逐文件
- **`embedding.py` `EmbeddingService`**：`_resolve_provider`(查 `model_type='embedding'` 配置)、`embed(texts)`(OpenAI 兼容 `/embeddings`)、`health_check`、`cosine_similarity`(纯 Python)。无配置抛 `ValueError`（降级源头）。
- **`reranker.py` `RerankerService`**：`rerank(query, documents, top_n)` — 调自定义 `/rerank` 端点（**非硬编码 Cohere**，模型由 DB `model_type='reranking'` 配置）；未配置返回恒等透传。
- **`external_search.py` `ExternalSearchAgent`**(SearXNG，只取不存)：`search_guidelines/search_drug_info/search_papers`、`healthcheck`、`_searxng_search`(异常返回 `[]`)、`_filter_trusted`(权威域白名单累加打分，`is_trusted=score>=50`)。
- **`document_parser.py`**：`_get_file_type`(MIME/扩展名)、`_parse_pdf/_parse_docx/_parse_txt`(gbk 兜底)、`parse_uploaded_file`(≤20MB，返回 (text,type))。**只抽取文本，不 OCR/不分块/不向量化**。
- **`document_router.py`**：`classify_file`(TEXT_FORMATS/IMAGE_FORMATS → FILE_EXTRACT/IMAGE_VISION)、`is_image_format/is_text_format`、`UnsupportedFormatError`。
- **`multimodal_parser.py` `LabReportParser`**(化验单多模态)：`parse_image(image_data, file_id)` → `LabReportResult`(指标含 LOINC 映射 `LOINC_MAP`、异常判定 `DEFAULT_REFERENCE`、置信度)；`_normalize_indicator/_try_numeric/_lookup_loinc/_check_abnormal/_extract_json_array`；`CONFIDENCE_THRESHOLD=0.7` 以下标 `requires_manual_review`。

### 6.3 端点
- **`rag.py`**：`POST /documents`(ADMIN/DOCTOR，建+索引，默认 `review_status=APPROVED`)、`POST /query`(登录/访客，检索+生成)、`GET /search`(原始 chunk)。
- **`documents.py`**：`POST /upload`(后台多模态解析)、`GET /{file_id}/result`(轮询)、`DELETE /{file_id}`。**实为「上传→解析→轮询」，非文档 CRUD/审核**（16.2）。解析任务存进程内字典 `_parse_tasks`（重启即丢，TODO）。
- **`upload.py`**：`POST /upload`(本地落盘附件 `UPLOAD_DIR`，≤10MB)、`GET /uploads/{filename}`。

### 6.4 与 Agent 的接口：`GLOBAL_REGISTRY.register("search_medical_knowledge", ...)`
`SearchMedicalKnowledgeTool.execute(query, doc_type?, top_k=10)` 合并 `RAGService.query`(内部知识库) + `ExternalSearchAgent`(SearXNG 外部)，去重后返回来源。被 `DiagnosisAgent`/`Track2Agent` 通过 `GLOBAL_REGISTRY.list_schemas()`(给 LLM) + `execute()` 调用。

---

## 7. API 路由层（逐端点）

> 所有 `APIRouter()` 未设 prefix，由 `main.py` 以 `/api/v1/<模块>` 挂载。权限依赖来自 `deps.py`。

### 7.1 `auth.py`
`POST /register`(患者/医生，医生待审核)、`POST /register/doctor`(表单+资质文件本地存储)、`GET /doctor-confirm`(医生邮件确认)、`GET /verify-email`(患者邮箱验证)、`POST /login`(OAuth2 密码流，多身份同 email 按 `ADMIN>DOCTOR>PATIENT` 选)、`POST /change-password`、`POST /guest`(限时访客+JWT)、`POST /switch-role`(患者↔医生，记 `RoleSwitchLog`)、`GET /guest/status`、`GET /me`、`POST /logout`(token 入 Redis 黑名单)、`DELETE /guest`(清理)、`POST /guest/migrate`(会话迁移)、`POST /resend-verification`、`PATCH /auth/me`、`POST /users/me/attachments`(走 OSS)、`GET /users/me/attachments`。

### 7.2 `users.py`
**空文件**：用户管理端点实际在 `admin.py`。

### 7.3 `patient.py`（全部需登录）
`GET/PATCH /profile`、`GET /cases`、`GET /care-plans`、`GET/PATCH /care-plans/{id}`、`POST /care-plans/{id}/ack`、`GET /reminders`、`PATCH /reminders/{id}/acknowledge`、`POST /check-in`、`GET /reminders/count`、`POST /health-profile/refresh`(触发 `generate_health_profile.delay`)。

### 7.4 `doctor.py`
注意：文件级**未强制** `require_role(DOCTOR)`，仅函数内 `if not ctx.user: 401`（隐含约定/TODO，16.3）。
`GET /stats`、`GET /cases`、`GET /cases/{id}`(含 AgentSession 时间线)、`POST /cases/{id}/plan`(**占位**，TODO)。

### 7.5 `admin.py`（全部需 ADMIN）
LLM 配置增删改查 + `/test`；系统设置（含 `DEFAULT_SETTINGS` 自动建、`/settings/batch` 路由须在 `/{key}` 前）；`/dashboard/stats`；用户管理(`/users`，禁踢自己/最后 admin)；医生审核(`/doctors/{id}/verify` approve 发确认邮件 / reject 删用户+本地目录)；知识库审核队列(`/knowledge/reviews/{doc_id}` approve/reject/request_revision)；文档增删改查+`/toggle`；审计日志(`/audit-logs`)；SearXNG 健康与搜索；凭证附件审核(`/attachments/{id}/verify`)。

### 7.6 `medical_cases.py`
权限矩阵(`_can_access_case/_can_modify_case`)：ADMIN 全权；PATIENT 仅自己；DOCTOR 仅被分配/本人。`list/create/get/PATCH/DELETE` 病例；`PATCH /{id}/diagnosis`(仅 DOCTOR/ADMIN，医生须为 `doctor_id`)；病例文档增删改查。

### 7.7 `notifications.py`（需 ADMIN）
`list`/`unread-count`/`detail`/`create`(直发) / `broadcast`(向全部 active 用户) / `read` / `delete`(双方软删)。

### 7.8 `email.py`（需 ADMIN）
SMTP 配置 CRUD + `/test` + `/set-default`；`/status`；模板 CRUD；日志 `list`；`POST /send`(模板发送)；服务商预设 `GET /providers`。

### 7.9 `health.py`
`GET /` → `{"status":"healthy"}`。

---

## 8. Celery 任务与工具层

### 8.1 `tasks/`
- `agent.py` `run_diagnosis_agent(session_id, patient_input)` — **占位**（TODO，返回 placeholder）。
- `audit.py` `cleanup_old_audit_logs()` — Beat 每日 03:00；按 `audit_log_retention_days`(默认 30) 删旧日志。
- `health.py` `ping()` — 存活 `pong`。
- `monitoring.py`：
  - `send_reminder(event_id)` `@shared_task` — **ETA 精确调度**（在 `event.scheduled_at` 触发），原子认领 `pending→in_flight`，抗重复投递；发邮件经 `email_service`。
  - `scan_pending_events()` — Beat 周期兜底：回收崩溃 worker 遗留 `in_flight`(>10min) 回 `pending`；扫描到期 `retry_count<3` 的 pending 批量发送。
  - `ensure_default_templates` / `_claim_and_send` / `_send_reminder_email` / `_is_patient_busy`(患者有 ACTIVE 诊断会话则跳过)。
- `planning.py` `generate_health_profile(patient_id)` `@shared_task` — 被 `patient.py` 的 `refresh_health_profile.delay()` 触发。

### 8.2 `tools/`
- `base.py`：`Tool`(ABC，`name/description/parameters/execute/to_openai_schema/run`)、`SimpleTool`(函数式封装，`_infer_params_from_func` 推断参数)。
- `registry.py`：`ToolRegistry`(`register/get/list_schemas/execute` 返回 `{success,result|error}`)；全局 `GLOBAL_REGISTRY`；`register_tool` 装饰器。
- `medical.py`（导入即注册到 `GLOBAL_REGISTRY`）：
  - `search_medical_knowledge(query, doc_type?, top_k=10)` — RAG + SearXNG 合并（见 6.4）。
  - `query_patient_history(patient_id, limit=5, include_documents?)` — 查患者 MedicalCase。
  - `check_drug_interactions(drugs, patient_allergies?)` — 规则启发式(4 组相互作用对+过敏匹配，MVP 占位)。
  - `generate_structured_diagnosis(symptoms, patient_history?, test_results?, knowledge_context?)` — 严格 JSON system_prompt 调 LLM 生成 ICD-11 结构化报告。

---

## 9. 服务层（audit / config / email）

- **`audit.py` `AuditService`**：`@staticmethod record(db, action, user_id?, ..., details?, request?, success?, error_message?)` — 写 `AuditLog`，`details` 经 `_mask_sensitive`(递归脱敏 api_key/password/token 等)、记 IP/UA。**患者侧操作故意不记录**（隐私）。
- **`config.py` `DynamicConfigService`**：全静态/类方法，从 `system_settings` 读（DB 不可用回退默认）；`get_str/int/bool/json/float` + 便捷访问器 `guest_session_ttl_hours/guest_max_messages/cors_origins/rag_*/external_search_*`。
- **`email_service.py` `EmailService`**：`_get_default_config`、`load_config`、`encrypt/decrypt_password`(委托 `core.encryption`)、`send_email(db, to, subject, html?, text?, config?, template_id?)`(`aiosmtplib`，按 `smtp_security` STARTTLS/SSL，写 `EmailLog`)、`render_template`(`{{var}}` 替换)、`send_templated_email`、`test_config`、`EMAIL_PROVIDER_PRESETS`(qq/163/gmail/outlook/custom)。全局单例 `email_service`。

---

## 10. 前端概览

> React 19 + TS + Vite + MUI。`frontend/src` 为源码（其余多为 `node_modules`）。

- **组件树**：`ChatPage`(主) → `Sidebar` / `ChatMessage`(`UploadReportCard`→`LabReportCard`、`DiagnosisCard`、`AgentWorkflow`、Markdown) / `PendingCardsPanel` / `FullScreenReport` / `UploadStatusBanner`(idle|parsing|ready|partial_failure|persistent_failure) / `ChatInput`。管理员端：`AdminLayout` + Dashboard/Users/LLMProviders/KnowledgeBase/DoctorVerification 等页面。
- **核心状态 `chatMode`**：`idle → consulting → diagnosed`，`diagnosed → startNewSession → idle`。
- **上传流程** `handleFileUpload`：`uploadDocument` → 插入「解析中」→ `setInterval` 每 1.5s 轮询 `getParseResult` → completed 则 `store_lab_reports` POST。
- **SSE 事件**：`streamDiagnoseContinue`(thinking/tool_call/tool_result/text/structured/question/complete/error；`structured`→diagnosed) 与 `streamChat`(text/complete/error)。**两者约 40 行 SSE 解析重复**（TODO 提取 `parseSSEStream`，见 14）。
- **样式**：`uploadTokens` 设计 token；`patientTheme` 主色 `#E8956A`。
- 医生端页面、错误消息透传、同类错误去重、SSE 解析复用 均为 **TODO**。

---

## 11. 关键数据流与调用链

1. **诊断流**：`POST /route/stream` → `MasterAgent.classify_intent`(diagnosis) → `DiagnosisAgent.interview`(`decide_next` 出问诊卡) → 用户答 → `interview_answer` → 信息足够 → `run_full_diagnosis_workflow`(先 SearXNG 搜) → `analyze`(Tool Use + 结构化 `DiagnosisReport`) → SSE `structured/text/complete`；注册用户自动建 `MedicalCase`。
2. **问诊去重**：Track1(病史)+Track2(搜索) 并行 → 三层去重 → `deduped` 空且 lab 完整 → `synthesize`（唯一出口）。
3. **诊断后对话（方案 C）**：`POST /sessions/{id}/chat` → 建 `CONVERSATION` 子会话(`parent_session_id`) → `_build_chat_context`(拼 parent 诊断+病史+lab+siblings+历史病例) → `llm.chat` 分块 SSE。父(DIAGNOSIS) 数据永不变（相位降级守卫 + `_update_session` 不碰 context）。
4. **Lab 桥**：图片上传 → `store_lab_reports`（UUID→`_frontend_sid` 归一化，单写入者）→ 注入 `session.context.lab_reports` → `interview` 加载。
5. **随访提醒**：`PlanningAgent.generate_care_plan` 建 `CarePlan`+`MonitoringEvent` → `send_reminder.apply_async(eta=scheduled_at)` → `monitoring.send_reminder` 原子认领发邮件；Beat `scan_pending_events` 兜底。
6. **审核（未接通）**：`models/rag.py` 已定义 `ReviewStatus`/`DocumentReview`，但 `RAGService.create_document` 默认 `APPROVED`、给定 API 无状态流转端点（16.2）。
7. **审计**：`AuditService.record` 在 admin/doctor 操作后写 `AuditLog`（去敏、记 IP/UA）。

---

## 12. 配置与环境变量

敏感值全部来自 `.env`（被 `config.Settings` 加载）。关键项：
- `SECRET_KEY`(必填)、`DATABASE_URL`(asyncpg)、`REDIS_URL`、`CELERY_BROKER_URL/RESULT_BACKEND`、`DEFAULT_ADMIN_EMAIL/PASSWORD`、`API_KEY_MASTER_KEY`(Fernet 主密钥)、`CORS_ORIGINS`、`RAG_CHUNK_SIZE/OVERLAP`、`ENVIRONMENT`(dev 才开 Swagger)。
- **业务动态配置**存 `system_settings` 表（经 `DynamicConfigService`）：`guest_session_ttl_hours`、`guest_max_messages`、`external_search.*`、`rag.*` 等。
- **LLM/Reranker/Embedding Provider** 存 `llm_provider_configs` 表（加密 key），按 `provider+platform+model_type` 解析，支持任意 OpenAI 兼容端点。

---

## 13. 部署

- **Docker Compose 栈**（`docker-compose.yml`，本机已验证可起）：`postgres`(5432)、`redis`(6389 宿主/6379 容器)、`backend`(127.0.0.1:8000)、`celery-worker`、`celery-beat`、`frontend`(127.0.0.1:3001→80)。另 `searxng`、`valkey`(部署文档提及)。
- **构建**：`Dockerfile` 多阶段（builder 装依赖 → production 运行时）；`python:3.12-slim-bookworm`（已修正 trixie 源问题）；前端 `frontend/Dockerfile`(Node22+Nginx)。
- **本地启动**：`docker compose up -d`；API `http://localhost:8000/docs`、UI `http://localhost:3001`。
- **VPS 部署**（`docs/deployment.mdx`）：git push → VPS `git reset --hard origin/main` → **顺序构建**（backend 先、frontend 后，禁并行）→ `docker compose up -d --force-recreate` → 健康检查。
- **迁移**：`docker exec medicareai-backend alembic upgrade head`。
- 端口冲突排查见本次会话历史（redis 6379→6389、frontend 3000→3001）。

---

## 14. 已实现 vs 未实现（TODO）

据 `docs/todos.mdx` + 源码事实：
- **已实现**：三轨问诊、`DiagnosisAgent`、诊断后对话(方案 B+C)、化验单 Bridge、上传 UX、`MedicalCase` 方案 C、RAG 基础管线、Agent 工具注册、管理后台(L可调)、Celery 提醒。
- **P0**：MasterAgent prompt 增强、patientHistory 轻量化、提取 `parseSSEStream`、前端错误透传、同类错误去重。
- **P1**：医生端核心功能(空壳)、`DoctorAgent`、`KnowledgeAgent`、访客/注册 UX。
- **P2**：MCP、GraphRAG、WebSocket、Android WebView。
- **P3**：**LLM provider 降级策略**(当前单 provider，主不可用不切换)、store_lab_reports 极端并发安全、ChatInput 完成态 placeholder。
- **占位/TODO**：`tasks/agent.run_diagnosis_agent`(占位)、`doctor.py POST /cases/{id}/plan`(占位)、审核工作流(模型就绪未接通)、`documents.py` 解析任务内存存储。

---

## 15. 快速上手

```bash
# 本地全栈（Docker）
cp .env.example .env        # 填 API key / SECRET_KEY / API_KEY_MASTER_KEY
docker compose up -d        # 6 服务；UI http://localhost:3001

# 本机后端开发
cd backend
pip install -e ".[dev]"     # 已修复 readme 路径（backend/README.md 副本）
pytest -q

# 配置 LLM Provider（首次需管理员在后台 /api/v1/admin/llm-providers 配置 OpenAI 兼容端点）
```

---

## 16. 源码与既有 docs 不一致清单（重要·以本文档为准）

> 编写文档时逐条核对，以下与 `docs/database.mdx`、`docs/architecture.mdx`、文件命名预期不符，**请以后端源码为准**：

1. **`users.email` 非单列唯一**：源码是 `UniqueConstraint("email","role")` —— 允许同邮箱不同角色分别注册。
2. **`UserStatus` 无 `banned`**：源码 `active/inactive/pending`（pending 已废弃）。
3. **`guest_sessions.max_messages` 默认 999**（非文档写的 3）。
4. **`medical_cases` 关闭时间列是 `resolved_at`**（非 `closed_at`）；`update_diagnosis` 在 CLOSED 时写 `resolved_at`。
5. **`hashed_password` 可空**（支持第三方/无密码，非文档 NOT NULL）。
6. **`documents.py` 不是文档 CRUD/审核接口**，而是「上传→多模态解析→轮询」；审核工作流(`ReviewStatus`/`DocumentReview`)数据模型已就绪但**未接通业务**（`create_document` 默认 `APPROVED`）。
7. **embedding 未用 pgvector**：`DocumentChunk.embedding_json`(JSONB) 存 `str(list)`，`Vector(1024)` 仅注释；相似度是纯 Python `cosine_similarity`，非向量索引检索。大规模检索需迁移 pgvector。
8. **`search_vector`(tsvector/GIN) 在 `create_document` 生成但 `search` 未使用**，检索实际用 ILIKE 粗排。
9. **`reranker` 非硬编码 Cohere**：走 OpenAI 兼容自定义 `/rerank` 端点，模型由 DB `model_type='reranking'` 配置。
10. **`CarePlan`/`MonitoringEvent` 定义在 `models/agent.py` 但未从 `models/__init__.py` 导出**。
11. **`users.py` 为空**，用户管理在 `admin.py`；`doctor.py` 未文件级强制 `require_role(DOCTOR)`（隐含约定）。
12. **`agent/` 子包无实现**，`services/agents.py` 才是 Agent 核心；`services/orchestrator.py` 仅三轨问诊决策（不负责意图路由）。
13. **`llm.chat_stream` 已基本弃用**（kimi-k2.6 流式 API 不工作），chat 端点与诊断后对话用 `llm.chat` 非流式。
14. **解析任务 `_parse_tasks` 存内存**（重启即丢，生产应换 Redis/DB）。
15. **文件存储不一致**：`/register/doctor` 存本地 `backend/uploads/credentials/`，`/users/me/attachments` 走 OSS。

---

---

# 第二部分 · 代码级深读（真实代码 + 分支走查）

> 本部分在第一部分「方法级总览」之上，**贴出核心方法的真实代码片段**（标注 `文件:行号`，均按源码原样），并逐分支走查控制流、还原送进 LLM 的 prompt 原文。所有内容来自第一部分的子代理精读，可安全用于对照源码。
> 阅读建议：先读第一部分建立全局，再读本部分理解「为什么这么写」与「每步到底做什么」。

---

## 17. 认证、权限与配置链（代码级）

### 17.1 `api/deps.py` — `_resolve_token`（核心解析，deps.py:32-92）

```python
# deps.py:44-84  按 type 分支
if not token:
    return None, "unknown", False, None
try:
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    redis_client = get_redis()
    is_blacklisted = await redis_client.get(f"token_blacklist:{token_hash}")
    if is_blacklisted:
        return None, "unknown", False, None          # 命中黑名单 → 视为未登录
except Exception:
    pass                                           # Redis 宕机 → fail open，放行

try:
    payload = decode_token(token)
    token_type = payload.get("type")
    if token_type == "guest":
        return None, payload.get("platform") or "unknown", True, payload.get("sub")
    if token_type == "access":
        user_id = payload.get("sub")
        if user_id is None:
            return None, "unknown", False, None
        user = (await db.execute(select(User).where(User.id == uuid.UUID(user_id)))).scalar_one_or_none()
        if user is None:
            return None, platform, False, None
        if user.status != "active":
            raise HTTPException(status_code=403, detail="User account is inactive or pending")
        return user, platform, False, None
    return None, "unknown", False, None
except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
    return None, "unknown", False, None               # 过期/非法 → 匿名（不抛错）
```

**分支语义**：①空 token → 匿名；②黑名单（token 经 sha256 后查 Redis `token_blacklist:{hash}`，明文不落库）→ 匿名；③Redis 异常 → 静默放行（fail open）；④`type=="guest"` → 不查库直接返回访客；⑤`type=="access"` → 查 `User`，**非 active 抛 403**（pending/inactive 账号被阻断），否则返回 user；⑥过期/非法 → 匿名（SSE 无法刷新 token，故不抛 401）。

### 17.2 Token 来源优先级（deps.py:110-123）

```python
cookie_token = request.cookies.get("auth_token")
query_token = request.query_params.get("token") or request.query_params.get("guest_token")
effective_token = token or x_guest_token or query_token or cookie_token
user, platform, is_guest, guest_id = await _resolve_token(effective_token, db)
if user is None and not is_guest:
    raise HTTPException(status_code=401, detail="Not authenticated",
                        headers={"WWW-Authenticate": "Bearer"})
```

**优先级真实顺序**：`Bearer` > `X-Guest-Token` Header > URL query(`token`/`guest_token`) > Cookie(`auth_token`)。query 优先于 cookie 是 SSE 刻意设计：`EventSource` 无法设自定义 Header/cookie，前端经 query 传 token，旧 `auth_token` Cookie 不得覆盖它。

### 17.3 权限工厂（deps.py:188-201）

```python
def require_role(*roles: UserRole) -> Callable:
    async def _check_role(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role not in roles:
            raise HTTPException(status_code=403,
                detail=f"Requires role: {', '.join(r.value for r in roles)}")
        return current_user
    return _check_role
```

`require_platform(*allowed)`（deps.py:164-178）同构，`require_role` 依赖 `get_current_user`（已排除 guest），故 guest 在 `get_current_user` 即被 401 拦截，不会漏进角色判断。`get_current_user_or_guest_lenient`（deps.py:209-230）为 SSE 宽松版——解析失败返回空 `UserContext`（不抛），专为无法刷新 token 的场景。

### 17.4 `core/security.py` — JWT 与 bcrypt（security.py:42-119）

```python
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7   # 7 天
GUEST_TOKEN_EXPIRE_MINUTES  = 60 * 24       # 1 天

def get_password_hash(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()   # 12 轮工作因子

def create_access_token(subject, platform=None, expires_delta=None) -> str:
    expire = datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode = {"exp": expire, "sub": str(subject), "type": "access",
                 "platform": platform or "unknown"}
    return jwt.encode(to_encode, settings.secret_key.get_secret_value(), algorithm=ALGORITHM)

def decode_token(token: str) -> dict:
    return jwt.decode(token, settings.secret_key.get_secret_value(), algorithms=[ALGORITHM])  # 仅 HS256，防 alg-confusion
```

`create_guest_token`（security.py:74-100）结构与 access 一致，差异在 `type:"guest"`、默认 1 天、带 `fingerprint`。`decode_token` 抛 `ExpiredSignatureError`/`InvalidTokenError` 由 `deps._resolve_token` 捕获转匿名。

### 17.5 `core/encryption.py` — Fernet 静态加密（encryption.py:17-63）

```python
def derive_fernet_key(master_key: str) -> bytes:
    return base64.urlsafe_b64encode(hashlib.sha256(master_key.encode()).digest())   # 任意串→32字节

def _get_cipher() -> Fernet:
    key_str = os.getenv("API_KEY_MASTER_KEY")
    if not key_str:
        raise RuntimeError("API_KEY_MASTER_KEY is required for secure operation.")
    return Fernet(derive_fernet_key(key_str))

def decrypt_value(ciphertext: str | None) -> str | None:
    if not ciphertext:
        return None
    try:
        return _get_cipher().decrypt(ciphertext.encode()).decode()
    except Exception:
        return None          # 吞异常 → 解密失败返回 None（密钥轮换/损坏时不崩溃）
```

`decrypt_value` 返回 `None` 是 `llm.py` 中 `decrypt_value(...) or ""` 兜底为空串的根因——密钥损坏时不会崩，仅导致后续 api_key 为空被拦截。`mask_api_key` 脱敏为 `sk-****wxyz`（前4后4）。

### 17.6 `core/config.py` — `Settings` 字段清单（config.py:16-96）

| 字段 | 类型 | 默认值 | 必填 | env 别名 |
|---|---|---|---|---|
| `secret_key` | `SecretStr` | 无 | **是** | — |
| `database_url` | `PostgresDsn` | `postgresql+asyncpg://postgres:postgres@localhost:5432/medicareai` | 否 | — |
| `redis_url` / `celery_broker_url` / `celery_result_backend` | `str` | `redis://localhost:6379/0` / `/1` / `/2` | 否 | — |
| `cors_origins_raw` | `str` | `"*"` | 否 | `CORS_ORIGINS` |
| `cors_fallback_origin` | `str` | `https://openmedicareagent.online` | 否 | — |
| `default_admin_email` / `default_admin_password` | `str` / `SecretStr` | `admin@medicareai.dev` / `None` | 无 admin 时必填 | — |
| `api_key_master_key` | `SecretStr` | `None` | 加密时必填 | — |
| `rag_chunk_size` / `rag_chunk_overlap` | `int` | `1000` / `200` | 否 | — |
| `rag_llm_temperature` / `rag_llm_max_tokens` | `float`/`int` | `0.3` / `2048` | 否 | — |
| `environment` | `Literal` | `"development"` | 否 | — |

`cors_origins` property（config.py:62-67）**生产降级**：`is_production and "*" in origins` → 丢弃通配符，仅返回 `[cors_fallback_origin]`（防生产误配 `*` 任意跨域）。`get_settings()` `@lru_cache` 全局单例。

---

## 18. LLM 服务（代码级）

### 18.1 `_get_provider_config` — 4 级优先级解析（llm.py:47-136）

按 `provider + platform + model_type` 从加密表 `llm_provider_configs` 解析配置，逐级降级：

```python
# L1 精确：platform + model_type 都匹配
if platform:
    config = (await db.execute(select(LLMProviderConfig).where(
        LLMProviderConfig.provider == provider,
        LLMProviderConfig.platform == platform.strip().lower(),
        LLMProviderConfig.model_type == model_type,
        LLMProviderConfig.is_active == True).limit(1))).scalars().first()
    if config:
        return {"base_url": config.base_url, "api_key": decrypt_value(config.api_key_encrypted) or "", ...}
# L2 全局+类型：platform IS NULL + model_type
# L3 兼容：platform 匹配（忽略 model_type）
# L4 全局兜底：platform IS NULL（忽略 model_type）
# 全 miss → raise ValueError("provider/platform/model_type ... not configured")
```

| 层级 | 条件 | 过滤 | 未命中 |
|---|---|---|---|
| L1 | `platform` 非空 | provider+platform(小写)+model_type+active | →L2 |
| L2 | 永远 | provider+platform IS NULL+model_type+active | →L3 |
| L3 | `platform` 非空 | provider+platform(小写)+active（无 type） | →L4 |
| L4 | 永远 | provider+platform IS NULL+active（无 type） | 抛 ValueError |

`db is None` → 立即 ValueError（提示经后台配置）。解密失败（`decrypt_value` 返回 None）→ `or ""` 兜底空串，最终在 `_get_client` 被 `if not config.get("api_key"): raise ValueError` 拦截。

### 18.2 `LLMService` 方法（llm.py:240-584）

**`chat`（非流式，默认 disable_thinking）**

```python
client = await self._get_client()
msgs = list(messages)
if system_prompt: msgs.insert(0, {"role": "system", "content": system_prompt})
kwargs = dict(model=_model, messages=msgs, max_tokens=max_tokens, stream=False)
merged_extra = {}
if disable_thinking: merged_extra["thinking"] = {"type": "disabled"}   # 兼容 Kimi/DeepSeek 省 token
if extra_body: merged_extra.update(extra_body)
if merged_extra: kwargs["extra_body"] = merged_extra
response = await client.chat.completions.create(**kwargs)
# tool_calls 原样返回（arguments 为原始 JSON 字符串）
```

**`chat_with_tools`（Function Calling）** — 与 `chat` 差异：`tools/tool_choice` 透传，`tool_choice` 默认 `"auto"`；**`arguments` 经 `json.loads` 解析为 dict**（agent 直接可用）；`thinking` 硬编码 `{"type":"disabled"}`（避免工具调用时推理耗 token）。

**`generate_structured`（JSON Schema 强结构）**

```python
schema = output_schema.model_json_schema()
schema_instruction = (f"\n\nYou must respond with a single JSON object matching this schema:\n"
                      f"{json.dumps(schema, indent=2, ensure_ascii=False)}\n"
                      f"Output ONLY the JSON object, no markdown formatting.")
msgs[0]["content"] += schema_instruction if msgs[0]["role"] == "system" else msgs.insert(0, ...)
try:
    response = await client.chat.completions.create(
        model=..., messages=msgs, max_tokens=max_tokens,
        response_format={"type": "json_schema", "json_schema": {"name": schema_name, "schema": schema, "strict": True}})
except Exception:
    response = await client.chat.completions.create(model=..., messages=msgs, max_tokens=max_tokens, stream=False)  # 降级普通 chat
content = response.choices[0].message.content or "{}"
# 剥离 ```json / ``` 围栏 → json.loads → output_schema.model_validate(data)
```

**`chat_vision`（多模态）**：content 为 `[{type:image_url, image_url:{url:data:image/{mime};base64,...}}, {type:text,...}]` 数组；MIME 由 magic bytes 检测（失败默认 jpeg）。

**`health_check`**：`client.models.list()`，任何异常捕获返回 `{"status":"error", "detail":...}`（健康探针不抛）。

> **OpenAI 兼容实现**：`_get_client` 用 `AsyncOpenAI(base_url=..., api_key=..., timeout=120, max_retries=2)`。因走 OpenAI 兼容协议，kimi / GLM / DeepSeek / Qwen 皆只需在后台配置 base_url 即可接入。

---

## 19. Agent 编排与诊断引擎（代码级 · 系统大脑）

### 19.1 `MasterAgent` 意图识别（agents.py:154-271）

**SYSTEM_PROMPT 核心规则**（原文）：
```
- Symptom reports ALWAYS use "diagnosis" intent, regardless of severity.
- Follow-up questions about an existing diagnosis are "diagnosis" — pipeline handles them.
- Progress updates on a known condition are "monitoring".
- Questions about external medical knowledge are "research" — not "diagnosis".
- Only use "planning" when explicitly asking about treatment schedules/appointments.
```

`classify_intent(user_input, session_context)` 控制流：① `session_context` 非空时动态拼接上下文（`has_completed_diagnosis` → 提示"可能是(a)随访/(b)进展/(c)新症状"；`diagnosis_summary` 与 `interview_collected` 经 `insert(0)` 置于最前，截断 300/100 字符）；② `llm.chat(max_tokens=512)`；③解析分支——`json.loads` 成功直接返回；`JSONDecodeError` 时正则 `"intent"\s*:\s*"(\w+)"` 命中返回低置信；都不行→**硬编码兜底 `intent="diagnosis"`**（绝不丢请求）。

### 19.2 `DiagnosisAgent.analyze` — 5 轮 Tool Use + 三重兜底（agents.py:419-572）

```python
for _round in range(5):
    if _round == 4:
        messages.append({"role": "user", "content": "所有工具已调用完毕。直接输出结构化 JSON 诊断报告。不要再调用任何工具。"})
        resp = await llm.chat_with_tools(messages=messages, tools=tool_schemas,
            system_prompt=self.SYSTEM_PROMPT, max_tokens=4096, tool_choice="none")   # 强制收口
    else:
        resp = await llm.chat_with_tools(..., tool_choice="auto")
    if resp.tool_calls:
        assistant_msg = {"role": "assistant", "content": resp.content or "",
                         "tool_calls": [{"id": tc["id"], "type": "function",
                                         "function": {"name": tc["name"], "arguments": json.dumps(tc["arguments"])}} for tc in resp.tool_calls]}
        messages.append(assistant_msg)
        for tc in resp.tool_calls:
            result = await GLOBAL_REGISTRY.execute(tc["name"], tc["arguments"])   # 执行工具
            messages.append({"role": "tool", "tool_call_id": tc["id"], "content": json.dumps(result, ensure_ascii=False)})
    else:
        break   # 模型不再调工具 → 提前收敛
```

**退出条件**：①正常收敛——`resp.tool_calls` 为空 `break`（最多提前到第 1~3 轮）；②强制收敛——第 5 轮以 `tool_choice="none"` 逼模型只出文本。每轮若调工具，把 assistant(tool_calls) 与 tool 结果消息依次入 `messages`，并累计 `all_tool_calls`。

**三重兜底结构化解析**（绝不返回裸文本）：

```python
content = resp.content or ""
structured = None
if content.strip().startswith("{"):
    try: structured = DiagnosisReport.model_validate(json.loads(content))           # Attempt 1 直接解析
    except Exception: pass
if structured is None:
    try: structured = await llm.generate_structured(messages + [强制结构化指令],    # Attempt 2 schema 约束
            output_schema=DiagnosisReport, max_tokens=4096)
    except Exception: pass
if structured is None:
    try:                                                                             # Attempt 2.5 普通 chat + 抽 JSON
        resp = await llm.chat(messages + [指令], system_prompt="你是医学诊断AI。只输出JSON格式的诊断报告。")
        structured = DiagnosisReport.model_validate(_extract_json(resp.content))
    except Exception: pass
if structured is None:
    structured = DiagnosisReport(primary_diagnosis="AI 诊断生成失败", confidence="low", ...)  # Attempt 3 合法兜底对象
```

`_update_session`（agents.py:574-596）**只写 `tool_calls/structured_output/status`，刻意不动 `context`**（注释明确：context 由 `_update_interview_state` 独占，防 read-modify-write 竞态把 `phase=completed` 抹掉）。

### 19.3 `DynamicInterviewEngine` + `InterviewState`（models/interview.py）

`InterviewState` 核心快照字段：`chief_complaint, collected_info(dict), raw_answers(dict), asked_questions(list), phase("interviewing"|"diagnosing"|"followup"|"completed"), asked_question_fingerprints, question_phase_keys, question_texts, regeneration_count, lab_reports, _frontend_sid(不持久化)`。

- **相位 vs 维度是两个概念**：`phase` 是流程状态机；`InterviewPhase` 是 22 个临床信息维度（HPI_*/PMH_*/PS_*/FH_*/MED_*），靠 `collected_info` 的 key 是否被填充判定"已覆盖"，非线性推进。
- **`get_summary`**（interview.py:317-373）是诊断输入来源：拼主诉 + 遍历 `collected_info`（跳过 `__` 前缀与否定/占位值"无/没有/不清楚/跳过/asked_by_track1"）+ raw_answers 原话 + 鉴别诊断(✓/×/?) + `⚠️ 危险信号` + `lab_reports` 中 `overall_confidence>=0.7` 的异常指标（`[异常]` 标记），`\n` 拼接成 `analyze` 的 `symptoms`。
- **去重辅助**：`_fingerprint`（去标点空白→sha256 前12位，内容级去重）、`_extract_phase_key`（`hpi_onset_001→hpi_onset`，按临床维度去重）。
- `process_answer`（interview.py:600-718）：否定快捷分支（`"跳过"/"不清楚"/"不记得"` → 直接存不调 LLM）；否则调 LLM 抽取结构化信息、更新 `DifferentialHypothesis` 的 confirmed/refuting_evidence、追加新鉴别诊断；异常兜底把原话存入 `collected_info`（不丢数据）。

### 19.4 `InterviewOrchestrator.decide_next` — 唯一合成出口（orchestrator.py:246-411 · 系统最核心）

**完整分支走查**：

```python
if state.phase == "completed":
    if (state.regeneration_count or 0) >= 1:
        return [], state, [], "completed", ""        # 再生耗尽 → 彻底结束
    state.phase = "interviewing"; state.regeneration_count += 1; regeneration = True   # 允许诊断后再补一轮
else:
    regeneration = False

# Phase 1：Track1(病史) + Search(SearXNG) 并行
track1_task = self.track1.generate(state, patient_history)
search_task = self._run_search(chief, state) if self.search else asyncio.sleep(0, result="")
track1_questions, diffs, red_flags, reasoning = await track1_task
search_results = await search_task
# 把 Track1 覆盖维度写 collected_info[q.phase]="asked_by_track1"（让 Track2 看见），累加 red_flags，set_differential_diagnoses

# Phase 2：Track2(搜索增强) 生成靶向问题
track2_questions = await self.track2.generate(state, search_results, diffs)

# 三级去重
all_questions = track1_questions + track2_questions
deduped = self._deduplicate(all_questions, state)               # ① phase_key 去重，[:2]
for q in deduped:
    if q.type == "text":                                        # ② 选项补全
        opts = await self._complete_options(q)
        if opts and len(opts) >= 2: q.options = opts; q.type = "multi_choice"
        else: filtered_out.add(q.question_id); continue
    if q.type in ("multi_choice","choice") and (not q.options or len(q.options) < 2): filtered_out.add(...)
    if q.type == "multi_choice" and "以上都没有" not in q.options: q.options += ["以上都没有"]
deduped = await self._semantic_dedup(deduped, state)            # ③ LLM 语义去重

if regeneration:
    action = "synthesize"; state.is_sufficient = True; state.phase = "completed"; return [], state, [], action, reasoning
if not deduped:
    pending_unanswered = set(state.question_texts) - set(state.asked_questions)
    if pending_unanswered: action = "ask"; return [], state, [], action, reasoning       # 先问完未答卡片
    try:                                                                        # lab 桥：新报告到达 → 多问一轮
        bridge_reports = _session_lab_bridge.get(state._frontend_sid, [])
        if len(bridge_reports) > len(state.lab_reports):
            state.lab_reports = bridge_reports; action = "ask"; return all_questions[:2], state, [], action, reasoning
    except Exception: pass
    state.is_sufficient = True; state.phase = "completed"; state.regeneration_count = 1; action = "synthesize"   # 自然合成出口
    return [], state, [], action, reasoning
action = "ask"                                                                  # 正常出口
for q in deduped:
    if _fingerprint(q.question) not in state.asked_question_fingerprints:
        state.asked_question_fingerprints.append(_fingerprint(q.question))
    state.question_phase_keys[q.question_id] = _extract_phase_key(q.question_id)
    ...
return deduped, state, [], action, reasoning
```

**三条出口**：①再生强制 `synthesize`；②`deduped` 空且无 pending 且无新 lab → `synthesize`（唯一自然合成出口）；③`deduped` 非空 → `action="ask"`，把 fingerprint/phase_key/text 写入 state，更新 `asked_questions/current_question_id/pending_question_ids`。

**`Track1Agent.generate`**（orchestrator.py:94-133）：`_build_prompt` 拼主诉+既往史(截500)+已收集(最近12条)+未问维度（`PHASE_ORDER` 中不在 `collected_info` 的）+ `TRACK1_DECISION_SCHEMA` → `llm.chat` → `_extract_json` → `_to_templates`；异常返回 `([],[],[],"")`（空而非崩）。**`Track2Agent.generate`**（orchestrator.py:179-200）：`search_results` 长度 < 20 → 直接返回 `[]`（不浪费 LLM）。**`_semantic_dedup`**（orchestrator.py:466-515）：prompt 让 LLM 判候选与已问问题是否语义重复，返回 `keep/drop` 索引；异常→保守返回全部候选（去重失败就都保留）。

### 19.5 `_update_interview_state` — 相位降级守卫（agents.py:866-907）

```python
if existing_phase == "completed" and state.phase != "completed":
    return  # 拒绝把 completed 降级回 interviewing（防迟到答案打回已完成会话）
session.context = {**(session.context or {}), "interview": state.to_dict()}  # 整 dict 重赋值触发 JSONB 变更
await db.commit()
await asyncio.sleep(0.1)   # 等提交对其他读可见
```

---

## 20. 诊断 SSE 流水线（代码级）

### 20.1 `GET /route/stream` 事件序列（agents.py:770-1196）

访客自动建 GuestSession（agents.py:795-824）：**完全无 Bearer 且未登录** → 建 GuestSession + `create_guest_token`；**有 Bearer 但解析失败** → 抛 401（不静默降级）。

真实 `yield` 事件序列：

```
guest_token?            # 仅访客自动建时下发新 token
→ thinking(master)      # "🧠 MasterAgent 正在分析您的需求..."
→ intent               # {intent, confidence, reasoning, clarifying_question}
→ thinking(master_done)# 意图识别完成
→ agent_switch         # {agent, agent_display, message}  escalation 在此改写为 diagnosis
→ [diagnosis 分支]:
     interview → 有 searches → thinking(step:search)
                → 有 questions → interview_progress + question + complete(waiting_for_answer) 并 return
                → 有 red_flags_detected → red_flags 事件（不 return，继续诊断）
                → Redis diag_lock 抢锁失败 → complete(already_diagnosed) return
                → thinking(step:diagnosis) → run_full_diagnosis_workflow
                → 遍历 tool_calls_used 发 tool_call + sleep(0.2) + tool_result
                → 有结构化报告 → structured + 按块发 text(Markdown)
                → Plan C：注册用户自动建 MedicalCase
                → complete
   [非 diagnosis 分支]: llm.chat_stream 逐块发 text
→ complete              # 外层
异常 → error            # 保留 KeyboardInterrupt/SystemExit 上抛
```

### 20.2 `GET|POST /route/stream/continue` — 续答（agents.py:1209-1371）

答案从 `X-Answer` 头读取（base64 解码防 URL/HTTP2 问题）→ POST body `answer` → query `answer` → 缺省 `"无"`。流程：`diag_lock` 存在→`complete(diagnosis_in_progress)` return；`interview_answer` → `action=="completed"`→`complete(already_diagnosed)` return；有 questions→`question`+`complete(waiting)`；`not is_sufficient`→`complete(waiting)`（**防死锁**分支，日志标 `EMPTY CARDS — DEADLOCK`）；否则抢锁→`tool_call(search_medical_knowledge)`→`run_full_diagnosis_workflow`→`structured`+`text`+`complete`。

`run_full_diagnosis_workflow`（agents.py:782-864）：`InterviewState.get_summary()` → `_generate_search_query` 生成检索词 → `asyncio.wait_for(GLOBAL_REGISTRY.execute("search_medical_knowledge",...), timeout=60.0)`（超时则 `search_result=None`，SearXNG 延迟绝不阻塞诊断）→ `analyze` → 把搜索包装成 tool_call 插入 `tool_calls_used` 头部供前端可见。

---

## 21. RAG 与工具（代码级）

### 21.1 `RAGService` — 入库 / 检索 / 生成（rag.py:35-304）

**`create_document`**：写 `Document` → `flush()` 取 id → 全文 `to_tsvector`（`zh` 用 `simple` 否则 `english`）→ 循环分块（每块独立 `UPDATE search_vector`）→ `EmbeddingService.embed` 逐块写 `embedding_json=str(emb)` → `vectorized_at`。**降级**：`except ValueError: pass`（embedding 未配则静默跳过，文档仍可用）。

**`search` 三阶段**（每阶段独立降级）：

```python
# 阶段1：ILIKE 粗排（取 query 连续子串中最长者作 primary_term）
stmt = select(...).join(Document).where(DocumentChunk.content.ilike(f"%{primary_term}%"), Document.is_active == True).limit(coarse_k)
# 阶段2：余弦重排
query_emb = (await embed_svc.embed([query]))[0]
scored = [(cosine_similarity(query_emb, json.loads(r.embedding_json) if isinstance(r.embedding_json,str) else r.embedding_json), r) for r in rows]
scored.sort(reverse=True); vector_top = scored[:top_k*2]
# 阶段3：rerank 精排
reranked = await RerankerService(db).rerank(query, [r.content for _,r in vector_top], top_n=top_k)
```

- 阶段1 空 → 直接 `return []`；阶段2 无 embedding（`ValueError`）→ 以 `0.0` 占位把关键字结果截到 `top_k*2`；阶段3 reranker 未配（`ValueError`）→ 退回余弦排序前 `top_k`。
- **`vectorized` 真相**：`embedding_json` 存 `str(list)`，`Vector(1024)` 仅注释；相似度是纯 Python `EmbeddingService.cosine_similarity` 循环，非 pgvector 索引检索（大规模需迁移）。

**`generate_answer`**（rag.py:235-274）：`context` 用 `"[来源: {title}]\n{content}"` + `---` 连接所有 chunks；`system` 优先用传入 `system_prompt`，否则内置 `"你是一位专业的医疗AI助手…"`；调 `LLMService.chat`；**降级**：未配 key → 返回 `[LLM 未配置] 以下是检索到的相关参考文献…` 原文拼接。

### 21.2 `EmbeddingService` / `RerankerService`（embedding.py:43-97 / reranker.py:66-95）

```python
# embedding.py
async def embed(self, texts): 
    return [item.embedding for item in (await self._get_client().embeddings.create(model=self._model, input=texts)).data]
@staticmethod
def cosine_similarity(a, b):
    dot = sum(x*y for x,y in zip(a,b)); na = math.sqrt(sum(x*x for x in a)); nb = math.sqrt(sum(x*x for x in b))
    return 0.0 if na==0 or nb==0 else dot/(na*nb)

# reranker.py — 自定义 /rerank 端点（非硬编码 Cohere），模型由 DB model_type='reranking' 配置
async def rerank(self, query, documents, top_n=5):
    try: client = await self._get_client()
    except ValueError: return [(i, 1.0) for i in range(len(documents))]   # 未配置 → 恒等透传
    resp = await client.post("/rerank", body={"model": self._model, "query": query, "documents": documents, "top_n": top_n}, cast_to=dict)
    return [(r["index"], r["relevance_score"]) for r in resp.get("results", [])]
```

### 21.3 `ExternalSearchAgent` — SearXNG + 可信打分（external_search.py:250-304）

```python
TRUSTED_DOMAINS = frozenset({".gov.cn", ".edu.cn", "who.int", "ncbi.nlm.nih.gov", "pubmed.ncbi.nlm.nih.gov",
    "cnki.net", "dxy.cn", "nmpa.gov.cn", "nejm.org", "thelancet.com", "cochrane.org", ...})   # 约 30 个权威域
def _filter_trusted(self, results):
    score = 0
    if any(hostname.endswith(d) or hostname == d.lstrip(".") for d in self.TRUSTED_DOMAINS): score += 50   # 命中白名单 → is_trusted
    if engine in self.HIGH_TRUST_ENGINES: score += 30       # pubmed/wikipedia/google scholar
    if len(content) > 200: score += 10
    elif len(content) > 50: score += 5
    if len(content) > 0: score += 1
    return SearchResult(..., trust_score=score, is_trusted=score >= 50)
```

`_searxng_search`（external_search.py:218-244）：成功取 `resp.json()["results"]`；任意异常 → `return []`（外部检索为补充，不阻断主流程）。`healthcheck` 区分 200/非200/Timeout/其他异常。

### 21.4 `LabReportParser` — 化验单多模态（multimodal_parser.py:228-292）

```python
CONFIDENCE_THRESHOLD = 0.7
async def parse_image(self, image_data, file_id=""):
    response = await self.llm.chat_vision(text_prompt=..., image_bytes=image_data, system_prompt=self.SYSTEM_PROMPT, max_tokens=4096)
    indicators_raw = self._extract_json_array(response.content)
    if not indicators_raw:
        result.error = "未能从图片中解析出检测指标"; result.requires_manual_review = True; return result
    indicators = [self._normalize_indicator(item) for item in indicators_raw]
    result.overall_confidence = round(sum(i.confidence for i in indicators)/len(indicators), 2) if indicators else 0.0
    result.requires_manual_review = (result.overall_confidence < self.CONFIDENCE_THRESHOLD) or not indicators
    return result
```

`_normalize_indicator`：LLM 未判异常且能转数值 → 调 `_check_abnormal`（查 `DEFAULT_REFERENCE` 精确→子串模糊，低于下限→`("low")`，高于上限→`("high")`）；`_lookup_loinc`（查 `LOINC_MAP` 精确→子串模糊）；`confidence` 夹到 `[0,1]`。低置信/解析失败 → `requires_manual_review=True` 人工复核门控。

### 21.5 四个医疗工具（tools/medical.py · 导入即注册 `GLOBAL_REGISTRY`）

- **`SearchMedicalKnowledgeTool.execute`**（medical.py:82-175）：① `rag.query`（失败降级空）；② 同时跑 `search_guidelines`（已过滤白名单）+ `_searxng_search+_filter_trusted`（通用），**按 URL 去重**取前 `top_k`；③拼接策略——有外部且内部非空非"未找到"→ 内部+`---`+外部；内部空/未找到→仅外部；无外部→仅内部。
- **`QueryPatientHistoryTool.execute`**（medical.py:189-242）：按 `patient_id`(转 UUID) 查 `MedicalCase` 倒序取 `limit`；`include_documents=True` 才额外查 `MedicalDocument`。
- **`CheckDrugInteractionsTool.execute`**（medical.py:256-296）：4 组相互作用规则（`{aspirin,warfarin}`/`{metformin,contrast}`/`{ace inhibitor,spironolactone}`/`{nsaid,ace inhibitor}`，交集≥2 触发告警）+ 过敏子串交叉检查（命中记 `CRITICAL`）；`safety_status` 有告警=`caution` 否则=`safe`。
- **`GenerateStructuredDiagnosisTool.execute`**（medical.py:310-388）：ICD-11 system_prompt 强约束（每个诊断必须带 `icd11_code`，如 `J05.0`/`J00`），`llm.chat` → `try json.loads` 失败兜底 `{"primary_diagnosis":"Parse error",...}`。

**`ToolRegistry.execute`**（registry.py:14-61）统一返回 `{success, result|error}`；`Tool.run`（base.py:29-94）先 `parameters.model_validate` 校验（失败 `ToolError`），执行异常包成 `ToolError`。`to_openai_schema` 剔除 Pydantic `title` 噪音以精简 LLM 上下文。

---

## 22. 关键降级与容错总表（代码级）

| 失败点 | 代码位置 | 降级行为 |
|---|---|---|
| Embedding 未配 | rag.py:127-129 | `create_document` 静默跳过向量化，文档仍可用 |
| 检索阶段2 无向量 | rag.py:198-200 | 以 `0.0` 占位，退回关键字结果前 `2*top_k` |
| Reranker 未配 | reranker.py:79-81 | 恒等透传 `(i,1.0)`，`search` 退回余弦排序 |
| LLM 未配 | rag.py:268-274 | `generate_answer` 返回原文拼接 + `[LLM 未配置]` |
| SearXNG 失败 | external_search.py:241-244 | 返回 `[]`，仅用 RAG 内部结果 |
| 化验单低置信/解析失败 | multimodal_parser.py:282-290 | `requires_manual_review=True` 人工复核 |
| 诊断结构化解析失败 | agents.py:485-555 | 三重兜底 → 构造合法 `DiagnosisReport("AI 诊断生成失败")` |
| 意图解析失败 | agents.py:262-271 | 正则兜底 → 硬编码 `intent="diagnosis"` |
| 过期/非法 token | deps.py:58-84 | 匿名（不抛，SSE 不阻断） |
| 解密失败 | encryption.py:45-54 | 返回 `None` → api_key 空串 → `ValueError` 拦截 |

---

*本文档由逐文件源码精读生成，覆盖 backend 全部 92 个 .py 的方法级实现、14 个 ORM 模型字段级结构、前端组件与数据流，以及部署与 TODO 全景。与 `docs/` 的差异已在第 16 章统一标注。第二部分（第 17–22 章）为代码级深读，含真实代码片段与分支走查。*
