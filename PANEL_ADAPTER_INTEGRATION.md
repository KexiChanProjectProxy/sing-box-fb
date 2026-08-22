# sing-box Panel Adapter 对接与测试文档

本文档详细说明如何将 sing-box 节点对接至云端管理面板，以及如何使用 Mock 服务端进行全面的集成测试。

## 目录

1. [API 概述](#1-api-概述)
2. [测试架构](#2-测试架构)
3. [Mock 服务端包 (internal/paneladapter/mock)](#3-mock-服务端包-internalpaneladaptermock)
4. [独立 Mock 服务端 (cmd/mock-panel-server)](#4-独立-mock-服务端-cmdmock-panel-server)
5. [测试场景与示例](#5-测试场景与示例)
6. [对接面板生产环境](#6-对接面板生产环境)
7. [常见问题](#7-常见问题)

---

## 1. API 概述

### 1.1 端点列表

面板适配器使用严格的 REST v1 API，共 4 个端点：

| 方法 | 路径 | 用途 |
|------|------|------|
| `GET` | `/api/v1/nodes/{node_id}/configuration` | 获取节点配置和适配器元数据 |
| `GET` | `/api/v1/nodes/{node_id}/inbounds/{inbound_id}/users` | 获取某个入站的用户快照 |
| `POST` | `/api/v1/nodes/{node_id}/traffic-reports` | 上报流量增量数据 |
| `POST` | `/api/v1/nodes/{node_id}/heartbeats` | 上报心跳和运行状态 |

### 1.2 认证

所有请求使用 Bearer Token 认证：

```http
Authorization: Bearer <node_token>
```

- Token 标识唯一的节点身份
- `{node_id}` 是资源标识符，不是认证机制
- 禁止在 URL 路径或查询参数中传递 token
- Token 错误返回 `401 Unauthorized`
- Token 与 node_id 不匹配返回 `403 NODE_MISMATCH`

### 1.3 条件请求 (ETag)

配置和用户端点支持 ETag 条件请求：

```http
GET /api/v1/nodes/{node_id}/configuration
If-None-Match: "cfg-0005"
```

- `200 OK`：资源已变更，返回新数据和新 ETag
- `304 Not Modified`：资源未变更，客户端继续使用缓存
- 200 响应必须包含 `Cache-Control: no-store`

### 1.4 错误格式

所有错误使用 RFC 7807 ProblemDetails JSON 格式：

```json
{
  "type": "https://panel.example.com/problems/revision-conflict",
  "title": "Revision conflict",
  "status": 409,
  "code": "REVISION_CONFLICT",
  "detail": "User set belongs to config revision cfg-0005, not cfg-0004.",
  "request_id": "req_01jzabcdef"
}
```

### 1.5 数据模型

**ConfigurationResponse** — 配置响应：

```json
{
  "revision": "cfg-0005",
  "api_version": "v1",
  "node_id": "101",
  "apply_strategy": {
    "on_configuration_change": "recreate_instance",
    "on_user_change": "hot_reload_users"
  },
  "poll_intervals": {
    "configuration_seconds": 60,
    "users_seconds": 30,
    "traffic_seconds": 60,
    "heartbeat_seconds": 120
  },
  "managed_inbounds": [
    {
      "inbound_id": "hy2-main",
      "tag": "hy2-in",
      "protocol": "hysteria2"
    }
  ],
  "clickhouse": {
    "server": "ch.example.com",
    "server_port": 9000,
    "database": "logs",
    "table": "sessions",
    "username": "writer",
    "password": "secret",
    "protocol": "native"
  },
  "sing_box_config_template": { ... }
}
```

`clickhouse` 可选。面板下发地址和凭据后，适配器用本机 `hostname` 作为 ClickHouse `node` 列（写入服务 `tag`），注入 `services[].type=clickhouse`。未设 `table` 时默认 `sessions`。表需预先创建。省略该字段则不注入。

**UserSnapshot** — 用户快照：

```json
{
  "revision": "usr-hy2-0011",
  "configuration_revision": "cfg-0005",
  "node_id": "101",
  "inbound_id": "hy2-main",
  "protocol": "hysteria2",
  "users": [
    {
      "user_id": "10001",
      "name": "10001",
      "credential": {
        "type": "password",
        "password": "user-password"
      }
    }
  ]
}
```

**TrafficReport** — 流量报告：

```json
{
  "started_at": "2026-06-19T10:00:00Z",
  "ended_at": "2026-06-19T10:01:00Z",
  "configuration_revision": "cfg-0005",
  "records": [
    {
      "inbound_id": "hy2-main",
      "user_id": "10001",
      "upload_bytes": 12345,
      "download_bytes": 67890
    }
  ]
}
```

**Heartbeat** — 心跳：

```json
{
  "observed_at": "2026-06-19T10:01:00Z",
  "sing_box_version": "1.12.0",
  "adapter_version": "0.1.0",
  "applied_configuration_revision": "cfg-0005",
  "inbound_statuses": [
    {
      "tag": "hy2-in",
      "protocol": "hysteria2",
      "status": "ok",
      "current_user_count": 1024
    }
  ],
  "runtime": {
    "uptime_seconds": 3600,
    "connections": 128,
    "memory_bytes": 104857600
  }
}
```

### 1.6 用户负载状态

| 状态 | 含义 |
|------|------|
| `ok` | 用户快照已成功应用 |
| `stale` | 轮询失败，保留旧用户快照 |
| `empty_initial_load` | 初次加载失败，入站无可用用户（fail-closed） |
| `revision_conflict` | 用户响应版本与配置版本不匹配，触发配置重新获取 |
| `unsupported_protocol` | 管理入站使用了不支持的协议 |
| `apply_failed` | 用户快照获取成功但应用失败 |

### 1.7 应用策略

**配置变更策略 (`on_configuration_change`)**

| 值 | 说明 |
|----|------|
| `restart_process` | 重新创建 Box 实例（不是 OS 进程重启） |
| `recreate_instance` | 同 `restart_process`，完整 Box 重建 |
| `manual` | 记录待定版本但不自动应用，通过心跳报告 |

**用户变更策略 (`on_user_change`)**

| 值 | 说明 |
|----|------|
| `hot_reload_users` | **v1 唯一支持**。热替换用户，无需重启 Box |
| `restart_process` | v1 不支持，将报告 `apply_failed` |
| `recreate_instance` | v1 不支持，将报告 `apply_failed` |

---

## 2. 测试架构

### 2.1 组件关系

```
┌─────────────────────────────────────────────────────────┐
│                      你的测试代码                        │
│                                                         │
│   ┌─────────────────────────────────────────────────┐   │
│   │           mock.Server (httptest)                │   │
│   │  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │   │
│   │  │ 配置端点  │  │ 用户端点  │  │ 流量/心跳端点 │  │   │
│   │  └──────────┘  └──────────┘  └──────────────┘  │   │
│   │  ┌──────────────────────────────────────────┐   │   │
│   │  │         请求录制 (Transcript)             │   │   │
│   │  └──────────────────────────────────────────┘   │   │
│   └─────────────────────────────────────────────────┘   │
│                         ↕ HTTP                          │
│   ┌─────────────────────────────────────────────────┐   │
│   │          client.Client (panel 客户端)             │   │
│   └─────────────────────────────────────────────────┘   │
│   ┌─────────────────────────────────────────────────┐   │
│   │        适配器组件 (runtime/users/heartbeat)      │   │
│   └─────────────────────────────────────────────────┘   │
│   ┌─────────────────────────────────────────────────┐   │
│   │             断言助手 (assertions)                 │   │
│   └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

### 2.2 两种测试方式

| 方式 | 适用场景 | 启动方式 |
|------|---------|---------|
| **Mock 包** (`internal/paneladapter/mock/`) | Go 单元测试、集成测试 | `mock.New(t)` 内嵌在测试中 |
| **独立服务端** (`cmd/mock-panel-server/`) | 外部集成测试、手动验证 | `./mock-panel-server --port 8080` |

---

## 3. Mock 服务端包 (internal/paneladapter/mock)

### 3.1 快速开始

```go
package my_test

import (
    "testing"
    "github.com/sagernet/sing-box/internal/paneladapter/mock"
)

func TestWithMockServer(t *testing.T) {
    // 创建 Mock 服务器（自动注册 Cleanup）
    s := mock.New(t, mock.WithNodeID("node-001"), mock.WithToken("test-token"))
    
    // 设置配置数据
    cfg := mock.ValidConfigurationResponse()
    cfg.Revision = "cfg-001"
    s.SetConfiguration(cfg)
    
    // 设置用户数据
    snap := mock.ValidUserSnapshot("inb-hy2", "hysteria2", "cfg-001")
    s.SetUserSnapshot("inb-hy2", snap)
    
    // URL 地址
    t.Log("Server URL:", s.URL())
    // 输出: http://127.0.0.1:PORT
}
```

### 3.2 API 参考

#### 创建选项

| 函数 | 默认值 | 说明 |
|------|--------|------|
| `WithNodeID(id string)` | `"default-node"` | 设置预期的 node_id |
| `WithToken(token string)` | `"test-token"` | 设置预期的 Bearer token |
| `WithETagAuto(enabled bool)` | `true` | 启用/禁用自动 ETag 生成 |
| `WithConfigHandler(fn ConfigHandler)` | `nil` | 自定义配置端点处理函数 |

#### 状态设置

```go
// 设置配置响应
s.SetConfiguration(cfg *contract.ConfigurationResponse)

// 设置用户快照
s.SetUserSnapshot(inboundID string, snap *contract.UserSnapshot)

// 设置 HTTP 状态码覆盖（0=默认200）
s.SetConfigStatus(code int)    // 配置端点
s.SetUsersStatus(code int)     // 用户端点
s.SetTrafficStatus(code int)   // 流量端点
s.SetHeartbeatStatus(code int) // 心跳端点
```

#### 请求录制

```go
// 获取所有录制的请求
transcript := s.Transcript()  // []RequestRecord

// 获取解码后的流量报告
reports := s.TrafficReports() // []contract.TrafficReport

// 获取解码后的心跳
hbs := s.Heartbeats()         // []contract.Heartbeat

// 清空录制数据（不影响已设置的响应状态）
s.Reset()
```

`RequestRecord` 结构：

```go
type RequestRecord struct {
    Method  string      // HTTP 方法
    Path    string      // 请求路径
    Headers http.Header // 请求头（已克隆）
    Body    []byte      // 请求体
}
```

### 3.3 数据构造器 (fixtures)

```go
// 默认有效配置（1 个 hysteria2 入站）
cfg := mock.ValidConfigurationResponse()

// 用户快照（2 个用户）
snap := mock.ValidUserSnapshot("inb-hy2", "hysteria2", "cfg-rev-001")

// 流量报告（2 条记录）
report := mock.ValidTrafficReport("cfg-rev-001")

// 心跳（1 个入站状态，状态=ok）
hb := mock.ValidHeartbeat("cfg-rev-001")

// ProblemDetails 错误
errResp := mock.ProblemDetails(400, "BAD_REQUEST", "Invalid request", "detail")

// 构建器模式 — 从默认值开始，逐步覆盖
cfg := mock.NewConfigurationResponse(
    mock.WithManagedInbound("inb-anytls", "anytls-in", "anytls"),
    mock.WithManagedInbound("inb-ss", "ss-in", "shadowsocks"),
)

// 批量生成用户
snap := mock.ValidUserSnapshot("inb-hy2", "hysteria2", "cfg-001")
mock.WithUserCount("inb-hy2", 10)(snap)  // 生成 10 个用户
```

### 3.4 断言助手 (assertions)

```go
// 验证某个请求被调用过
mock.AssertCalled(t, s, "GET", "/configuration")

// 验证某个请求未被调用
mock.AssertNotCalled(t, s, "POST", "/configuration")

// 验证调用次数
mock.AssertCalledN(t, s, "GET", "/configuration", 3)

// 验证所有请求都使用 Bearer 认证
mock.AssertAuthorizationBearer(t, s, "test-token")

// 验证 URL 中没有泄露 token
mock.AssertNoTokenInURL(t, s)

// 验证所有请求都只访问规范的 4 个路径
mock.AssertOnlySpecPaths(t, s, "node-001")

// 验证请求头
mock.AssertHeader(t, s, "POST", "/traffic-reports", "Content-Type", "application/json")

// 验证配置和用户响应包含 Cache-Control: no-store
mock.AssertCacheControlNoStore(t, s)

// 通用 JSON 解码
report := mock.MustDecodeBody[contract.TrafficReport](t, body)
snap := mock.MustDecodeResponse[contract.UserSnapshot](t, record)
```

### 3.5 测试场景示例

#### 场景 1：初始化 + 配置获取

```go
func TestServer_InitAndFetchConfig(t *testing.T) {
    s := mock.New(t, mock.WithNodeID("node-001"), mock.WithToken("token-001"))
    
    cfg := mock.ValidConfigurationResponse()
    cfg.Revision = "cfg-001"
    s.SetConfiguration(cfg)
    
    req, _ := http.NewRequest("GET", s.URL()+"/api/v1/nodes/node-001/configuration", nil)
    req.Header.Set("Authorization", "Bearer token-001")
    resp, _ := http.DefaultClient.Do(req)
    defer resp.Body.Close()
    
    var got contract.ConfigurationResponse
    json.NewDecoder(resp.Body).Decode(&got)
    
    assert.Equal(t, "cfg-001", got.Revision)
    mock.AssertCalled(t, s, "GET", "/configuration")
    mock.AssertAuthorizationBearer(t, s, "token-001")
}
```

#### 场景 2：用户添加

```go
func TestServer_AddUser(t *testing.T) {
    s := mock.New(t)
    
    // 设置 2 个用户
    snap := mock.ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
    s.SetUserSnapshot("inb-hy2", snap)
    
    // 第一次获取：2 个用户
    resp1 := doAuthenticatedGET(t, s, "/inbounds/inb-hy2/users")
    var got1 contract.UserSnapshot
    json.NewDecoder(resp1.Body).Decode(&got1)
    resp1.Body.Close()
    assert.Equal(t, 2, len(got1.Users))
    
    // 更新为 5 个用户
    s.Reset()
    snap2 := mock.ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
    mock.WithUserCount("inb-hy2", 5)(snap2)
    snap2.Revision = "user-rev-002"
    s.SetUserSnapshot("inb-hy2", snap2)
    
    // 第二次获取：5 个用户
    resp2 := doAuthenticatedGET(t, s, "/inbounds/inb-hy2/users")
    var got2 contract.UserSnapshot
    json.NewDecoder(resp2.Body).Decode(&got2)
    resp2.Body.Close()
    assert.Equal(t, 5, len(got2.Users))
    assert.Equal(t, "user-rev-002", got2.Revision)
}
```

#### 场景 3：用户删除

```go
func TestServer_DeleteUser(t *testing.T) {
    s := mock.New(t)
    
    // 设置 3 个用户 → 减少到 1 个
    snap := mock.ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
    mock.WithUserCount("inb-hy2", 3)(snap)
    s.SetUserSnapshot("inb-hy2", snap)
    
    // 验证初始数量
    resp1 := doAuthenticatedGET(t, s, "/inbounds/inb-hy2/users")
    var got1 contract.UserSnapshot
    json.NewDecoder(resp1.Body).Decode(&got1)
    resp1.Body.Close()
    assert.Equal(t, 3, len(got1.Users))
    
    // 删除到 1 个用户
    s.Reset()
    snap2 := mock.ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001")
    mock.WithUserCount("inb-hy2", 1)(snap2)
    snap2.Revision = "user-rev-002"
    s.SetUserSnapshot("inb-hy2", snap2)
    
    resp2 := doAuthenticatedGET(t, s, "/inbounds/inb-hy2/users")
    var got2 contract.UserSnapshot
    json.NewDecoder(resp2.Body).Decode(&got2)
    resp2.Body.Close()
    assert.Equal(t, 1, len(got2.Users))
}
```

#### 场景 4：流量上报

```go
func TestServer_TrafficReport(t *testing.T) {
    s := mock.New(t)
    
    report := mock.ValidTrafficReport("cfg-001")
    body, _ := json.Marshal(report)
    
    req, _ := http.NewRequest("POST", 
        s.URL()+"/api/v1/nodes/default-node/traffic-reports",
        strings.NewReader(string(body)))
    req.Header.Set("Authorization", "Bearer test-token")
    req.Header.Set("Content-Type", "application/json")
    http.DefaultClient.Do(req)
    
    // 验证服务端存储了报告
    reports := s.TrafficReports()
    assert.Equal(t, 1, len(reports))
    assert.Equal(t, "cfg-001", reports[0].ConfigurationRevision)
    assert.Equal(t, int64(1024), reports[0].Records[0].UploadBytes)
}
```

#### 场景 5：心跳上报

```go
func TestServer_Heartbeat(t *testing.T) {
    s := mock.New(t)
    
    hb := mock.ValidHeartbeat("cfg-001")
    body, _ := json.Marshal(hb)
    
    req, _ := http.NewRequest("POST",
        s.URL()+"/api/v1/nodes/default-node/heartbeats",
        strings.NewReader(string(body)))
    req.Header.Set("Authorization", "Bearer test-token")
    req.Header.Set("Content-Type", "application/json")
    http.DefaultClient.Do(req)
    
    // 验证服务端存储了心跳
    hbs := s.Heartbeats()
    assert.Equal(t, 1, len(hbs))
    assert.Equal(t, "1.12.0", hbs[0].SingBoxVersion)
    assert.Equal(t, 1, len(hbs[0].Inbounds))
    assert.Equal(t, contract.UserLoadStatusOK, hbs[0].Inbounds[0].UserLoadStatus)
}
```

#### 场景 6：ETag 条件请求

```go
func TestServer_ETag304(t *testing.T) {
    s := mock.New(t)
    s.SetConfiguration(mock.ValidConfigurationResponse())
    
    // 第一次请求获取 ETag
    req1, _ := http.NewRequest("GET", 
        s.URL()+"/api/v1/nodes/default-node/configuration", nil)
    req1.Header.Set("Authorization", "Bearer test-token")
    resp1, _ := http.DefaultClient.Do(req1)
    etag := resp1.Header.Get("ETag")
    resp1.Body.Close()
    
    // 第二次请求带 If-None-Match → 304
    req2, _ := http.NewRequest("GET",
        s.URL()+"/api/v1/nodes/default-node/configuration", nil)
    req2.Header.Set("Authorization", "Bearer test-token")
    req2.Header.Set("If-None-Match", etag)
    resp2, _ := http.DefaultClient.Do(req2)
    defer resp2.Body.Close()
    
    assert.Equal(t, 304, resp2.StatusCode)
}
```

#### 场景 7：错误模拟

```go
func TestServer_ErrorSimulation(t *testing.T) {
    s := mock.New(t)
    s.SetConfiguration(mock.ValidConfigurationResponse())
    
    // 模拟 503
    s.SetConfigStatus(http.StatusServiceUnavailable)
    resp, _ := doAuthenticatedGET(t, s, "/configuration")
    assert.Equal(t, 503, resp.StatusCode)
    
    // 恢复
    s.SetConfigStatus(0)
    resp, _ = doAuthenticatedGET(t, s, "/configuration")
    assert.Equal(t, 200, resp.StatusCode)
    
    // 模拟 429 Rate Limited
    s.SetConfigStatus(http.StatusTooManyRequests)
    resp, _ = doAuthenticatedGET(t, s, "/configuration")
    assert.Equal(t, 429, resp.StatusCode)
}
```

#### 场景 8：并发安全

```go
func TestServer_ConcurrentAccess(t *testing.T) {
    s := mock.New(t)
    s.SetConfiguration(mock.ValidConfigurationResponse())
    s.SetUserSnapshot("inb-hy2", 
        mock.ValidUserSnapshot("inb-hy2", "hysteria2", "rev-001"))
    
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            doAuthenticatedGET(t, s, "/configuration")
            doAuthenticatedPOST(t, s, "/traffic-reports", 
                mock.ValidTrafficReport("rev-001"))
            cfg := mock.NewConfigurationResponse(
                mock.WithManagedInbound("new", "new", "hysteria2"))
            s.SetConfiguration(cfg)
        }()
    }
    wg.Wait() // 不触发 data race 即通过
}
```

---

## 4. 独立 Mock 服务端 (cmd/mock-panel-server)

### 4.1 编译与运行

```bash
# 编译
go build -o mock-panel-server ./cmd/mock-panel-server/

# 使用默认配置运行
./mock-panel-server

# 指定端口和节点信息
./mock-panel-server --port 8080 --node-id node-sh-01 --token my-secret-token

# 加载配置文件
./mock-panel-server --port 8080 --config-file panel-data.json

# 详细日志输出
./mock-panel-server --port 8080 --pretty
```

### 4.2 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--port` | `8080` | HTTP 监听端口 |
| `--node-id` | `"default-node"` | 节点 ID |
| `--token` | `"test-token"` | Bearer token |
| `--config-file` | `""` | JSON 配置文件路径 |
| `--pretty` | `false` | 格式化输出日志 |

### 4.3 配置文件格式

```json
{
  "configuration": {
    "revision": "cfg-2026-06-20-001",
    "api_version": "v1",
    "node_id": "node-sh-01",
    "apply_strategy": {
      "on_configuration_change": "recreate_instance",
      "on_user_change": "hot_reload_users"
    },
    "poll_intervals": {
      "configuration_seconds": 60,
      "users_seconds": 30,
      "traffic_seconds": 60,
      "heartbeat_seconds": 120
    },
    "managed_inbounds": [
      { "inbound_id": "hy2-main", "tag": "hy2-in", "protocol": "hysteria2" },
      { "inbound_id": "anytls-main", "tag": "anytls-in", "protocol": "anytls" },
      { "inbound_id": "ss-main", "tag": "ss-in", "protocol": "shadowsocks" }
    ],
    "clickhouse": {
      "server": "ch.example.com",
      "server_port": 9000,
      "database": "logs",
      "table": "sessions",
      "username": "writer",
      "password": "secret",
      "protocol": "native"
    },
    "sing_box_config_template": {
      "inbounds": [],
      "outbounds": [{ "type": "direct", "tag": "direct" }]
    }
  },
  "user_snapshots": {
    "hy2-main": {
      "revision": "usr-hy2-003",
      "configuration_revision": "cfg-2026-06-20-001",
      "node_id": "node-sh-01",
      "inbound_id": "hy2-main",
      "protocol": "hysteria2",
      "users": [
        { "user_id": "u-1001", "name": "alice",
          "credential": { "type": "password", "password": "alice-pass" } },
        { "user_id": "u-1002", "name": "bob",
          "credential": { "type": "password", "password": "bob-pass" } }
      ]
    },
    "ss-main": {
      "revision": "usr-ss-001",
      "configuration_revision": "cfg-2026-06-20-001",
      "node_id": "node-sh-01",
      "inbound_id": "ss-main",
      "protocol": "shadowsocks",
      "users": [
        { "user_id": "u-2001", "name": "dave",
          "credential": { "type": "password", "password": "dave-pass" } }
      ]
    }
  }
}
```

### 4.4 使用 curl 测试

```bash
# 基础 URL
BASE="http://localhost:8080"
NODE="node-sh-01"
TOKEN="my-secret-token"

# 1. 获取配置
curl -s -H "Authorization: Bearer $TOKEN" \
  "$BASE/api/v1/nodes/$NODE/configuration" | jq .

# 2. 获取用户
curl -s -H "Authorization: Bearer $TOKEN" \
  "$BASE/api/v1/nodes/$NODE/inbounds/hy2-main/users" | jq .

# 3. 上报流量
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "started_at":"2026-06-20T10:00:00Z",
    "ended_at":"2026-06-20T10:01:00Z",
    "configuration_revision":"cfg-001",
    "records":[{
      "inbound_id":"hy2-main",
      "user_id":"u-1001",
      "upload_bytes":1048576,
      "download_bytes":2097152
    }]
  }' \
  "$BASE/api/v1/nodes/$NODE/traffic-reports"

# 4. 上报心跳
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "observed_at":"2026-06-20T10:01:00Z",
    "sing_box_version":"1.12.0",
    "adapter_version":"0.1.0",
    "applied_configuration_revision":"cfg-001",
    "inbound_statuses":[{
      "tag":"hy2-in",
      "protocol":"hysteria2",
      "status":"ok",
      "current_user_count":2
    }]
  }' \
  "$BASE/api/v1/nodes/$NODE/heartbeats"

# 5. 测试 ETag 304
ETAG=$(curl -sI -H "Authorization: Bearer $TOKEN" \
  "$BASE/api/v1/nodes/$NODE/configuration" | grep -i etag | cut -d' ' -f2)
curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer $TOKEN" \
  -H "If-None-Match: $ETAG" \
  "$BASE/api/v1/nodes/$NODE/configuration"
# 输出: 304

# 6. 测试认证拒绝
curl -s -o /dev/null -w "%{http_code}" \
  "$BASE/api/v1/nodes/$NODE/configuration"
# 输出: 401（无认证）

curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer wrong-token" \
  "$BASE/api/v1/nodes/$NODE/configuration"
# 输出: 403（token 错误）
```

---

## 5. 测试场景与示例

### 5.1 完整的端到端测试

```go
func TestFullLifecycle(t *testing.T) {
    s := mock.New(t, mock.WithNodeID("node-001"), mock.WithToken("token-001"))
    
    // Step 1: Bootstrap — 获取配置
    cfg := mock.NewConfigurationResponse(
        mock.WithManagedInbound("inb-hy2", "hy2-in", "hysteria2"),
        mock.WithManagedInbound("inb-ss", "ss-in", "shadowsocks"),
    )
    s.SetConfiguration(cfg)
    
    // Step 2: 初始用户获取 — fail-closed 验证
    // 不设置用户快照 → 获取应返回 404
    resp := doAuthenticatedGET(t, s, "/inbounds/inb-hy2/users")
    assert.Equal(t, 404, resp.StatusCode)
    
    // Step 3: 首次成功加载用户
    snap := mock.ValidUserSnapshot("inb-hy2", "hysteria2", "cfg-rev-001")
    mock.WithUserCount("inb-hy2", 3)(snap)
    s.SetUserSnapshot("inb-hy2", snap)
    
    resp = doAuthenticatedGET(t, s, "/inbounds/inb-hy2/users")
    var got contract.UserSnapshot
    json.NewDecoder(resp.Body).Decode(&got)
    resp.Body.Close()
    assert.Equal(t, 3, len(got.Users))
    assert.Equal(t, contract.UserLoadStatusOK, "users should be loaded")
    
    // Step 4: 配置变更
    cfg2 := mock.NewConfigurationResponse(
        mock.WithManagedInbound("inb-hy2", "hy2-in", "hysteria2"),
    )
    cfg2.Revision = "cfg-002"
    s.Reset()
    s.SetConfiguration(cfg2)
    
    resp = doAuthenticatedGET(t, s, "/configuration")
    var gotCfg contract.ConfigurationResponse
    json.NewDecoder(resp.Body).Decode(&gotCfg)
    resp.Body.Close()
    assert.Equal(t, "cfg-002", gotCfg.Revision)
    
    // Step 5: 上报流量
    report := mock.ValidTrafficReport("cfg-002")
    doAuthenticatedPOST(t, s, "/traffic-reports", report)
    assert.Equal(t, 1, len(s.TrafficReports()))
    
    // Step 6: 上报心跳
    hb := mock.ValidHeartbeat("cfg-002")
    doAuthenticatedPOST(t, s, "/heartbeats", hb)
    assert.Equal(t, 1, len(s.Heartbeats()))
    
    // Step 7: 全局断言
    mock.AssertOnlySpecPaths(t, s, "node-001")
    mock.AssertAuthorizationBearer(t, s, "token-001")
    mock.AssertNoTokenInURL(t, s)
}
```

### 5.2 全部 47 个测试覆盖的场景

Mock 包自带的测试覆盖了以下完整场景：

```
# 初始化 (3 tests)
TestServer_NewDefault
TestServer_NewWithCustomNodeIDAndToken
TestServer_URLIsValid

# 配置更新 (11 tests)
TestServer_Configuration_GetReturnsConfiguredResponse
TestServer_Configuration_EndpointPathAndMethod
TestServer_Configuration_CacheControlNoStore
TestServer_Configuration_ETagReturned
TestServer_Configuration_IfNoneMatchMatchingETagReturns304
TestServer_Configuration_IfNoneMatchNonMatchingETagReturns200
TestServer_Configuration_RevisionChangesBetweenPolls
TestServer_Configuration_StatusOverride503
TestServer_Configuration_403NodeMismatchOnWrongToken
TestServer_Configuration_401OnMissingAuth
TestServer_Configuration_NoConfigReturns404

# 用户添加 (5 tests)
TestServer_Users_GetReturnsUserSnapshot
TestServer_Users_AddNewUsers
TestServer_Users_UserCountIncreases
TestServer_Users_RevisionChanges
TestServer_Users_MultipleInboundsHaveSeparateSnapshots

# 用户删除 (4 tests)
TestServer_Users_RemoveUsers
TestServer_Users_EmptyUsersSlice
TestServer_Users_UserCountDecreases
TestServer_Users_InboundNotFoundReturns404

# 流量统计 (6 tests)
TestServer_Traffic_PostAcceptsPayload
TestServer_Traffic_BodyDecodedAndStored
TestServer_Traffic_MultipleReportsAccumulated
TestServer_Traffic_InvalidBodyReturns400
TestServer_Traffic_StatusOverride503
TestServer_Traffic_ValidationFailureReturns400

# 心跳上报 (6 tests)
TestServer_Heartbeat_PostAcceptsPayload
TestServer_Heartbeat_BodyDecodedAndStored
TestServer_Heartbeat_MultipleAccumulated
TestServer_Heartbeat_InvalidBodyReturns400
TestServer_Heartbeat_StatusOverride
TestServer_Heartbeat_ValidationFailureReturns400

# 边界情况 (12 tests)
TestServer_UnknownPathReturns404
TestServer_WrongHTTPMethodReturns404
TestServer_NodeIDMismatchInPathReturns403
TestServer_TranscriptCapturesAllRequestsIncludingFailures
TestServer_ResetClearsTranscriptAndPayloads
TestServer_ConcurrentAccessIsSafe
TestServer_AssertAuthorizationBearer
TestServer_AssertNoTokenInURL
TestServer_AssertOnlySpecPaths
TestServer_Users_CacheControlNoStore
TestServer_Users_ETagAndIfNoneMatch
TestServer_Users_StatusOverride
```

### 5.3 运行测试

```bash
# 运行全部 Mock 服务器测试
go test ./internal/paneladapter/mock/ -v

# 运行全部 Panel Adapter 测试
go test ./internal/paneladapter/...

# 运行 Panel Adapter 集成测试
go test ./internal/paneladapter/integration/ -v

# 运行 Panel 客户端测试
go test ./internal/paneladapter/client/ -v

# 测试并发安全性
go test -race ./internal/paneladapter/...
```

---

## 6. 对接面板生产环境

### 6.1 适配器配置

```json
{
  "panel_base_url": "https://panel.example.com",
  "node_id": "node-sh-01",
  "node_token": "nt_xxxxxxxxxxxxxxxxxxxxx",
  "state_path": "/var/lib/sing-box/adapter-state.json",
  "generated_config_path": "/var/lib/sing-box/generated-config.json",
  "http_timeout": "30s",
  "log_level": "info"
}
```

### 6.2 适配器架构

```
┌─────────────────────────────────────────────────────┐
│                sing-box-panel-adapter                │
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌────────┐  ┌──────┐  │
│  │ Config    │  │ Client   │  │Runtime │  │State │  │
│  │ Loader    │  │ (REST)   │  │Manager │  │Store │  │
│  └─────┬─────┘  └────┬─────┘  └───┬────┘  └──┬───┘  │
│        │             │             │          │       │
│        │        ┌────┴────┐  ┌─────┴────┐          │
│        │        │  User   │  │  Traffic │          │
│        │        │  Poller │  │  Tracker │          │
│        │        └────┬────┘  └─────┬────┘          │
│        │             │             │               │
│        └─────────────┴──────┬──────┘               │
│                             │                      │
│                       ┌─────┴─────┐                │
│                       │  Box      │ ← sing-box core│
│                       │ (in-proc) │                │
│                       └─────┬─────┘                │
└─────────────────────────────┼──────────────────────┘
                              │
                     ┌────────┴────────┐
                     │    Panel API    │
                     │  (HTTPS REST)   │
                     └─────────────────┘
```

### 6.3 启动流程

1. 加载本地配置 → 验证字段完整性
2. 创建 Panel 客户端 → `client.New(panelURL, nodeID, token)`
3. 初始化状态存储 → `state.NewStore(path)`
4. Bootstrap → 获取配置、创建 Box、获取用户
5. 启动轮询循环 → 配置/用户/流量/心跳
6. 等待信号 → SIGINT/SIGTERM 优雅关闭

### 6.4 安全要求

- 生产环境必须使用 `https://`
- Token 仅出现在 `Authorization: Bearer` 头中
- 禁止在 URL、日志、配置文件名中包含 Token
- 配置和用户响应必须包含 `Cache-Control: no-store`
- 状态文件不存储 Token、密码和 TLS 密钥
- 日志中必须脱敏 `Authorization`、密码和凭据体

---

## 7. 常见问题

### 7.1 Mock 服务器返回 401

```go
// 错误：没有设置 Authorization 头
http.Get(s.URL() + "/api/v1/nodes/default-node/configuration")

// 正确：设置正确的 Bearer token
req, _ := http.NewRequest("GET", s.URL()+"/api/v1/nodes/...", nil)
req.Header.Set("Authorization", "Bearer test-token")
http.DefaultClient.Do(req)
```

### 7.2 配置不存在返回 404

如果调用 GET configuration 时没有先 `SetConfiguration()`，Mock 服务器会返回 404。

### 7.3 用户快照不存在返回 404

如果调用 GET users 时没有先 `SetUserSnapshot(inboundID, snap)`，Mock 服务器会返回 404。

### 7.4 ETag 导致意外 304

Mock 服务器默认启用自动 ETag。如果两次设置相同的数据，第二次请求带 `If-None-Match` 会返回 304。用 `WithETagAuto(false)` 禁用。

### 7.5 并发测试

Mock 服务器是线程安全的（使用 `sync.Mutex`），可以安全地在多个 goroutine 中同时调用。测试使用 `go test -race` 验证无 data race。

---

## 附录

### A. 相关文件

| 文件 | 说明 |
|------|------|
| `internal/paneladapter/mock/server.go` | Mock 服务器核心 |
| `internal/paneladapter/mock/fixtures.go` | 测试数据构造器 |
| `internal/paneladapter/mock/assertions.go` | 测试断言助手 |
| `internal/paneladapter/mock/mock_test.go` | 47 个测试用例 |
| `cmd/mock-panel-server/main.go` | 独立 Mock 服务端 |
| `internal/paneladapter/contract/models.go` | API 数据模型 |
| `internal/paneladapter/contract/validate.go` | 数据校验规则 |
| `internal/paneladapter/client/client.go` | Panel 客户端 |
| `docs/sing-box-panel-adapter.md` | 适配器使用文档 |

### B. 支持的协议

| 协议 | v1 支持 | 凭证类型 |
|------|---------|---------|
| Hysteria2 | ✅ | `password` |
| AnyTLS | ✅ | `password` |
| Shadowsocks | ✅ | `password` |

### C. HTTP 状态码

| 状态码 | 含义 |
|--------|------|
| `200 OK` | 请求成功 |
| `304 Not Modified` | ETag 匹配，资源未变更 |
| `400 Bad Request` | 请求体无效 |
| `401 Unauthorized` | 缺少或无效的认证 |
| `403 NODE_MISMATCH` | Token 与节点不匹配 |
| `404 Not Found` | 资源不存在 |
| `409 REVISION_CONFLICT` | 版本冲突 |
| `409 IDEMPOTENCY_CONFLICT` | 幂等键冲突 |
| `422 Validation Failed` | 校验失败 |
| `429 Rate Limited` | 请求过频（可重试） |
| `503 Service Unavailable` | 服务暂不可用（可重试） |
