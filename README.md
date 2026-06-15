# Keeps

本仓库包含 macOS / iOS 照片资产管理器客户端，以及 NAS-first control plane / preview 衍生图层的部署定义。

## 目录

- `macos/`：SwiftPM macOS app、测试和本地打包脚本。
- `ios/`：Xcode iOS app，当前先做自动同步回放验证和瀑布流图库。
- `control_plane/`：FastAPI/SQLAlchemy 后端、ledger API、preview derivative storage 和测试。
- `deploy/nas/`：NAS + Docker Compose 生产入口，默认把 Keeps 状态放在 `/myphoto/keeps` 下。
- `feature.md`：产品和同步架构设计记录。

## macOS app

```bash
cd macos
swift test
swift build
./scripts/pre_merge_gate.sh
./scripts/package_app.sh
open .build/app/Keeps.app
```

根目录保留兼容入口：

```bash
./scripts/pre_merge_gate.sh
./scripts/package_app.sh
open macos/.build/app/Keeps.app
```

## iOS app

```bash
open ios/KeepsIOS.xcodeproj

xcodebuild \
  -project ios/KeepsIOS.xcodeproj \
  -scheme KeepsIOS \
  -destination 'generic/platform=iOS Simulator' \
  CODE_SIGNING_ALLOWED=NO \
  build
```

（注意：以上仅 Simulator 构建，供日常开发调试使用。企业内部发布、IPA 打包及直接部署到设备请参考下文“## iOS 企业内部发布（仅内部 In-House 发行）”章节。）

iOS 端当前是最小只读回放器：

- 首次打开会在 app 自己的 sandbox 里创建本地 `Library.sqlite`。
- 设置页里填入 control-plane base URL、`libraryID` 和可选 access token 后，app 会在前台自动拉取远端 ledger 并回放到本地投影；设置页里的“立即刷新”只用于手动补拉一次。
- 远端 ledger 会 materialize 成本地 `assets` 投影；瀑布流只读本地预览图缓存或通过 derivative metadata 下载远端预览图，不会回退去读原图路径。
- 当前不支持 iOS 侧扫描、导入、归档或修改原片文件，也不会把原图同步到 iOS。

macOS 端当前会把可同步的资料库变化自动写入本地 ledger，并在配置好 control-plane 后自动上传：

- 扫描、导入、元数据回填、评分、旗标、标签、回收站状态等变化都会先写本地 ledger，再由后台自动同步。
- bootstrap 只补资产快照和原片 placement 快照；预览图声明必须在本地生成并上传预览图后再进入 ledger，不会伪造远端 derivative。
- 当前只同步 1200px HEIF (`.heic`) 预览图；原图、RAW、sidecar canonical 仍只留在 macOS / NAS 一侧，不会上行到 iOS。

## NAS 部署

默认生产边界是 NAS + Docker Compose。Keeps 相关服务端状态统一放在 `/myphoto/keeps` 下，原片根目录必须放在 Keeps 外部：

```text
/myphoto/
  keeps/
    db/
    ledger/
    previews/
    cache/
    ingest/
    exports/
    backups/
    logs/
    tmp/
  library/
```

启动：

```bash
cd deploy/nas
cp .env.example .env
docker compose up -d --build
```

`/myphoto/keeps/previews` 保存 1200px `.heic` preview derivative；`/myphoto/keeps/db` 保存 Postgres 数据。原片、RAW、sidecar canonical 和 canonical export 永远不放进 `/myphoto/keeps`，第一阶段通过 `/myphoto/library` 只读挂载给 control-plane。

## iOS 企业内部发布（仅内部 In-House 发行）

**前置条件**：
- 本机已用企业账户登录 Xcode（team 3TZ6RCL8NE）。
- “Chuan iPhone” 已配对（UDID 036DD950-A8BC-5B88-B477-167F1DFB73E1）。
- 首次安装需在 iPhone “设置 > 通用 > VPN 与设备管理” 信任企业开发者证书。

**打包企业 IPA**（推荐用于内部分发）：
```bash
./ios/scripts/package_app.sh
# 输出：ios/.build/enterprise/KeepsIOS.ipa
```

**直接发布到我的 iOS（Chuan iPhone，端到端验证）**：
```bash
xcodebuild \
  -project ios/KeepsIOS.xcodeproj \
  -scheme KeepsIOS \
  -destination 'platform=iOS,id=036DD950-A8BC-5B88-B477-167F1DFB73E1' \
  -configuration Release \
  build

xcrun devicectl device install app \
  --device 036DD950-A8BC-5B88-B477-167F1DFB73E1 \
  $(find ~/Library/Developer/Xcode/DerivedData -path '*Keeps.app' | head -1)
```

**内部分发说明**：
- IPA 可通过内部 HTTPS + manifest、MDM、Apple Configurator、邮件等方式分发给授权设备。
- 保留原有 simulator 命令用于日常开发调试。

## Control plane

```bash
cd control_plane
uv run pytest

export CONTROL_PLANE_DATABASE_URL='sqlite+pysqlite:////myphoto/keeps/db/control_plane.sqlite'
export DERIVATIVE_STORAGE_BACKEND=filesystem
export KEEPS_ROOT=/myphoto/keeps
export ORIGINAL_ROOT=/myphoto/library
export CONTROL_PLANE_PUBLIC_BASE_URL='http://localhost:2283'
uv run uvicorn control_plane.app:app --host 0.0.0.0 --port 2283

docker buildx build \
  --platform linux/arm64 \
  -f control_plane/Dockerfile.nas \
  -t keeps-control-plane:local \
  .
```

后端通过 control-plane API 访问 NAS-hosted authoritative event store，不允许客户端直连数据库或服务端目录。`actorID == "server"` / trusted device 只是第一版开发和测试授权 stub，不是最终生产权限边界；生产环境应通过迁移管理 schema，并显式关闭自动建表。
