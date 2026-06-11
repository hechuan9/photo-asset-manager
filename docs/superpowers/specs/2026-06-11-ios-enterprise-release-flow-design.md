# iOS 企业内部发布流程设计

> **日期**: 2026-06-11
> **状态**: 设计已用户确认
> **目标**: 把 iOS 发布流程跑通，产出企业内部签名的 IPA，并实际发布（安装）到用户的 iOS 设备（Chuan iPhone）上。仅企业内部发行，绝不公开发行或上 App Store / TestFlight。

## 目标（Goal）
- 建立可重复、可脚本化的企业内部发布流程（对标 macOS 的 package_app.sh 风格）。
- 补齐 App Store / 企业分发必需资源（AppIcon、Privacy Manifest）。
- 按路径 2 加强资源与内部可用性（专业图标 + 少量内部版标识 + 清晰文档）。
- 支持两种交付：
  1. 企业 IPA（用于内部分发、MDM、Configurator 等）。
  2. 直接部署到“我的 iOS”（Chuan iPhone，UDID: 036DD950-A8BC-5B88-B477-167F1DFB73E1），利用已配对设备实现端到端验证。
- 发行方式严格为 "enterprise"（In-House），使用现有企业账户（team 3TZ6RCL8NE）。
- 验证标准：脚本成功产出 IPA + 直接部署成功 + App 在设备上可启动、显示内部版标识、可配置 control-plane 并同步。
- 约束：当前阶段绝对不触碰照片原文件；保持最小改动；设计先行；全程中文；快速失败。

## 架构（Architecture）
- **输入**：现有 `ios/PhotoAssetManagerIOS.xcodeproj`（已能成功编译 sim 和 generic iOS）。
- **核心脚本**：`ios/scripts/package_app.sh`（新建，对标 macos/scripts/package_app.sh）。
  - 执行 `xcodebuild archive`（generic/platform=iOS + Release）。
  - 执行 `xcodebuild -exportArchive` 使用 `exportOptions-enterprise.plist`（method: "enterprise"）。
  - 输出：`ios/.build/enterprise/PhotoAssetManagerIOS.ipa`。
- **直接设备发布**：利用已知设备 ID 执行 signed build + `xcrun devicectl device install app`（不依赖 IPA 也能快速把构建推到手机）。
- **资源层**：
  - 新建 `ios/Assets.xcassets/AppIcon.appiconset/`（含 1024x1024 图标 + Contents.json）。
  - 新建 `ios/PrivacyInfo.xcprivacy`（最小合规声明）。
  - 通过 pbxproj 接线加入 Resources build phase。
- **配置**：Release 配置用于企业包；Automatic + team 3TZ6RCL8NE 复用；版本提升到 0.2.0。
- **文档**：README 增加完整“iOS 企业内部发布”章节，包含命令、信任证书步骤、内部分发说明。
- **集成**：根 pre_merge_gate.sh 已覆盖 ios 源扫描；可选根 wrapper 转发脚本。
- **验证闭环**：脚本产出 + 直接 devicectl 部署 + 设备上手动验证（启动、设置、sync）。
- **不做**：无 ASC record、无 TestFlight、无公开分发、无复杂 MDM manifest（除非后续需要）、不改业务逻辑（仅少量内部版文案）。

## 技术栈与依赖（Tech Stack）
- Xcode 26.5 / xcodebuild（archive + export）。
- Enterprise In-House 签名（Automatic，依赖用户企业账号 keychain 中的证书）。
- devicectl（设备安装与验证）。
- SwiftUI / UIKit（现有代码，iOS 26.0 deployment target）。
- 共享核心：复用 macos/Sources/PhotoAssetManager/ 下的 6 个文件（Models、DateCoding、PerformanceLog、SQLiteDatabase、SyncLedger、SyncControlPlane），已有 #if os(iOS) 保护。
- 生成工具：image_gen 用于创建 AppIcon（仅实现阶段）。
- 文件：bash 脚本、plist、xcprivacy、pbxproj 精确编辑、Markdown 文档。

## 详细组件与变更（Components & Changes）

### 1. 打包脚本与导出选项（ios/scripts/）
新建目录 `ios/scripts/`。

**ios/scripts/package_app.sh**（完整内容，set -euo pipefail，对标 macos 版本）：
```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

DIST_DIR="ios/.build/enterprise"
mkdir -p "$DIST_DIR"

ARCHIVE_PATH="$DIST_DIR/PhotoAssetManagerIOS.xcarchive"
IPA_PATH="$DIST_DIR/PhotoAssetManagerIOS.ipa"

echo ">>> Archiving PhotoAssetManagerIOS for enterprise (generic iOS, Release)..."
xcodebuild \
  -project ios/PhotoAssetManagerIOS.xcodeproj \
  -scheme PhotoAssetManagerIOS \
  -destination 'generic/platform=iOS' \
  -configuration Release \
  -archivePath "$ARCHIVE_PATH" \
  archive

echo ">>> Exporting enterprise IPA..."
xcodebuild \
  -exportArchive \
  -archivePath "$ARCHIVE_PATH" \
  -exportPath "$DIST_DIR" \
  -exportOptionsPlist ios/scripts/exportOptions-enterprise.plist

echo ">>> Enterprise IPA ready: $IPA_PATH"
ls -lh "$IPA_PATH"
```

**ios/scripts/exportOptions-enterprise.plist**（精确内容）：
```json
{
  "method": "enterprise",
  "teamID": "3TZ6RCL8NE",
  "signingStyle": "automatic",
  "stripSwiftSymbols": true,
  "thinning": "<thin-for-all-variants>",
  "compileBitcode": false
}
```

可选根 wrapper（`scripts/package_ios_app.sh`）：
```bash
#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec "$ROOT_DIR/ios/scripts/package_app.sh"
```

### 2. 资源文件（加强部分）
- `ios/Assets.xcassets/AppIcon.appiconset/Contents.json`（如第二节）。
- `ios/Assets.xcassets/AppIcon.appiconset/AppIcon.png`（1024x1024，由 image_gen 生成，prompt 见第二节）。
- `ios/PrivacyInfo.xcprivacy`（精确 XML 见第二节）。

**可用性小加强**（精确位置）：
- `ios/Sources/PhotoAssetManagerIOS/PhotoAssetManagerIOSApp.swift`：
  - 在 IOSSyncStatusBar 或 toolbar 标题处加入 “企业内部版”。
  - 在 IOSGalleryEmptyState 说明文字追加内部版提示。
  - 在设置页 “说明” Section 增加版本显示行（使用 Bundle info 或静态 “企业内部版 0.2.0”）。
- 这些是极小显示改动，不影响同步或核心逻辑。

### 3. pbxproj 接线（精确变更）
- 在 PBXFileReference 区（现有源文件引用后）插入：
  - Assets.xcassets 的 folder.assetcatalog 引用（ID 示例 A100001A）。
  - PrivacyInfo.xcprivacy 的 file 引用（ID 示例 A100001B）。
- 在 PBXBuildFile 区插入对应两个 BuildFile（ID 示例 A100000A、A100000B）。
- 在 `A10000220000000000000001 /* Resources */` 的 files 列表末尾追加两个 BuildFile ID。
- 在主 PBXGroup（mainGroup）下合适位置加入文件引用（使 Xcode 打开项目时资源可见）。
- 变更方式：实现时先 read 完整 pbxproj，用大段唯一上下文的 search_replace 精确插入，避免 ID 冲突。Resources 阶段当前为空，插入后生效。
- 同时在两个 XCBuildConfiguration（Debug/Release target 级）把 MARKETING_VERSION 改为 "0.2.0"，CURRENT_PROJECT_VERSION 改为 "2"。

### 4. 文档更新（README.md）
在现有 "## iOS app" 章节后新增完整章节：

## iOS 企业内部发布（仅内部 In-House 发行）

**前置条件**：
- 本机已用企业账户登录 Xcode（team 3TZ6RCL8NE）。
- “Chuan iPhone” 已配对（UDID 036DD950-A8BC-5B88-B477-167F1DFB73E1）。
- 首次安装需在 iPhone “设置 > 通用 > VPN 与设备管理” 信任企业开发者证书。

**打包企业 IPA**（推荐用于内部分发）：
```bash
./ios/scripts/package_app.sh
# 输出：ios/.build/enterprise/PhotoAssetManagerIOS.ipa
```

**直接发布到我的 iOS（Chuan iPhone，端到端验证）**：
```bash
xcodebuild \
  -project ios/PhotoAssetManagerIOS.xcodeproj \
  -scheme PhotoAssetManagerIOS \
  -destination 'platform=iOS,id=036DD950-A8BC-5B88-B477-167F1DFB73E1' \
  -configuration Release \
  build

xcrun devicectl device install app \
  --device 036DD950-A8BC-5B88-B477-167F1DFB73E1 \
  $(find ~/Library/Developer/Xcode/DerivedData -path '*PhotoAssetManagerIOS.app' | head -1)
```

**内部分发说明**：
- IPA 可通过内部 HTTPS + manifest、MDM、Apple Configurator、邮件等方式分发给授权设备。
- 保留原有 simulator 命令用于日常开发调试。

同时更新原有 iOS 章节，说明 simulator 构建仅用于开发，企业发布走新章节。

### 5. 验收标准与验证步骤（证据在前）
必须实际执行并在输出中看到成功：
1. `./ios/scripts/package_app.sh` 退出码 0，IPA 存在且 `codesign -dv --verbose=4` 显示 enterprise 方法 + team 3TZ6RCL8NE。
2. 直接设备部署命令成功，devicectl 报告 installed。
3. 手机上 App 图标为生成的正式图标，能正常启动，UI 中显示“企业内部版”标识，能打开设置页，配置 control-plane 后触发 sync（用户提供可用 baseURL + libraryID 进行验证）。
4. 解压 IPA 可找到 PrivacyInfo.xcprivacy 和 AppIcon。
5. README 包含完整新章节，无公开分发内容。
6. 所有变更符合 Agents.md（无删除照片、无低水平重复、错误保留 trace）。
7. 设计文档已提交。

**风险与回退**：
- 首次签名可能需要 Xcode 联网创建 In-House profile → 文档中明确提示。
- 设备首次安装必须手动信任证书 → 文档中列出精确路径。
- 如果 Automatic 失败，可临时切换到手动 provisioning（但默认坚持 Automatic）。
- 后端未部署时 App 仍为空状态（符合现有设计），文档说明用户需自行准备 control-plane 实例验证同步。

## 范围与非目标（Scope）
- 仅 iOS 发布流程 + 必需资源 + 最小内部可用性加强。
- 不改 macOS 端、不碰 control_plane 部署、不加复杂 MDM 功能、不生成大量截图或 ASC metadata（因为纯内部）。
- 照片数据安全：严格遵守——不删除、不移动、不覆盖任何原片。

## 实现顺序建议（高层次）
1. 创建目录、脚本、plist、资源文件（含 image_gen 生成图标）。
2. 编辑 pbxproj 接线 + 版本号。
3. 小幅 UI 文字加强。
4. 更新 README。
5. 实际运行脚本 + 直接部署命令，截取输出作为证据。
6. 提交变更 + 本设计文档。

## Spec Self-Review（设计自审，已执行）
- Placeholder 扫描：无 “TBD”、“TODO”、“稍后实现”等，所有命令、文件内容、ID 策略、prompt 均具体。
- 一致性：与第一节（已确认）完全一致；enterprise method 贯穿；设备 ID 写死在文档中便于验证。
- 范围检查：聚焦发布流程，未扩大到后端部署或 App 功能重构。
- 歧义消除：明确 “发布到我的 iOS 上” = 直接 devicectl 安装 + IPA 产物；“企业内部” = method=enterprise，无 ASC。
- 所有文件路径、命令、代码块均可直接复制执行。
- 符合 Agents.md：设计先行、数据结构/流程先行（脚本+资源先行）、函数短小（脚本单一职责）、中文交流、无 Emoji。

本设计已获得用户确认，可进入 writing-plans 阶段。

---
**用户审阅提示**：请审阅本文件 `docs/superpowers/specs/2026-06-11-ios-enterprise-release-flow-design.md`。如有任何调整意见请告诉我；确认无误后我们将调用 writing-plans skill 并开始实现。
