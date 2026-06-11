# iOS 企业内部发布流程 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建立可重复的企业内部 iOS 发布流程，产出 enterprise-signed IPA 并直接部署到用户的 Chuan iPhone（UDID 036DD950-A8BC-5B88-B477-167F1DFB73E1），补齐 AppIcon、Privacy Manifest 和内部版标识，更新文档，所有变更仅限 ios/ + README，严格内部发行（method=enterprise），不触碰照片原文件。

**Architecture:** 复用现有 Xcode 项目（已验证 sim + generic iOS 构建成功）；新建 ios/scripts/package_app.sh 执行 archive（generic/platform=iOS, Release）+ export（exportOptions-enterprise.plist, method=enterprise）；创建 Assets.xcassets/AppIcon（1024x1024 图标由 image_gen 生成）和 PrivacyInfo.xcprivacy；通过精确 pbxproj 编辑接线资源并提升版本到 0.2.0；小幅 UI 加强显示“企业内部版”；直接设备部署使用已知 device ID + devicectl；所有步骤脚本化 + 文档化，验证以实际命令输出为证据。

**Tech Stack:** Xcode 26.5 / xcodebuild (archive/export), devicectl (设备安装), bash 脚本, SwiftUI (现有 iOS 代码 + 少量显示改动), pbxproj 编辑, image_gen (图标), git 提交。

**参考设计文档:** docs/superpowers/specs/2026-06-11-ios-enterprise-release-flow-design.md （已提交，包含完整细节、prompt、代码块、验收标准）。

---

### Task 1: 准备目录与基础文件

**Files:**
- Create: ios/scripts/package_app.sh
- Create: ios/scripts/exportOptions-enterprise.plist
- Create: scripts/package_ios_app.sh (可选 wrapper)

- [ ] **Step 1.1: 创建目录**
  ```bash
  mkdir -p ios/scripts
  ```
  Expected: 目录存在，无错误。

- [ ] **Step 1.2: 写入 exportOptions-enterprise.plist（精确内容）**
  ```bash
  cat > ios/scripts/exportOptions-enterprise.plist << 'PLISTEOF'
  {
    "method": "enterprise",
    "teamID": "3TZ6RCL8NE",
    "signingStyle": "automatic",
    "stripSwiftSymbols": true,
    "thinning": "<thin-for-all-variants>",
    "compileBitcode": false
  }
  PLISTEOF
  ```
  Expected: 文件创建，内容与设计文档第二节匹配。验证：`cat ios/scripts/exportOptions-enterprise.plist`

- [ ] **Step 1.3: 写入 package_app.sh（完整脚本，基于设计）**
  ```bash
  cat > ios/scripts/package_app.sh << 'SCRIPTEOF'
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
  SCRIPTEOF
  chmod +x ios/scripts/package_app.sh
  ```
  Expected: 脚本可执行，内容与设计匹配。验证：`head -20 ios/scripts/package_app.sh && ls -l ios/scripts/package_app.sh`

- [ ] **Step 1.4: 写入根 wrapper（可选，保持与 macOS 一致）**
  ```bash
  cat > scripts/package_ios_app.sh << 'WRAPEOF'
  #!/usr/bin/env bash
  set -euo pipefail
  ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
  exec "$ROOT_DIR/ios/scripts/package_app.sh"
  WRAPEOF
  chmod +x scripts/package_ios_app.sh
  ```
  Expected: wrapper 创建。验证：`ls -l scripts/package_ios_app.sh`

- [ ] **Step 1.5: Commit**
  ```bash
  git add ios/scripts/ scripts/package_ios_app.sh
  git commit -m "feat(ios): add enterprise package scripts and exportOptions (internal only)"
  ```
  Expected: commit 成功。

### Task 2: 创建资源文件（AppIcon + Privacy + 图标生成）

**Files:**
- Create: ios/Assets.xcassets/AppIcon.appiconset/Contents.json
- Create: ios/Assets.xcassets/AppIcon.appiconset/AppIcon.png (1024x1024)
- Create: ios/PrivacyInfo.xcprivacy

- [ ] **Step 2.1: 创建 asset catalog 目录结构**
  ```bash
  mkdir -p ios/Assets.xcassets/AppIcon.appiconset
  ```
  Expected: 目录存在。

- [ ] **Step 2.2: 写入 Contents.json**
  ```bash
  cat > ios/Assets.xcassets/AppIcon.appiconset/Contents.json << 'JSONEOF'
  {
    "images" : [
      {
        "filename" : "AppIcon.png",
        "idiom" : "universal",
        "platform" : "ios",
        "size" : "1024x1024"
      }
    ],
    "info" : {
      "author" : "xcode",
      "version" : 1
    }
  }
  JSONEOF
  ```
  Expected: 文件匹配设计。

- [ ] **Step 2.3: 使用 image_gen 生成 AppIcon.png（1024x1024，专业企业内部风格）**
  调用 image_gen 工具，prompt: "专业简洁的 iOS App 图标，适用于照片资产管理器（Photo Asset Manager）企业内部应用。深色优雅风格，简洁的照片/相册/资产符号（如折叠画廊或山形文件夹），高对比度，现代企业感，适合暗色 UI（参考 LinearGradient 深色主题），无文字，1024x1024 正方形 PNG，透明背景处理得当。"
  aspect_ratio: "1:1"
  保存为 ios/Assets.xcassets/AppIcon.appiconset/AppIcon.png
  Expected: 图标文件存在，大小合理（~几十KB 到几MB），视觉专业。

- [ ] **Step 2.4: 写入 PrivacyInfo.xcprivacy（精确最小合规内容）**
  ```bash
  cat > ios/PrivacyInfo.xcprivacy << 'PRIVACYEOF'
  <?xml version="1.0" encoding="UTF-8"?>
  <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
  <plist version="1.0">
  <dict>
      <key>NSPrivacyTracking</key>
      <false/>
      <key>NSPrivacyTrackingDomains</key>
      <array/>
      <key>NSPrivacyAccessedAPITypes</key>
      <array>
          <dict>
              <key>NSPrivacyAccessedAPIType</key>
              <string>NSPrivacyAccessedAPICategoryUserDefaults</string>
              <key>NSPrivacyAccessedAPITypeReasons</key>
              <array>
                  <string>CA92.1</string>
              </array>
          </dict>
          <dict>
              <key>NSPrivacyAccessedAPIType</key>
              <string>NSPrivacyAccessedAPICategoryFileTimestamp</string>
              <key>NSPrivacyAccessedAPITypeReasons</key>
              <array>
                  <string>C617.1</string>
              </array>
          </dict>
      </array>
  </dict>
  </plist>
  PRIVACYEOF
  ```
  Expected: 文件存在，内容与设计第二节完全一致。验证：`cat ios/PrivacyInfo.xcprivacy`

- [ ] **Step 2.5: Commit**
  ```bash
  git add ios/Assets.xcassets/ ios/PrivacyInfo.xcprivacy
  git commit -m "feat(ios): add AppIcon asset catalog (generated) and PrivacyInfo.xcprivacy for enterprise build"
  ```

### Task 3: pbxproj 接线与版本更新

**Files:**
- Modify: ios/PhotoAssetManagerIOS.xcodeproj/project.pbxproj

- [ ] **Step 3.1: 读取当前 pbxproj 确认结构（使用 read_file 或 grep 工具）**
  确认 Resources 阶段为空、target 配置有 DEVELOPMENT_TEAM = 3TZ6RCL8NE、版本 0.1.0/1。
  Expected: 结构与设计文档第三节匹配。

- [ ] **Step 3.2: 精确 search_replace 添加 FileReference 和 BuildFile（使用设计中示例 ID 策略，实际用不冲突 ID，如 A100001A 等）**
  在 PBXFileReference 区后插入 Assets 和 Privacy 引用。
  在 PBXBuildFile 区插入对应。
  在 A10000220000000000000001 /* Resources */ 的 files 列表追加 ID。
  在主 PBXGroup 添加引用。
  （具体字符串匹配使用设计文档中的上下文，确保唯一。）

- [ ] **Step 3.3: 更新两个 target XCBuildConfiguration 中的版本**
  将 MARKETING_VERSION 改为 "0.2.0"，CURRENT_PROJECT_VERSION 改为 "2"（Debug 和 Release target 级）。
  使用 search_replace 针对 "MARKETING_VERSION = 0.1.0;" 等精确替换。

- [ ] **Step 3.4: 验证 pbxproj 语法（xcodebuild -project ... -showBuildSettings 或 dry build）**
  ```bash
  xcodebuild -project ios/PhotoAssetManagerIOS.xcodeproj -scheme PhotoAssetManagerIOS -showBuildSettings | grep -E '(MARKETING_VERSION|CURRENT_PROJECT_VERSION|DEVELOPMENT_TEAM)'
  ```
  Expected: 看到 0.2.0 / 2 / 3TZ6RCL8NE。

- [ ] **Step 3.5: Commit**
  ```bash
  git add ios/PhotoAssetManagerIOS.xcodeproj/project.pbxproj
  git commit -m "chore(ios): wire Assets.xcassets and PrivacyInfo into pbxproj, bump to 0.2.0 for enterprise release"
  ```

### Task 4: 小幅 UI 可用性加强（内部版标识）

**Files:**
- Modify: ios/Sources/PhotoAssetManagerIOS/PhotoAssetManagerIOSApp.swift (主要)
- Modify: ios/Sources/PhotoAssetManagerIOS/WaterfallGalleryView.swift (如需)

- [ ] **Step 4.1: 在 IOSSyncStatusBar 或 toolbar 加入“企业内部版”**
  例如在标题或状态文字中添加静态或 Bundle 版本显示。
  精确改动：参考设计，添加 Text("企业内部版") 或版本行。使用 search_replace 针对具体 View 代码块。

- [ ] **Step 4.2: 在 IOSGalleryEmptyState 和设置页“说明” Section 追加内部提示**
  更新说明文字，加入 “（本构建为企业内部发行，仅授权设备）” 和版本。
  精确匹配现有 Text 字符串替换或插入。

- [ ] **Step 4.3: 验证编译（无签名构建测试）**
  ```bash
  xcodebuild \
    -project ios/PhotoAssetManagerIOS.xcodeproj \
    -scheme PhotoAssetManagerIOS \
    -destination 'generic/platform=iOS Simulator' \
    CODE_SIGNING_ALLOWED=NO \
    build
  ```
  Expected: BUILD SUCCEEDED（资源和改动生效）。

- [ ] **Step 4.4: Commit**
  ```bash
  git add ios/Sources/PhotoAssetManagerIOS/PhotoAssetManagerIOSApp.swift
  git commit -m "feat(ios): add enterprise internal build labels and version in UI for released app"
  ```

### Task 5: 更新 README 文档

**Files:**
- Modify: README.md

- [ ] **Step 5.1: 在现有 iOS 章节后新增完整“## iOS 企业内部发布”章节**
  内容精确复制设计文档第四节，包括前置、打包命令、直接部署命令（含具体 UDID）、信任证书步骤、内部分发说明。
  使用 search_replace 在合适位置插入整段 Markdown。

- [ ] **Step 5.2: 轻微调整原有 iOS 章节说明 simulator 仅开发用**
  添加引用新发布章节的句子。

- [ ] **Step 5.3: 验证文档（无工具，人工或 grep）**
  Expected: 新章节完整、命令可复制、无公开分发痕迹。

- [ ] **Step 5.4: Commit**
  ```bash
  git add README.md
  git commit -m "docs: add complete iOS enterprise internal release section with direct device deploy steps"
  ```

### Task 6: 验证打包流程（产出 IPA）

**Files:** (无新代码，运行验证)

- [ ] **Step 6.1: 运行 package 脚本**
  ```bash
  ./ios/scripts/package_app.sh
  ```
  Expected: 成功，输出 "Enterprise IPA ready"，文件存在于 ios/.build/enterprise/PhotoAssetManagerIOS.ipa，大小合理。

- [ ] **Step 6.2: 验证 IPA（enterprise 签名）**
  ```bash
  unzip -l ios/.build/enterprise/PhotoAssetManagerIOS.ipa | grep -E '(AppIcon|PrivacyInfo)'
  codesign -dv --verbose=4 ios/.build/enterprise/PhotoAssetManagerIOS.ipa 2>&1 | grep -E '(Authority|TeamIdentifier)'
  ```
  Expected: 看到图标/隐私文件，签名显示 enterprise / team 3TZ6RCL8NE。

- [ ] **Step 6.3: Commit 验证相关（如有输出日志或 .build 忽略）**
  （.build 通常 gitignore，确认 .gitignore 已覆盖）

### Task 7: 验证直接发布到我的 iOS（Chuan iPhone）

**Files:** (运行命令)

- [ ] **Step 7.1: 执行直接 signed build 到设备**
  ```bash
  xcodebuild \
    -project ios/PhotoAssetManagerIOS.xcodeproj \
    -scheme PhotoAssetManagerIOS \
    -destination 'platform=iOS,id=036DD950-A8BC-5B88-B477-167F1DFB73E1' \
    -configuration Release \
    build
  ```
  Expected: BUILD SUCCEEDED，.app 出现在 DerivedData Release-iphoneos。

- [ ] **Step 7.2: 使用 devicectl 安装**
  ```bash
  APP_PATH=$(find ~/Library/Developer/Xcode/DerivedData -path '*PhotoAssetManagerIOS.app' -type d | grep Release-iphoneos | head -1)
  xcrun devicectl device install app --device 036DD950-A8BC-5B88-B477-167F1DFB73E1 "$APP_PATH"
  ```
  Expected: 成功输出 "Installed" 或类似，设备上 App 出现。

- [ ] **Step 7.3: 手动验证（用户侧）**
  - 在 iPhone 上打开 App，确认图标为生成的，UI 显示“企业内部版”，能进入设置，配置 control-plane 后 sync 工作（用户提供可用实例）。
  - 首次需在 设置 > 通用 > VPN 与设备管理 信任企业证书。
  Expected: App 正常运行，无 crash，内部标识可见。

- [ ] **Step 7.4: 最终验证命令输出收集**
  记录所有命令的 exit code 和关键日志作为证据。

- [ ] **Step 7.5: 最终 commit（清理或记录）**
  ```bash
  git commit -m "verify: complete iOS enterprise release flow - IPA packaged and deployed to Chuan iPhone" --allow-empty
  ```

## Self-Review of this Plan
- 所有步骤 bite-sized (2-5min)，有精确命令/代码/预期输出。
- 覆盖设计文档所有细节（脚本、资源、pbxproj、UI、README、验证）。
- TDD-like：先创建/编辑，再运行验证命令（build/test-like），再 commit。
- 频繁 commit，每任务结束。
- YAGNI：只做企业 IPA + 直接设备部署 + 最小资源/UI 加强，无多余。
- 企业约束：所有签名/方法用 "enterprise"，无 ASC/TestFlight。
- 设备 ID 写死在命令中（匹配设计）。
- 风险处理：脚本 set -euo，文档信任步骤，验证以实际输出为准。
- 准备好 subagent 或 inline 执行。

**Plan complete and saved to `docs/superpowers/plans/2026-06-11-ios-enterprise-release-flow.md`.**

Two execution options:
1. **Subagent-Driven (recommended)** - Dispatch fresh subagent per task + two-stage review.
2. **Inline Execution** - Execute tasks in this session using executing-plans, with checkpoints.

Which approach? (I recommend 1 for complex multi-file + verification.)

If Subagent-Driven: Use superpowers:subagent-driven-development.
If Inline: Use superpowers:executing-plans.

After your choice, we start task-by-task. All steps will produce evidence before claiming success (per verification-before-completion).