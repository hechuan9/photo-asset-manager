# Photo Asset Manager

本仓库包含 macOS 照片资产管理器客户端，以及后续 AWS control plane / S3 衍生图层的基础设施定义。

## 目录

- `macos/`：SwiftPM macOS app、测试和本地打包脚本。
- `infra/terraform/`：AWS control plane 与 S3 衍生图层的 Terraform 入口。
- `feature.md`：产品和同步架构设计记录。

## macOS app

```bash
cd macos
swift test
swift build
./scripts/pre_merge_gate.sh
./scripts/package_app.sh
open .build/app/PhotoAssetManager.app
```

根目录保留兼容入口：

```bash
./scripts/pre_merge_gate.sh
./scripts/package_app.sh
open macos/.build/app/PhotoAssetManager.app
```

## Infra

```bash
cd infra/terraform
terraform init
terraform fmt -check
terraform validate
```

当前 infra 目录只建立 AWS control plane / S3 衍生图层的 Terraform 边界和变量面，还没有申请真实云资源。原片、RAW、sidecar canonical 不应进入 S3。
