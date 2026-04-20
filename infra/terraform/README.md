# Photo Asset Manager Infra

这里是 AWS control plane 和 S3 衍生图层的 Terraform 入口。

第一版 infra 边界：

- AWS control plane 保存 ledger、设备游标、冲突状态、归档 receipt 和 derivative metadata。
- S3 长期保存 thumbnail 与 preview 两类非原片衍生图。
- S3 不保存 RAW、原始 JPEG、sidecar canonical 或导出 canonical。
- 原片 canonical store 仍然是单一 NAS/server，可以离线。

## 本地检查

```bash
terraform init
terraform fmt -check
terraform validate
```

本目录当前只定义 provider、公共变量和标准 tag 面，后续资源实现应继续保持 ledger 与原片存储分离。
