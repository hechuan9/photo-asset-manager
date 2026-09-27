# 统一 HTTP 资料库验证

本轮移除 Mac 的 NAS / 本地暂存分区，统一为全部照片、精选、回收站；服务器来源默认收起，HTTP 导航与图库查询独立。共享 Swift DTO 及 Rust 导航响应仅保留 path/directories，不返回未实现的本地分区。

## 自动验证

- macOS Swift Testing：17 项通过，含切换范围清空旧选择/筛选、来源失败不阻止图库查询、服务不可用不回退本地。
- 共享 KeepsAPI Swift Testing：8 项通过。
- Rust `cargo test navigation`：3 项单元测试、3 项 HTTP 集成测试通过。
- iOS Simulator Debug 构建：通过。
- macOS 应用构建、打包、签名验证：通过。
- `git diff --check`：通过。

## 运行验证

从工作区启动新构建，确认只有统一资料库入口，来源目录默认收起。首次请求出现 NSURLErrorDomain -1009 / Local network prohibited；随后来源 HTTP 请求成功，点击图库重试后成功显示总数 117,344、首批 100 张照片。

真实来源返回 myphoto 与 photo。选择照片后切换单张视图，打开信息面板，成功读取文件名、相机、镜头、拍摄时间及评分状态。恢复网格并收起信息与来源面板。未修改真实照片元数据或原片。

本轮没有部署 Rust 服务端契约调整；运行验证使用现有部署，新的解码模型忽略其额外响应字段。去除旧字段后的服务端响应由本地 HTTP 集成测试验证。未新增上传、逻辑相册或 OpenAPI 客户端生成器。

## 后续生产部署（2026-09-26）

已部署 `keeps-server:unified-http-20260926`，运行镜像 SHA 为 `e527e706e61cf29bb2fa5260656713f7cd90d9b3830a51265af1115e71467b08`；容器健康状态 healthy。线上源码比对仅 navigation.rs 不同，无数据库迁移，Compose 无差异。

回退镜像为 `keeps-server:before-unified-http-20260926`（原 SHA `43557dfc28e7a76c2ade5cbfda52c4767199edb94c215e1d604e4506d9ed8cd6`）。升级前两个数据库已通过 SQLite backup API 备份，integrity_check 均为 ok：

- `/volume2/myphoto/keeps/backups/before-unified-http-20260926-154601-control_plane.sqlite`
- `/volume2/myphoto/keeps/backups/before-unified-http-20260926-154601-jobs.sqlite`

线上验证：导航只含 path/directories；未认证业务请求返回 401；照片总数 117,344，示例查询返回 10 条；预览 HTTP 200、600,194 字节，SHA-256 为 `e647499b9313a0c50c5d4a8ecc87b429b5ab5564b722cc1160e77a0be1206c4f`。四个原片挂载的 RW 均为 false。

扫描任务 `34d27418-7f2c-4434-9144-d8d1f063280c` 重建后仍为 running，恢复后开始跳过已处理文件；当前尝试的计数重新开始，不表示扫描已完成。Mac 客户端刷新图库及来源成功，显示 myphoto 与 photo。未修改真实照片或评分标签。

远端完整结果位于 `/volume2/docker/keeps/unified-http-before.json`、`unified-http-after.json`；构建日志为 `unified-http-build.log`。
