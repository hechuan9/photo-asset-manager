# 图库 ID 校验修复

## 问题与原因

手机使用 hechuan 作为图库 ID，请求 /libraries/hechuan/revision 得到 HTTP 200。未知图库查询被当作空库；iOS 的连接验证因此通过，随后显示没有照片。真实图库 local-library 的现场查询返回 121443 张照片。

## 修改

AppState 保存服务配置的 KEEPS_LIBRARY_ID（未设置时使用 local-library）。Bearer 鉴权成功后，所有 /libraries/{library} 路由统一校验图库 ID；未知值返回 HTTP 404，错误码 library_not_found，中文错误提示明确该字段不是 NAS 用户名。预览元数据中的显式 libraryID 也校验。合法空库仍返回 200。现有 iOS 验证并保存先请求 counts/assets，错误响应阻止连接保存。

## 验证

- cargo test：59 通过，1 个已有测试忽略。
- 最终 HTTP 回归：11 通过。
- cargo clippy --all-targets -- -D warnings：通过。
- NAS 构建前已比对远端 api.rs/main.rs，差异仅为本次校验修复；保留旧镜像 keeps-server:before-library-validation-20260928。
- 不修改数据库 schema，不触碰原片。

## 部署

2026-09-28 12:05 UTC 线上验收通过：hechuan 的 counts/assets/revision 均返回 404 / library_not_found；local-library 的三个接口均返回 200，照片总数 121447。镜像 sha256:a22011663b0d877d562ecae286746a44727a7abebcc4db62ae21d96e7f921a01。现场证据见同名 JSON。部署源码与最终本地源码仅有 Clippy 建议的等价 if-let 合并格式差异。
