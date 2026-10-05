# 2026-10-05 Synology 原生 SPK 可行性验证

结论：当前 DS1520+ 可以原生运行 Keeps，并通过 DSM 套件生命周期完成安装、升级、启动和停止。此轮是隔离原型，不是生产迁移。原生用户服务的硬内存隔离尚未解决。

## 实测环境与交付

- DS1520+，geminilake / x86_64，Celeron J4125，DSM 7.3.2-86009 Update 4。
- DSM glibc 2.36，systemd 219，cgroups v1。
- 包：`KeepsNativeProbe-0.1.0-0004.spk`，125173760 bytes（约 119.4 MiB）。
- SHA256：`79929e88d774274667285058e8207cc8f844589dfb97077de6f88d511c78c26e`。
- 本地安装包：`.build/spk/KeepsNativeProbe-0.1.0-0004.spk`。
- NAS 包及证据：`/volume2/docker/keeps/releases/spk-probe-20261005/`，包括 `native-tools.json`、`service-evidence.json`、`final-verification.json`。
- 构建源为现有已验证镜像 `keeps-server:import-20261005`；容器只用于导出构建产物。运行时由套件自带私有加载器、媒体工具和共享库组成，约 330 MB 未压缩。

## 通过的验证

| 检查 | 实测结果 |
|---|---|
| 原生运行 | Keeps 进程与 PID 1 的 mount namespace 相同，无容器 |
| 套件账号 | UID/GID 164747，KeepsNativeProbe，非 root |
| 生命周期 | synopkg 安装、0003→0004 升级、启动、停止均 success=true |
| CPU 亲和性 | `/proc/<pid>/status` 的 `Cpus_allowed_list=0-1` |
| 媒体工具 | ExifTool 13.25、heif-convert 1.23.4、FFmpeg 7.1.5 启动成功；ImageMagick HEIC rw+ |
| HEIF 处理 | 原生 keeps-render 解码、缩放、编码通过 |
| 导入 | 两个源子目录同名 HEIC 和关联 XMP，目的地平铺且没有覆盖；错误 hash 拒绝、上传暂存、恢复和 finish 幂等通过 |
| 索引 | job completed，2 个资产 |
| 缩略图 | 两个缓存状态 ready，均 512×512、132536 bytes，无 last_error，文件存在 |
| 升级后状态 | 2 个资产与缩略图保留，healthz=ok |
| 数据安全 | 使用生成的 1200×1200 HEIC 样本；源 SHA256 不变；未操作用户原片 |
| 原生产服务 | keeps-control-plane 健康，PID 28213、StartedAt 2026-10-05T15:52:41Z 未改变 |
| 本地验证 | 3 个打包/令牌保留/生命周期测试通过，Python 编译检查通过 |

测试套件最终处于 stopped，保留状态和生成样本，未卸载。没有迁移生产数据库、共享目录、客户端连接或反向代理。

## 发现与修正

1. libheif 插件实际位于 `libheif/plugins`。只指向父目录会报 HEVC 无解码插件；修正后 HEIC 读写通过。
2. 普通套件账号不能控制系统级 systemd 服务。尝试仅为启停脚本配置 root 时，本机 DSM 拒绝安装（319 invalid package privilege content）。最终使用官方 `systemd-user-unit` 资源、`pkguser-*` 用户服务，所有生命周期脚本保持 package 权限。
3. 升级要求同时提供 preupgrade/postupgrade，缺失时 DSM 返回 261。原型数据格式未变，钩子不修改持久数据。
4. 用户服务中的 `MemoryLimit=2G` 不生效：实际 memory cgroup 仍为 `/user.slice`，`memory.limit_in_bytes=9223372036854771712`。最终移除无效声明。CPU 亲和性有效，但不等于 CPU 百分比限额。
5. 服务出现在用户 systemd 的 `KeepsNativeProbe.slice` 下；synomonitor 层级只到用户服务，尚未验证 DSM 图形资源监控按套件聚合。

## 生产化前仍需验证

- 共享目录 ACL、配置界面、持久目录与生产迁移/回退方案。
- 真实 RAW 样本及完整 RAW 派生链路；本轮只验证 HEIF 图像链路，FFmpeg 仅验证启动。
- NAS 重启、自启动、故障恢复、并发压力和其他 CPU/DSM 版本。
- 硬内存限制与 DSM 资源监控归属；不能因为使用 SPK 就宣称优于 Docker。
- 媒体运行时裁剪、依赖许可证与商业再分发条件、签名及套件分发审批。

构建方式和官方参考见 `deploy/spk/README.md`。原型不是 Synology 官方认证套件。

## 第二轮：共享文件夹与持久配置（0005）

用户确认继续后，在同一台 NAS 上完成以下验证，仍未迁移生产服务：

- 包版本 `0.1.0-0005`，125173760 bytes；SHA256 `37051777136354d25b02be95838ab6c11cd7ab4a2480a066ed6e95eecdef4a7b`。
- 安装前确认测试共享目录不存在。通过 `conf/resource` 的 `data-share` 声明，DSM 自动创建 `/volume2/keeps-native-probe` 和套件下 `shares/keeps-native-probe` 链接。
- `synoacltool -get` 确认存在 `user:KeepsNativeProbe:allow:rwxpdDaARWc--:fd--`，无需手工 chmod/chown/ACL 操作。没有申请现有照片共享目录的权限。
- 原片目录配置保存于 `var/original-root`，默认值是上述链接。启动要求绝对路径以及目录读写/遍历权限。该文件按普通文本读取，不作为 shell 执行。目前没有图形安装向导。
- 第一次导入返回 422 `target must be inside an active tracked folder`。通过既有 folders API 启用该目录追踪后导入成功；目录权限不会自动代表用户已启用图库追踪。
- 导入结果为 `photo.heic`、`photo (1).heic`、`photo.xmp`，均直接位于共享目录。DSM 自动生成 `@eaDir`；初版验证脚本“任何子目录都不允许”的断言误报，核验时区分 DSM 元数据与导入文件，不删除 `@eaDir`。
- 共享目录扫描完成，processed=2、failed=0；加上此前保留的两条资产，共 4 个资产、4 个 ready 缩略图，均无缓存错误。生成的源样本 SHA256 不变。
- 将配置值改为共享目录的实际绝对路径，再通过 synopkg 重装同版本（DSM 报告 upgrade）并启动：配置、访问令牌和 3 个共享文件的字节摘要全部保留。
- 本地 4 个测试通过：包结构与权限、配置和令牌保留、非法/缺失目录启动拒绝及含空格有效目录、生命周期错误传播。shell 语法和包内配置/启动脚本与源码一致性检查通过。
- 最后已停止测试套件；生产 `keeps-control-plane` 为 healthy。测试共享目录、旧测试原片和状态都保留。

NAS 证据位于原验证目录下的 `share-install.json`、`share-runtime.json`、`share-reinstall.json`、`share-final.json`。本地包位于 `.build/spk/KeepsNativeProbe-0.1.0-0005.spk`。

资源管理没有新增未经证实的声明：当前媒体流程已有超时、单线程、并发 1 和默认 1.5 GiB 虚拟地址空间限制；它们不是整套件物理内存硬上限。官方文档要求 root 套件使用 Synology 签名，合作伙伴可申请开发 token，故正式系统级资源隔离仍需合作渠道验证；本轮未引入提权 helper。参考 [官方开发要求](https://help.synology.com/developer-guide/getting_started/system_requirement.html) 与 [Data Share](https://help.synology.com/developer-guide/resource_acquisition/data_share.html)。

## 第三轮：Mac 开发环境可用（0006）

按用户要求收敛到开发环境，不推进商业分发或复杂资源隔离。

- 原生套件保持运行：`http://192.168.0.50:2285`，资料库 `spk-probe`，原片根目录 `/volume2/keeps-native-probe`。监听 `0.0.0.0:2285`，仍要求 Bearer 鉴权。
- `var/server-url` 提供预览链接的局域网基址。首次升级时，操作脚本以 root 预写此文件，postinst 的 chmod 被拒绝；核对日志后将该配置文件属主改回套件账号，repair 和启动成功。没有改变用户照片权限。
- Mac 现有 Keychain 凭据保持不变，开发套件使用同一客户端凭据；旧原型令牌保留于 NAS `var/access-token-before-mac`（0600），没有在输出或仓库中记录令牌。Mac 旧地址及资料库保存在 `.build/spk/mac-connection-before.json`。
- 直接编译当前共享 Swift 客户端代码，在 Mac 经局域网上传生成的 `mac-client.heic`，完成索引、列表查询、评分/标签修改及恢复；随后下载缩略图并通过 ImageIO 解码，源样本不变。第一次在缓存生成前请求预览返回 404，后台生成后成功。
- GUI 验证发现导入面板无法选择没有子目录的原片根目录。修复 ImportView：首次从 folders API 取得根目录，作为默认目标，并禁用根目录的返回按钮。
- Mac 导入相关 7 个测试通过，SPK 4 个测试通过。通过项目现有脚本打包、签名验证后更新 `/Applications/Keeps.app`；旧应用备份位于 `~/Library/Application Support/Keeps/AppBackups/Keeps-before-spk-dev-20261005.app`。
- 在已安装 Mac 应用的实际导入界面选择 `.build/spk/mac-import-sample`，目标显示 `/volume2/keeps-native-probe`，点击开始后显示“已导入 1 个文件”，1.7 MB 上传完成。图库随后显示 6 张测试照片。
- 正式 Docker 服务保持 healthy；没有修改正式图库、反向代理或外网路由。

0006 安装包：`.build/spk/KeepsNativeProbe-0.1.0-0006.spk`，SHA256 `d7acde0efba1a3b54c330baf0c09b03c271005495faf49fcb97a78b9858947a1`。本轮保持套件与 Mac 运行，便于直接继续开发。

最终读回：6 个缓存全部 ready，Docker 生产健康；NAS 证据 `mac-dev-ready.json`。
