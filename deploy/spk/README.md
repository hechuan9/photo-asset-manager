# Synology 原生套件可行性原型

当前 NAS 已在 0012 套件上运行正式资料库 `local-library`，Mac 地址为 `http://192.168.0.50:2283`，原片仍位于 `/volume2/photo`，状态位于 `/volume2/@appdata/KeepsNativeProbe/production-state`。旧 Docker 已停止并保留。见[正式迁移记录](../../docs/validation/2026-10-05-spk-production-migration.md)。0010 更新及验证见[任务展示部署记录](../../docs/validation/2026-10-05-task-settings-deployment.md)。0012 增量调度与故障隔离见[增量后台验证](../../docs/validation/2026-10-05-incremental-work.md)。下文的 2285、spk-probe 和测试共享目录是原型默认配置。

`KeepsNativeProbe` 是独立测试套件，已在 DS1520+（geminilake、DSM 7.3.2-86009 Update 4）验证。它原生运行 Keeps 和媒体工具，不需要运行容器。当前只允许该架构安装，不代表其他 DSM/CPU 已兼容。

## 构建

从已经验证的 Linux amd64 镜像提取程序及依赖；Docker 仅用于构建。当前导出器针对 Debian trixie / Perl 5.40 / ImageMagick 7 的已知布局，不是通用 Linux 打包器。构建机器需要至少两个逻辑 CPU。

```sh
python3 deploy/spk/export_runtime.py --image keeps-server:import-20261005 --output /tmp/keeps-native-payload
python3 deploy/spk/pack.py --payload /tmp/keeps-native-payload --output /tmp/KeepsNativeProbe.spk
python3 -m unittest discover -s deploy/spk -p test_pack.py -v
```

程序通过私有动态加载器使用随包附带的共享库。不能全局设置 `LD_LIBRARY_PATH`，否则可能影响 DSM 的 shell 和系统工具。libheif 的插件目录必须指向 `libheif/plugins`。保留了运行时 `/usr/share/doc` 中的版权资料；这不等于已完成商业再分发许可审查。

## 安装与运行边界

通过 DSM 手动安装，或管理员执行 `synopkg install <绝对路径.spk>`。用 `synopkg start KeepsNativeProbe` / `synopkg stop KeepsNativeProbe` 启停。

- DSM 创建独立套件账号，`conf/resource` 注册用户级 systemd 服务；所有生命周期脚本均以套件账号运行。
- 0006 起监听 `0.0.0.0:2285`，供局域网 Mac 开发使用，保留 Bearer 鉴权。当前 NAS 地址 `http://192.168.0.50:2285`，资料库 `spk-probe`。
- `var/server-url` 保存客户端可访问的服务地址，用于返回预览链接；首次安装默认 localhost，在当前 NAS 已设为上述局域网地址。修改时保留文件属主 KeepsNativeProbe。
- 随机访问令牌位于 `/var/packages/KeepsNativeProbe/var/access-token`，权限 0600。
- 状态放 `var/state`；0005 起，DSM 的 `data-share` 创建 `keeps-native-probe` 测试共享目录并授予套件账号读写权限。旧 `var/sample-originals` 原样保留，严禁把用户图库移入测试套件。
- `var/original-root` 保存原片目录的绝对路径，默认指向 DSM 生成的 `shares/keeps-native-probe` 链接。安装仅在配置缺失时创建，升级保留已有值；启动检查目录存在及读写/遍历权限，不执行配置中的 shell 文本。当前是文件配置，还没有图形安装向导。
- 正式图库、生产数据库、生产 Docker 容器和反向代理均不迁移。
- CPU 亲和性限制到 CPU 0、1；这不是 CPU 百分比配额。
- 用户级服务的 `MemoryLimit=2G` 实测没有成为内核内存限制，已从最终配置删除。不要声称此包有 2 GiB 硬内存上限。
- `Slice=KeepsNativeProbe.slice` 出现在用户 systemd 层级；尚未在 DSM 图形资源监控器确认按套件聚合显示。

## 已验证与未覆盖

安装、升级、启停、普通账号运行、HEIF 导入/重名处理/平铺/XMP、索引和两张 512×512 HEIC 缩略图均通过。升级后两条资产和缩略图仍在，测试源文件 SHA256 不变。0006 开发环境保持运行，Mac 已连接，保留测试数据。完整证据见 `docs/validation/2026-10-05-spk-feasibility.md`。

独立测试共享目录 ACL 与文件配置已验证通过；下一阶段需要完善图形安装配置、正式状态目录和迁移策略，补测真实 RAW、多机型、重启与故障恢复，并完成媒体依赖裁剪、许可证与分发流程。当前包不是正式发行版，也未取得 Synology 官方审核。

## 官方参考

- [DSM 用户服务及资源注册](https://help.synology.com/developer-guide/resource_acquisition/systemd_user_unit.html)
- [套件权限](https://help.synology.com/developer-guide/privilege/privilege_config.html)
- [持久目录](https://help.synology.com/developer-guide/integrate_dsm/fhs.html)
- [资源监控集成](https://help.synology.com/developer-guide/integrate_dsm/resource_monitor.html)

## 目录授权与资源限制的后续验证

`data-share` 是 DSM 官方的共享目录创建/授权机制，声明 `once=true` 避免每次启动重复授权。只对独立测试共享目录申请权限，不给现有 photo 共享目录增加权限。参见 [Data Share](https://help.synology.com/developer-guide/resource_acquisition/data_share.html)。

媒体处理沿用现有 `prlimit --as`、超时、单线程与缓存并发 1；地址空间限制不等于物理内存限制，也不是整个套件的总量限制。官方要求 root 套件由 Synology 签名，合作伙伴可以申请开发 token。商业化的系统级服务/资源隔离需沿正式途径验证，当前不引入 root helper：[官方开发要求](https://help.synology.com/developer-guide/getting_started/system_requirement.html)。

## 正式图库配置

沿用同一套件，通过 `var/server-overrides` 的逐行 `KEY=value` 指定状态根目录、资料库 ID、监听地址和现有后台处理开关。只接受启动脚本列出的设置，不执行 shell 文本。`original-root`、`server-url` 和 `access-token` 仍分别保存原片目录、客户端服务地址和凭据。配置文件应由套件账号持有，权限 0600。

切换时必须保留正式 library ID、token、缩略图质量和本地编码开关；停止旧服务后复制状态，原片目录保持原位。不要同时启动两个写同一状态目录的服务。旧 Docker 容器和旧状态目录保留用于回退。
