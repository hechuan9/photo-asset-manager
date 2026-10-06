# Synology 原生套件

Keeps 在 NAS 原生运行，Docker 只用于构建。当前支持 DS1520+ 的 geminilake 架构及 DSM 7；尚未取得 Synology 官方套件审核。

## 构建与安装

```sh
docker build -f server/Dockerfile -t keeps-server:spk .
python3 deploy/spk/export_runtime.py --image keeps-server:spk --output /tmp/keeps-native-payload
python3 deploy/spk/pack.py --payload /tmp/keeps-native-payload --output /tmp/KeepsNativeProbe.spk
python3 -m unittest discover -s deploy/spk -p test_pack.py
```

在 NAS 管理员终端使用 `synopkg install <绝对路径.spk>` 安装，`synopkg start KeepsNativeProbe` / `synopkg stop KeepsNativeProbe` 启停。套件以 DSM 创建的专用账号运行，CPU 亲和性为 0–1；没有整个套件的 2 GiB 硬内存上限，媒体子进程另有地址空间和超时限制。

运行时使用私有动态加载器，不全局设置 `LD_LIBRARY_PATH`。导出器针对 Debian trixie、Perl 5.40、ImageMagick 7 的布局；随包保留依赖版权资料。

## 配置

配置位于 `/var/packages/KeepsNativeProbe/var/`，由套件账号持有，权限 0600；升级保留已有配置。

- `original-root`：原片目录的绝对路径。
- `server-url`：客户端可达的服务地址，也用于签名预览链接。
- `access-token`：随机访问令牌，不提交 Git。
- `server-overrides`：启动脚本白名单内的逐行 `KEY=value`，不执行 shell 文本。

正式资料库为 `local-library`，局域网地址 `http://192.168.0.50:2283`，原片 `/volume2/photo`，状态 `/volume2/@appdata/KeepsNativeProbe/production-state`。新安装的原型默认使用独立测试共享目录、`spk-probe` 和 2285，不能将其当作正式配置。

日常本地编码开启（`KEEPS_LOCAL_CACHE_ENCODING_ENABLED=1`），远端派发关闭（`KEEPS_REMOTE_WORKER_ENABLED=0`）。Linux 仅用于显式安排的[一次性批量处理](../linux-worker/README.md)。

## 升级与恢复

1. 暂停独立维护进程，停止服务；使用 SQLite backup API 一致备份状态目录的 `db/control_plane.sqlite`、`db/jobs.sqlite`，检查备份，并保留当前套件与配置。
2. 安装新包，保留 library ID、token、原片和状态路径。禁止同时启动两个写入同一状态目录的服务。
3. 启动后核对 `/healthz`、鉴权、资产计数、缩略图下载及后台状态，再恢复维护进程。可用性与全库处理完成是两回事。
4. 回退前另存最新数据库。使用与数据库 schema 匹配的套件和配置；恢复旧数据库会丢失备份之后的整理修改，不能直接覆盖最新状态。

NAS 历次安装包与数据库备份位于 `/volume2/docker/keeps/releases/`。旧 Docker 服务 `keeps-control-plane` 保持停止；它保存的是迁移时点的旧状态，不能直接启动代替当前服务。原片始终保留原位，不参与套件清理。
