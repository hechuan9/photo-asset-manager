# 视频 Range 缓存生产验证（2026-10-05）

最终 2026-10-05T14:25:46Z：定点 7 视频与 8 JPEG 内容 ARW 全部 ready，last_error=null；8 原片 SHA256 均与部署前一致。

视频源合计 50,512,979,819 字节，Linux 实际读取 81,526,784 字节（0.1614%，77.75 MiB），仅抓首帧生成 HEIC。源完整 SHA256 在 NAS 领取及发布阶段仍校验，未降低校验语义。HTTP Range 走原认证 HTTPS，token 不进入 ffmpeg 参数；首帧本机代理仅绑定 127.0.0.1。

真实故障修复：并发 claim 清理过期任务目录的竞态导致 ENOENT，现与发布使用同一锁；Synology Keeps HTTPS 反代 60 秒读超时令大型视频完整 SHA 校验返回 504，仅该 route read/send 调整为 1800 秒，connect 60 秒不变；ARW 扩展但 JPEG 内容须显式 JPEG coder。

最终维护：Linux 排空 14:15:54Z，NAS 停 14:16:45Z、健康 14:17:31Z，Linux 恢复 14:17:51Z。首轮测试维护为 14:03:17Z 至 14:07:15Z，跨两窗口不得估算吞吐。

最终 NAS image config：sha256:109cec315137604c802b53e75c385a321b11218925223e97b49cedd96183c6f0；Linux image manifest：sha256:61fef3786c5b64b9192d897806fa9a1b57a3c0ccf98af31294269d1d872826e0。不同 Docker 版本展示 config / manifest，来自同一 save/load 镜像归档。

构建基于生产 identity-20260929/source.tar.gz，仅加入 remote_worker.rs、linux_cache_worker.py、media.rs、cache_pipeline.rs 四文件差异，未包含工作区导入功能修改。Rust remote worker 9 测试、Python 5 测试通过。

14:26:17Z Linux：CPU 0%，29.75 MiB / 24 GiB，restart=0，OOM=false，memory.events 所有值 0，新增错误 0；根盘可用 441 GiB。16 并发、16 CPU、128 GiB 临时预算、每进程 4 GiB 地址空间保持。NAS 2 核 / 2 GiB 与 localEncodingEnabled=false 保持。

两容器 restart policy 均 unless-stopped；Linux Docker 服务 enabled。worker 主动连接 https://keeps.hechuannas.synology.me:8443，只挂载 Linux 本机工作目录及 token 文件，无 Mac 挂载或 SSH 隧道依赖。

NAS 证据：/volume2/docker/keeps/releases/video-range-20261005/deployment-final.json、retry-verification.json、proxy-timeout.json、source-final.tar.gz、image-final.tar.gz；配置、双库维护前备份留在同目录。
Linux 证据：/home/hechuan/keeps-worker/releases/video-range-20261005/worker-deployment-final.json、final-worker-verification.json、drain-final-start.txt、resume-final.txt。
