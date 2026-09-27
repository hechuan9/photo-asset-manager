# 目录读取状态与数据库计数

目录行复用原生 NSProgressIndicator：读取子目录或当前目录图库时替换文件夹图标，完成后停止并隐藏；cell 复用时显式清除旧状态。首次图库读取显示居中“正在读取目录内容…”，底栏不以 0/0 冒充空结果，错误保留错误与重试。

navigation.directory.photoCount 为必需 Int，NAS 通过 catalog_paths 与 catalog_assets 的路径索引范围读取，后代资产去重、排除回收站、资料库隔离；不受客户端过滤影响。不新增数据库schema，不枚举原片统计。目录筛选同步使用区分大小写的路径范围，避免 Foo/foo 计数不一致。

服务端40项通过、1项依赖Linux媒体运行时的测试按标记跳过。测试覆盖中文/特殊字符、前缀边界、多路径资产、回收站、空目录、计数与图库total一致。shared8/macOS16项通过，含延迟响应读取状态和cell复用spinner清理。

现场查询验证发现相关 EXISTS 在大目录会对每个资产重复扫描路径范围；改为非相关 IN 集合查询，并追加 EXPLAIN 回归测试，确保 LIST SUBQUERY、无 CORRELATED。NAS 实测 photo/照片 的 29,560 项计数查询约 0.245 秒。

已部署 keeps-server:directory-counts-20260926（最终修正镜像43557dfc28e7），Mac更新已安装。现场API逐目录核对photoCount与gallery.total全部一致：验证结果：`directory-counts-live-20260926.json`（本地现场记录，未入库）。初次根导航约1226ms、2005目录22ms、photo/照片277ms，仅为本次现场样本。

应用现场可见目录旋转指示和“正在读取目录内容…”；完成后spinner消失，目录显示照片数。2005下音乐会2、广州30、大连65；选中2005广州，图库显示30/30。原始文件未改动。
