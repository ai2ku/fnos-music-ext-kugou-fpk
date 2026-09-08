# 飞牛音乐酷狗扩展 FPK

把 fnos_music_ext 改造成飞牛原生 FPK 应用包，一键安装、配置向导、零命令。

## 特点

- **一键安装**：飞牛应用中心 → 手动安装 → 上传 .fpk
- **图形化配置向导**：填 KuGouMusicApi 地址、选音质，无需碰命令
- **卸载即还原**：卸载时自动切回官方 socket，不留残留
- **零侵入**：不修改飞牛官方 nginx、二进制或数据库
- **升级无痛**：新版本 FPK 直接覆盖安装，保留 venv 与缓存

## 目录结构

```
fnmusic-ext-kugou-fpk/
├── build-fpk.sh                      # 打包脚本（本地跑）
├── README.md                         # 本文件
└── fnmusic-ext-kugou.fpk/            # FPK 源码目录
    ├── manifest                      # 应用元信息
    ├── ICON.png                      # 64x64 图标
    ├── ICON192.png                   # 192x192 图标
    ├── ICON256.png                   # 256x256 图标
    ├── app/                          # → 安装到 target/（TRIM_APPDEST）
    │   └── proxy/
    │       ├── app.py                # 已改好的代理（Kugou 唯一源）
    │       ├── kugou_source.py       # 酷狗源适配
    │       ├── recommend.py          # 每日推荐
    │       ├── run_proxy.sh          # 启动脚本
    │       └── requirements.txt      # Python 依赖
    ├── cmd/                          # 生命周期钩子
    │   ├── install_init.sh           # 预检（飞牛音乐是否启动）
    │   ├── install_callback.sh       # 装依赖、写 .env
    │   ├── start_init.sh
    │   ├── start_callback.sh         # 接管 socket、启动 systemd
    │   ├── stop_callback.sh
    │   ├── upgrade_callback.sh       # 升级时重启服务
    │   ├── uninstall_init.sh
    │   └── uninstall_callback.sh     # 卸载：还原官方 socket
    ├── config/
    │   ├── privilege.json            # 声明 root 权限
    │   └── resource.json             # 无额外共享
    └── wizard/
        ├── wizard.json               # 向导定义
        └── ui/index.html             # 配置页面
```

## 打包

```bash
# 1. 下载 fnpack（如未安装）
# 从 https://www.fnnas.com/download/fnpack 下载对应平台的 fnpack
# 放到本目录（build-fpk.sh 所在目录），或设置环境变量 FNPACK_BIN

# 2. 执行打包
./build-fpk.sh

# 3. 产物在 dist/fnmusic_ext_kugou-1.0.0.fpk
```

## 安装

1. 打开飞牛应用中心
2. 点击右上角「手动安装」
3. 上传 `dist/fnmusic_ext_kugou-1.0.0.fpk`
4. 系统会弹窗要求 root 权限授权 → 输入密码
5. 进入配置向导 → 填 KuGouMusicApi 地址 → 保存并启动
6. 打开飞牛音乐，搜索即可

## 前置条件

- **飞牛音乐应用（`trim_music`）已安装** — 已通过 `install_dep_apps="trim_music"` 在 manifest 中声明，应用中心会自动处理依赖
- **KugouMusicApi 服务可访问**（本机容器用 http://127.0.0.1:8899，远程用对应 IP:端口）
- **Python 3 与 python3-venv 已安装**（飞牛镜像一般自带）

## 运行时布局

| 路径 | 用途 |
|---|---|
| `/opt/target/fnmusic_ext_kugou/` | 应用代码（`TRIM_APPDEST`） |
| `/opt/target/fnmusic_ext_kugou/.venv-proxy/` | Python 虚拟环境 |
| `/opt/target/fnmusic_ext_kugou/.env` | 用户配置（向导写入，chmod 600） |
| `/opt/target/fnmusic_ext_kugou/cache/` | 音频 / 歌词缓存 |
| `/opt/target/fnmusic_ext_kugou/online_favorites/` | 在线收藏 |
| `/opt/target/fnmusic_ext_kugou/log/` | 应用日志 |
| `/var/run/trim_music.socket` | 代理 socket（被本应用接管） |
| `/var/run/trim_music_upstream.socket` | 官方 socket（保留备份） |
| `/etc/systemd/system/fnmusic-ext.service` | systemd 服务 |

## 卸载

飞牛应用中心 → 已安装 → 飞牛音乐酷狗扩展 → 卸载

`uninstall_callback.sh` 会：
1. 停止并禁用 `fnmusic-ext.service`
2. 删除 systemd unit
3. 把 `/var/run/trim_music_upstream.socket` 移回 `/var/run/trim_music.socket`

卸载后飞牛音乐自动切回官方模式。

## 升级

- 新版本 FPK 覆盖安装
- `upgrade_callback.sh` 语法检查新代码 → 重启 systemd → 健康检查
- 保留 `.venv-proxy`、`.env`、`cache/`（用户数据不丢）

## 常见问题

### Q: 安装时提示"未检测到飞牛音乐应用"
先安装并启动飞牛官方音乐应用，再来装本扩展。

### Q: 启动后飞牛音乐搜索不到歌
```bash
curl -s --unix-socket /var/run/trim_music.socket http://localhost/_ext/healthz
# 应返回 {"ok":true,"upstream":"ok","kugou":"ok",...}
```
如果 `kugou` 是 `fail`，检查 KuGouMusicApi 是否可访问。

### Q: 音质只有 64kbps
向导里选的音质档可能没通过 KuGouMusicApi 授权。Kugou VIP 账号才能拿 320kbps。

### Q: 日志在哪看
```bash
sudo journalctl -u fnmusic-ext -f     # 实时日志
tail -f /opt/target/fnmusic_ext_kugou/log/*.log
```

### Q: 想彻底清理（含用户数据）
```bash
# 先在飞牛应用中心卸载
# 再清理残留目录（会删掉缓存和配置）
sudo rm -rf /opt/target/fnmusic_ext_kugou /opt/var/fnmusic_ext_kugou
```

## License

MIT（继承自 fnos_music_ext 上游）
