#!/usr/bin/env bash
# 打包脚本：调用 fnpack 工具把 fnmusic-ext-kugou.fpk 目录打成 .fpk 安装包
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="${SCRIPT_DIR}/fnmusic-ext-kugou.fpk"
OUT_DIR="${SCRIPT_DIR}/dist"
mkdir -p "${OUT_DIR}"

# 1. 语法检查
echo "==> 语法检查 Python 源码..."
python3 -m py_compile \
    "${PKG_DIR}/app/proxy/app.py" \
    "${PKG_DIR}/app/proxy/kugou_source.py" \
    "${PKG_DIR}/app/proxy/app_socket_bridge.py"
echo "Python 源码语法 OK"

# 2. 脚本语法检查
echo "==> 脚本语法检查..."
for sh in "${PKG_DIR}"/cmd/* "${PKG_DIR}"/app/proxy/run_proxy.sh; do
    [ -f "${sh}" ] || continue
    bash -n "${sh}"
done
echo "Shell 脚本语法 OK"

# 3. JSON 校验
echo "==> JSON 校验..."
for jf in "${PKG_DIR}"/config/*.json "${PKG_DIR}/wizard/wizard.json" "${PKG_DIR}/app/ui/config"; do
    [ -f "${jf}" ] || continue
    python3 -c "import json,sys;json.load(open(sys.argv[1]))" "${jf}"
    echo "  ${jf}"
done

# 4. 定位 fnpack
FPKNAME="fnmusic_ext_kugou-$(sed -n 's/^version[[:space:]]*=[[:space:]]*//p' "${PKG_DIR}/manifest" | tr -d '"' | head -n1)"
FPK_OUT="${OUT_DIR}/${FPKNAME}.fpk"

if [ -n "${FNPACK_BIN:-}" ]; then
    FNPACK="${FNPACK_BIN}"
elif command -v fnpack >/dev/null 2>&1; then
    FNPACK="fnpack"
elif [ -x "${SCRIPT_DIR}/fnpack" ]; then
    FNPACK="${SCRIPT_DIR}/fnpack"
else
    echo "❌ 未找到 fnpack 工具。请从 https://www.fnnas.com/download/fnpack 下载，或设置 FNPACK_BIN=/path/to/fnpack"
    exit 1
fi

echo "==> 使用 fnpack 打包: ${FNPACK}"
"${FNPACK}" build -d "${PKG_DIR}"

# fnpack build 会把产物写在当前工作目录；这里统一搬到 dist/
if [ -f "fnmusic_ext_kugou.fpk" ]; then
    cp -f "fnmusic_ext_kugou.fpk" "${FPK_OUT}"
elif [ -f "${FPK_OUT}" ]; then
    :
else
    echo "❌ 未找到 fnpack 输出文件 fnmusic_ext_kugou.fpk"
    exit 1
fi

echo "✅ 打包完成: ${FPK_OUT}"
ls -la "${FPK_OUT}"
