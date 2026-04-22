#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RUNTIME_DIR="$HOME/marquee-runtime"
# Allow CARGO_TARGET_DIR (e.g. a shared cache across worktrees) to redirect
# where the release binary lives. Defaults to the in-tree `target/`.
TARGET_DIR="${CARGO_TARGET_DIR:-target}"

cd "$SCRIPT_DIR"

echo "=== Step 1: 编译前端 ==="
cd web && (command -v pnpm >/dev/null && pnpm run build || npm run build)
cd "$SCRIPT_DIR"

echo ""
echo "=== Step 2: 验证前端产物 ==="
HTML_JS=$(grep -o 'src="/assets/[^"]*"' web/dist/index.html | sed 's|src="/assets/||;s|"||')
HTML_CSS=$(grep -o 'href="/assets/[^"]*"' web/dist/index.html | sed 's|href="/assets/||;s|"||')
if [ ! -f "web/dist/assets/$HTML_JS" ] || [ ! -f "web/dist/assets/$HTML_CSS" ]; then
    echo "前端产物不匹配，清除后重新构建..."
    rm -rf web/dist
    cd web && (command -v pnpm >/dev/null && pnpm run build || npm run build)
    cd "$SCRIPT_DIR"
    HTML_JS=$(grep -o 'src="/assets/[^"]*"' web/dist/index.html | sed 's|src="/assets/||;s|"||')
    HTML_CSS=$(grep -o 'href="/assets/[^"]*"' web/dist/index.html | sed 's|href="/assets/||;s|"||')
fi
echo "前端产物: $HTML_JS, $HTML_CSS ✓"

echo ""
echo "=== Step 3: 编译后端 (release) ==="
cargo build --release

echo ""
echo "=== Step 4: 停止旧进程 ==="
OLD_PID=$(ps aux | grep '[.]\/marquee' | grep -v grep | awk '{print $2}' | head -1)
if [ -n "$OLD_PID" ]; then
    echo "杀掉旧进程 PID=$OLD_PID"
    kill "$OLD_PID" 2>/dev/null || true
    sleep 1
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "警告: 进程未退出，强制 kill -9"
        kill -9 "$OLD_PID" 2>/dev/null || true
        sleep 1
    fi
    echo "旧进程已停止 ✓"
else
    echo "没有运行中的旧进程"
fi

echo ""
echo "=== Step 5: 部署二进制 ==="
cp "$TARGET_DIR/release/marquee" "$RUNTIME_DIR/"
codesign --sign - --force "$RUNTIME_DIR/marquee" 2>/dev/null
echo "二进制已复制并签名 ($(du -h "$RUNTIME_DIR/marquee" | awk '{print $1}')) ✓"

echo ""
echo "=== Step 6: 启动新进程 ==="
cd "$RUNTIME_DIR"
nohup ./marquee > marquee.log 2>&1 &
NEW_PID=$!
echo "新进程已启动 PID=$NEW_PID"

echo ""
echo "=== Step 7: 健康检查 ==="
# Embedding 模型加载可能需要 15-20 秒，轮询等待
for i in $(seq 1 10); do
    sleep 3
    STATS=$(curl -s http://localhost:8080/api/movies/stats 2>/dev/null | head -c 80)
    if [ -n "$STATS" ]; then
        echo "API: $STATS ✓ (${i}次尝试, $((i*3))s)"
        break
    fi
    echo "等待服务启动... ($((i*3))s)"
done

if [ -z "$STATS" ]; then
    echo "错误: API 30秒内无响应！"
    tail -10 "$RUNTIME_DIR/marquee.log"
    exit 1
fi

LIVE_JS=$(curl -s http://localhost:8080/ 2>/dev/null | grep -o 'index-[^"]*\.js' | head -1)
if [ "$LIVE_JS" = "$HTML_JS" ]; then
    echo "前端: $LIVE_JS ✓"
else
    echo "警告: 前端不匹配! 期望=$HTML_JS 实际=$LIVE_JS"
    exit 1
fi

echo ""
echo "=== 部署完成 ==="
