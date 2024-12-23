#!/bin/bash

# 名稱: manage_virtual_serial.sh
# 功能: 用 tmux 管理虛擬串口對的創建與關閉
# 用法: 
#   ./manage_virtual_serial.sh start
#   ./manage_virtual_serial.sh stop

ACTION=$1
SESSION_NAME="ho_emu_tmux_sess"
PORT1="/tmp/ttyV0"
PORT2="/tmp/ttyV1"

# 檢查 tmux 是否存在
if ! command -v tmux &>/dev/null; then
    echo "Error: tmux is not installed. Please install it first."
    exit 1
fi

# 檢查 socat 是否存在
if ! command -v socat &>/dev/null; then
    echo "Error: socat is not installed. Please install it first."
    exit 1
fi

# 啟動虛擬串口
start_session() {
    if [ -z "$SESSION_NAME" ] || [ -z "$PORT1" ] || [ -z "$PORT2" ]; then
        echo "Usage: $0 start <session_name> <port_name_1> <port_name_2>"
        exit 1
    fi

    # 檢查是否已有相同會話
    if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        echo "Error: tmux session '$SESSION_NAME' already exists."
        exit 1
    fi

    # 創建 tmux 會話並啟動 socat
    tmux new-session -d -s "$SESSION_NAME" "socat -d -d pty,raw,echo=0,mode=777,link=$PORT1 pty,raw,echo=0,mode=777,link=$PORT2"
    echo "Virtual serial ports created: $PORT1 <-> $PORT2 in tmux session '$SESSION_NAME'."
}

# 關閉虛擬串口
stop_session() {
    if [ -z "$SESSION_NAME" ]; then
        echo "Usage: $0 stop <session_name>"
        exit 1
    fi

    # 檢查是否存在會話
    if ! tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        echo "Error: tmux session '$SESSION_NAME' does not exist."
        exit 1
    fi

    # 終止 tmux 會話
    tmux kill-session -t "$SESSION_NAME"
    echo "Tmux session '$SESSION_NAME' stopped and virtual serial ports closed."
}

# 主流程
case "$ACTION" in
    start)
        start_session
        ;;
    stop)
        stop_session
        ;;
    *)
        echo "Usage: $0 <start|stop> <session_name> [port_name_1] [port_name_2]"
        exit 1
        ;;
esac