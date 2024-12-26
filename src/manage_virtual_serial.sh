#!/bin/bash

# 名稱: manage_virtual_serial.sh
# 功能: 用 tmux 管理虛擬串口對的創建與關閉
# 用法: 
#   ./manage_virtual_serial.sh start
#   ./manage_virtual_serial.sh stop

ACTION=$1
RADIO1_SESSION_NAME="ho_emu_tmux_sess1"
PORT1_1="/tmp/ttyV0"
PORT1_2="/tmp/ttyV1"

RADIO2_SESSION_NAME="ho_emu_tmux_sess2"
PORT2_1="/tmp/ttyV2"
PORT2_2="/tmp/ttyV3"

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
    if [ -z "$RADIO1_SESSION_NAME" ] || [ -z "$PORT1_1" ] || [ -z "$PORT1_2" ] || [ -z "$RADIO2_SESSION_NAME" ] || [ -z "$PORT2_1" ] || [ -z "$PORT2_2" ]; then
        echo "Usage: $0 start <RADIO1_SESSION_NAME> [port_name_1_1] [port_name_1_2] <RADIO2_SESSION_NAME> [port_name_2_1] [port_name_2_2]"
        exit 1
    fi

    # 檢查是否已有相同會話
    if tmux has-session -t "$RADIO1_SESSION_NAME" 2>/dev/null; then
        echo "Error: tmux session '$RADIO1_SESSION_NAME' already exists."
        exit 1
    fi

    # 檢查是否已有相同會話
    if tmux has-session -t "$RADIO2_SESSION_NAME" 2>/dev/null; then
        echo "Error: tmux session '$RADIO2_SESSION_NAME' already exists."
        exit 1
    fi

    # 創建 tmux 會話並啟動 socat
    tmux new-session -d -s "$RADIO1_SESSION_NAME" "socat -d -d pty,raw,echo=0,mode=777,link=$PORT1_1 pty,raw,echo=0,mode=777,link=$PORT1_2"
    echo "Virtual serial ports created: $PORT1_1 <-> $PORT1_2 in tmux session '$RADIO1_SESSION_NAME'."

    # 創建 tmux 會話並啟動 socat
    tmux new-session -d -s "$RADIO2_SESSION_NAME" "socat -d -d pty,raw,echo=0,mode=777,link=$PORT2_1 pty,raw,echo=0,mode=777,link=$PORT2_2"
    echo "Virtual serial ports created: $PORT2_1 <-> $PORT2_2 in tmux session '$RADIO2_SESSION_NAME'."
}

# 關閉虛擬串口
stop_session() {
    if [ -z "$RADIO1_SESSION_NAME" ]; then
        echo "Usage: $0 stop <RADIO1_SESSION_NAME>"
        exit 1
    fi

    # 檢查是否存在會話
    if ! tmux has-session -t "$RADIO1_SESSION_NAME" 2>/dev/null; then
        echo "Error: tmux session '$RADIO1_SESSION_NAME' does not exist."
        exit 1
    fi

    if [ -z "$RADIO2_SESSION_NAME" ]; then
        echo "Usage: $0 stop <RADIO2_SESSION_NAME>"
        exit 1
    fi

    # 檢查是否存在會話
    if ! tmux has-session -t "$RADIO2_SESSION_NAME" 2>/dev/null; then
        echo "Error: tmux session '$RADIO2_SESSION_NAME' does not exist."
        exit 1
    fi

    # 終止 tmux 會話
    tmux kill-session -t "$RADIO1_SESSION_NAME"
    echo "Tmux session '$RADIO1_SESSION_NAME' stopped and virtual serial ports closed."

    # 終止 tmux 會話
    tmux kill-session -t "$RADIO2_SESSION_NAME"
    echo "Tmux session '$RADIO2_SESSION_NAME' stopped and virtual serial ports closed."
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
        echo "Usage: $0 <start|stop> <RADIO1_SESSION_NAME> [port_name_1_1] [port_name_1_2] <RADIO2_SESSION_NAME> [port_name_2_1] [port_name_2_2]"
        exit 1
        ;;
esac