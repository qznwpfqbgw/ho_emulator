#!/bin/bash

# 名稱: manage_virtual_serial.sh
# 功能: 用 tmux 管理多個虛擬串口對的創建與關閉
# 用法: 
#   ./manage_virtual_serial.sh start [num_of_pairs]   # 預設 1 組，可指定數量
#   ./manage_virtual_serial.sh stop                   # 關閉所有組

ACTION=$1
NUM_PAIRS=${2:-1}  # 預設創建 1 組，如果有參數則使用指定數量
SESSION_PREFIX="ho_emu_tmux_sess"

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

# 啟動多對虛擬串口
start_sessions() {
    for ((i=0; i<NUM_PAIRS; i++)); do
        SESSION_NAME="${SESSION_PREFIX}_$i"
        PORT1="/dev/ttyV$((i))"
        PORT2="/dev/ttyV$((NUM_PAIRS+i))"

        if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
            echo "Warning: tmux session '$SESSION_NAME' already exists. Skipping..."
            continue
        fi

        echo "Creating virtual serial ports: $PORT1 <-> $PORT2 in tmux session '$SESSION_NAME'..."
        tmux new-session -d -s "$SESSION_NAME" "socat -d -d pty,raw,echo=0,mode=777,link=$PORT1 pty,raw,echo=0,mode=777,link=$PORT2"
    done
    echo "Virtual serial port creation completed."
}

# 關閉所有虛擬串口
stop_sessions() {
    TMUX_SESSIONS=$(tmux list-sessions 2>/dev/null | grep "$SESSION_PREFIX" | awk -F: '{print $1}')

    if [ -z "$TMUX_SESSIONS" ]; then
        echo "No virtual serial port sessions found."
        exit 0
    fi

    echo "Stopping all virtual serial port sessions..."
    for SESSION in $TMUX_SESSIONS; do
        tmux kill-session -t "$SESSION"
        echo "Stopped session: $SESSION"
    done

    echo "All virtual serial ports stopped."
}

# 主流程
case "$ACTION" in
    start)
        start_sessions
        ;;
    stop)
        stop_sessions
        ;;
    *)
        echo "Usage: $0 <start [num_of_pairs] | stop>"
        exit 1
        ;;
esac
