#!/bin/bash

set -e  # 確保錯誤時停止執行

echo "[關閉] 刪除 Network Namespaces 和 veth 介面..."
sudo ip netns del Client 2>/dev/null || echo "Client 已刪除"
sudo ip netns del NETEM 2>/dev/null || echo "R 已刪除"
sudo ip netns del Server 2>/dev/null || echo "S2 已刪除"

sudo ip link del dl_veth 2>/dev/null || echo "dl_veth 已刪除"
sudo ip link del up_veth 2>/dev/null || echo "up_veth 已刪除"

echo "✅ Network Namespace 已關閉！"