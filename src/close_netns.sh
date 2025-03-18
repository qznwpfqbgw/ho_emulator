#!/bin/bash

set -e  # 確保錯誤時停止執行

echo "[關閉] 刪除 Network Namespaces 和 veth 介面..."
sudo ip netns del S1 2>/dev/null || echo "S1 已刪除"
sudo ip netns del R 2>/dev/null || echo "R 已刪除"
sudo ip netns del S2 2>/dev/null || echo "S2 已刪除"

sudo ip link del veth0 2>/dev/null || echo "veth0 已刪除"
sudo ip link del veth2 2>/dev/null || echo "veth2 已刪除"

echo "✅ Network Namespace 已關閉！"