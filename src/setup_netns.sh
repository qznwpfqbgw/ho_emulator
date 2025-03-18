#!/bin/bash

set -e  # 遇到錯誤時停止執行

echo "[清理] 刪除舊的 Network Namespace 和 veth..."
sudo ip netns del Client 2>/dev/null || true
sudo ip netns del NETEM 2>/dev/null || true
sudo ip netns del Server 2>/dev/null || true
sudo ip link del veth0 2>/dev/null || true
sudo ip link del up_veth 2>/dev/null || true

echo "[1] 創建 Network Namespaces..."
sudo ip netns add Client
sudo ip netns add NETEM
sudo ip netns add Server

echo "[2] 創建 veth 配對..."
# Client <--> NETEM
sudo ip link add veth0 type veth peer name dl_veth
# NETEM <--> Server
sudo ip link add up_veth type veth peer name veth3

echo "[3] 移動 veth 到對應的 Namespace..."
# Client 端
sudo ip link set veth0 netns Client

# NETEM 端
sudo ip link set dl_veth netns NETEM
sudo ip link set up_veth netns NETEM

# Server 端
sudo ip link set veth3 netns Server

echo "[4] 設定 IP 地址..."
# Client 內部
sudo ip netns exec Client ip addr add 192.168.1.1/24 dev veth0
sudo ip netns exec Client ip link set veth0 up
sudo ip netns exec Client ip link set lo up
sudo ip netns exec Client ip route add 192.168.2.0/24 via 192.168.1.2 dev veth0

# NETEM 內部（中間節點）
sudo ip netns exec NETEM ip addr add 192.168.1.2/24 dev dl_veth
sudo ip netns exec NETEM ip addr add 192.168.2.1/24 dev up_veth
sudo ip netns exec NETEM ip link set dl_veth up
sudo ip netns exec NETEM ip link set up_veth up
sudo ip netns exec NETEM ip link set lo up

# Server 內部
sudo ip netns exec Server ip addr add 192.168.2.2/24 dev veth3
sudo ip netns exec Server ip link set veth3 up
sudo ip netns exec Server ip link set lo up
sudo ip netns exec Server ip route add 192.168.1.0/24 via 192.168.2.1 dev veth3

# 在 NETEM 啟用 IP 轉發
sudo ip netns exec NETEM sysctl -w net.ipv4.ip_forward=1

echo "✅ Network Namespace & veth 設定完成！"
echo "🌐 Client (192.168.1.1) <--> NETEM (192.168.1.2 / 192.168.2.1) <--> Server (192.168.2.2)"
