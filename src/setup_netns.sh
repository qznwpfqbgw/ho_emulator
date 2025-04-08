#!/bin/bash

set -e  # 遇到錯誤時停止執行

echo "[清理] 刪除舊的 Network Namespace 和 veth..."
sudo ip netns del Client 2>/dev/null || true
sudo ip netns del NETEM 2>/dev/null || true
sudo ip netns del Server 2>/dev/null || true
sudo ip link del ul_veth1 2>/dev/null || true
sudo ip link del ul_veth2 2>/dev/null || true
sudo ip link del dl_veth1 2>/dev/null || true
sudo ip link del dl_veth2 2>/dev/null || true

echo "[1] 創建 Network Namespaces..."
sudo ip netns add Client
sudo ip netns add NETEM
sudo ip netns add Server

echo "[2] 創建 veth 配對..."
# Client <--> NETEM
sudo ip link add veth0_dl1 type veth peer name dl_veth1
sudo ip link add veth0_dl2 type veth peer name dl_veth2

# NETEM <--> Server
sudo ip link add ul_veth1 type veth peer name veth3_ul1
sudo ip link add ul_veth2 type veth peer name veth3_ul2

echo "[3] 移動 veth 到對應的 Namespace..."
# Client 端
sudo ip link set veth0_dl1 netns Client
sudo ip link set veth0_dl2 netns Client

# NETEM 端
sudo ip link set dl_veth1 netns NETEM
sudo ip link set dl_veth2 netns NETEM
sudo ip link set ul_veth1 netns NETEM
sudo ip link set ul_veth2 netns NETEM

# Server 端
sudo ip link set veth3_ul1 netns Server
sudo ip link set veth3_ul2 netns Server

echo "[4] 設定 IP 地址..."
# Client 內部
sudo ip netns exec Client ip addr add 192.168.1.1/24 dev veth0_dl1
sudo ip netns exec Client ip addr add 192.168.3.1/24 dev veth0_dl2
sudo ip netns exec Client ip link set veth0_dl1 up
sudo ip netns exec Client ip link set veth0_dl2 up
sudo ip netns exec Client ip link set lo up
sudo ip netns exec Client ip route add 192.168.2.0/24 via 192.168.1.2 dev veth0_dl1
sudo ip netns exec Client ip route add 192.168.4.0/24 via 192.168.3.2 dev veth0_dl2

# NETEM 內部（中間節點）
sudo ip netns exec NETEM ip addr add 192.168.1.2/24 dev dl_veth1
sudo ip netns exec NETEM ip addr add 192.168.3.2/24 dev dl_veth2
sudo ip netns exec NETEM ip addr add 192.168.2.1/24 dev ul_veth1
sudo ip netns exec NETEM ip addr add 192.168.4.1/24 dev ul_veth2
sudo ip netns exec NETEM ip link set dl_veth1 up
sudo ip netns exec NETEM ip link set dl_veth2 up
sudo ip netns exec NETEM ip link set ul_veth1 up
sudo ip netns exec NETEM ip link set ul_veth2 up
sudo ip netns exec NETEM ip link set lo up

# Server 內部
sudo ip netns exec Server ip addr add 192.168.2.2/24 dev veth3_ul1
sudo ip netns exec Server ip addr add 192.168.4.2/24 dev veth3_ul2
sudo ip netns exec Server ip link set veth3_ul1 up
sudo ip netns exec Server ip link set veth3_ul2 up
sudo ip netns exec Server ip link set lo up
sudo ip netns exec Server ip route add 192.168.1.0/24 via 192.168.2.1 dev veth3_ul1
sudo ip netns exec Server ip route add 192.168.3.0/24 via 192.168.4.1 dev veth3_ul2

# 在 NETEM 啟用 IP 轉發
sudo ip netns exec NETEM sysctl -w net.ipv4.ip_forward=1

echo "✅ Network Namespace & veth 設定完成！"
echo "🌐 Client (192.168.1.1) <--> NETEM (192.168.1.2 / 192.168.2.1) <--> Server (192.168.2.2)"
echo "🌐 Client (192.168.3.1) <--> NETEM (192.168.3.2 / 192.168.4.1) <--> Server (192.168.4.2)"
