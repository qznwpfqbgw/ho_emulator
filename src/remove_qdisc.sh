#!/bin/bash
# usage: ./remove_qdisc.sh <interface_name>
# 設定介面
interface=$1

# 刪除子 qdisc (netem)
tc qdisc del dev "$interface" parent 1:1

# 刪除根 qdisc (tbf)
tc qdisc del dev "$interface" root