import subprocess

class Controller:
    def __init__(self, interface, rate_mbit=1000, burst_mbit=100, latency_ms=5):
        proc = subprocess.Popen(
            [
                "tc",
                "qdisc",
                "add",
                "dev",
                interface,
                "root",
                "handle",
                "1:",
                "tbf",
                "rate",
                f"{rate_mbit}mbit",
                "burst",
                f"{burst_mbit}mbit",
                "latency",
                f"{latency_ms}ms"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = proc.communicate()
        if stdout:
            print(stdout)
        if stderr:
            print(stderr)

        proc = subprocess.Popen(
            [
                "tc",
                "qdisc",
                "add",
                "dev",
                interface,
                "parent",
                "1:1",
                "handle",
                "10:",
                "netem",
                "delay",
                "0ms",
                "loss",
                "0%",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = proc.communicate()
        if stdout:
            print(stdout)
        if stderr:
            print(stderr)
            
            
    def run_netem_cmd(self, packet_loss, latency_mean, latency_std, distribution, interface):
        if latency_std == 0:
            cmds = [
                "tc",
                "qdisc",
                "change",
                "dev",
                interface,
                "parent",
                "1:1",
                "handle",
                "10:",
                "netem",
                "delay",
                f"{latency_mean:.6f}ms",
                "loss",
                f"{packet_loss:.6f}%",
            ]
        else:
            cmds = [
                "tc",
                "qdisc",
                "change",
                "dev",
                interface,
                "parent",
                "1:1",
                "handle",
                "10:",
                "netem",
                "delay",
                f"{latency_mean:.6f}ms",
                f"{latency_std:.6f}ms",
                "distribution",
                distribution,
                "loss",
                f"{packet_loss:.6f}%",
            ]

        print(" ".join(cmds))

        proc = subprocess.Popen(cmds, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()

        if stdout:
            print(stdout.decode())
        if stderr:
            print(stderr.decode())