import pandas as pd
import subprocess

class Event:
    def __init__(self, event_name, event_effect_data_file):
        self.event_name = event_name
        df = pd.read_csv(event_effect_data_file)
        df = df[df["Tag"] == self.event_name].sort_values(by="Bin")
        self.impact_params = df.set_index("Bin")[
            ["Loss_rate", "Latency_mean", "Latency_std"]
        ].to_dict(orient="index")

    def get_impact_time(self):
        return [i for i in list(self.impact_params.keys())]

    def set_effect_params(self, time_slot, interface):
        proc = subprocess.Popen(
            "sudo tc qdisc change dev {interface} root delay {latency} {latency_std} loss {loss_rate}%".format(
                interface,
                self.impact_params[time_slot]['Latency_mean'],
                self.impact_params[time_slot]['Latency_std'],
            ),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = proc.communicate()
        if stdout:
            print(stdout)
        if stderr:
            print(stderr)
