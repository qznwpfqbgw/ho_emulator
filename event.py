import pandas as pd
import subprocess

class Event:
    def __init__(self, event_name, impact_params: dict):
        self.event_name = event_name
        self.impact_params = impact_params

    def get_impact_time(self):
        return [i for i in list(self.impact_params.keys())]

    def set_effect_params(self, time_slot, interface):
        proc = subprocess.Popen(
            "sudo tc qdisc change dev {interface} root delay {latency} {latency_std} loss {loss_rate}%".format(
                interface,
                self.impact_params[time_slot]["Latency_mean"],
                self.impact_params[time_slot]["Latency_std"],
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = proc.communicate()
        if stdout:
            print(stdout)
        if stderr:
            print(stderr)

def create_event_params_list(event_params_file):
    event_list = []
    event_name_list = [
        "ENBH",
        "LTEH",
        "MCGF",
        "MCGH",
        "MNBH",
        "NASR",
        "SCGA",
        "SCGC-I",
        "SCGC-II",
        "SCGF",
        "SCGM",
        "SCGR-I",
        "SCGR-II",
        "Stable"
    ]
    df = pd.read_csv(event_params_file)
    for e in event_name_list:
        tmp = df[df["tag"] == e].sort_values(by="bin")
        impact_params = tmp.set_index("bin")[
            ["plr_mean", "latency_mean", "latency_std_mean"]
        ].to_dict(orient="index")
        event_list.append(Event(e, impact_params))
    
    return event_list

if __name__ == "__main__":
    event_list: list[Event] = create_event_params_list('test/br_dl_test_event_params.csv')
    for i in event_list:
        print(i.event_name, i.impact_params)