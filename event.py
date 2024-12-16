import pandas as pd
import subprocess


class Event:
    def __init__(self, event_name, impact_params: dict):
        self.event_name = event_name
        self.impact_params = impact_params

    def get_impact_time(self):
        return [i for i in list(self.impact_params.keys())]

    def set_effect_params(self, time_slot, interface):
        print(self.event_name, time_slot)
        # print(
        #     "tc qdisc change dev {interface} root netem delay {latency:.2f}ms {latency_std:.2f}ms loss {loss_rate:.2f}%".format(
        #         interface=interface,
        #         latency=self.impact_params[time_slot]["latency_mean"],
        #         latency_std=self.impact_params[time_slot]["latency_std_mean"],
        #         loss_rate=self.impact_params[time_slot]["plr_mean"]
        #     )
        # )
        proc = subprocess.Popen(
            [
                "tc", "qdisc", "change", "dev", interface, "root", "netem",
                "delay", f"{self.impact_params[time_slot]['latency_mean']:.2f}ms", f"{self.impact_params[time_slot]['latency_std_mean']:.2f}ms",
                "loss", f"{self.impact_params[time_slot]['plr_mean']:.2f}%"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = proc.communicate()
        if stdout:
            print(stdout)
        if stderr:
            print(stderr)


def create_event_params(event_params_file):
    event_dict = {}
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
    df['bin'] = df['bin'].round(2)
    for e in event_name_list:
        tmp = df[df["tag"] == e].sort_values(by="bin")
        impact_params = tmp.set_index("bin")[
            ["plr_mean", "latency_mean", "latency_std_mean"]
        ].to_dict(orient="index")
        event_dict[e] = Event(e, impact_params)
    return event_dict


if __name__ == "__main__":
    event_dict = create_event_params('test/br_dl_test_event_params.csv')
    print(event_dict)
