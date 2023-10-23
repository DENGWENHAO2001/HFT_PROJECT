import argparse

def dict_to_args(dicts):
    for k, v in dicts.items():
        if isinstance(v, dict):
            dicts[k] = dict_to_args(v)
        else:
            continue
    args = argparse.Namespace(**dicts)
    return args