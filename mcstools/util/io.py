import yaml


def load_yaml(path):
    with open(path, "r") as file:
        print(f"Loading config from {path}")
        return yaml.safe_load(file)
