def raw_path(raw_base_path: str, dataset: str) -> str:
    return f"{raw_base_path.rstrip('/')}/{dataset}"

def checkpoint_path(checkpoints_base_path: str, dataset: str) -> str:
    return f"{checkpoints_base_path.rstrip('/')}/{dataset}"
