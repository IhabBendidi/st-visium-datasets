import os

from datasets import load_dataset as _load_dataset

from st_atlas_datasets import st_atlas


def load_dataset(name: str, *, cache_dir: str = "./data", num_proc: int | None = 2):
    path = os.path.join(os.path.dirname(st_atlas.__file__), f"{name}.py")
    return _load_dataset(
        path,
        cache_dir=os.path.abspath(cache_dir),
        save_infos=True,
        num_proc=num_proc,
    )


if __name__ == "__main__":
    load_dataset("visium_human_breast_cancer")
