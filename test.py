from st_visium_datasets import load_visium_dataset, setup_logging

setup_logging()

num_proc = 2
ds = load_visium_dataset("human", num_proc=num_proc)
