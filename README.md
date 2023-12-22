# st-visium-datasets


Spatial Transcriptomics Atlas for 10XGenomics datasets in huggingface datasets format :huggingface:

## Installation

TODO

## Usage

This library provides a common interface for 10XGenomics 'Gene Expression' datasets based on huggingface.

This library provides a single dataset: `visium` with different configurations

```python
from st_visium_datasets import load_visium_dataset
import torch

ds = load_visium_dataset() # default lodas 'all' config
# or ds = load_visium_dataset("human")

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

torch_ds = ds.with_format("torch", device=device)
# cf: https://huggingface.co/docs/datasets/v2.15.0/en/use_with_pytorch
```

## Availlable configs

| name                   |   number_of_spots_under_tissue |   number_of_genes_detected |
|------------------------|--------------------------------|----------------------------|
| all                    |                         344961 |                    1651817 |
| human                  |                         192976 |                     863878 |
| human-heart            |                           8482 |                      40491 |
| human-lymph-node       |                           8074 |                      48178 |
| human-kidney           |                           5936 |                      18068 |
| human-colorectal       |                           9080 |                      18077 |
| human-skin             |                           3458 |                      18069 |
| human-prostate         |                          14334 |                      68215 |
| human-ovary            |                          15153 |                      77975 |
| human-brain            |                          27696 |                     110543 |
| human-large-intestine  |                           6276 |                      39440 |
| human-spinal-cord      |                           5624 |                      39951 |
| human-cerebellum       |                           4992 |                      17355 |
| human-brain-cerebellum |                           9984 |                      41163 |
| human-lung             |                          10053 |                      36143 |
| human-breast           |                          38063 |                     217985 |
| human-colon            |                          25771 |                      72225 |
| mouse                  |                         151985 |                     787939 |
| mouse-olfactory-bulb   |                           2370 |                      38664 |
| mouse-kidney           |                           6000 |                      57469 |
| mouse-brain            |                         123254 |                     614201 |
| mouse-kidney-brain     |                           2805 |                      19399 |
| mouse-mouse-embryo     |                          12877 |                      38808 |
| mouse-lung-brain       |                           4679 |                      19398 |
