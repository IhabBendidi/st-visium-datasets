# st-visium-datasets


Spatial Transcriptomics Atlas for 10XGenomics datasets in huggingface datasets format :huggingface:

## Installation

With python 3.12 and poetry installed, you can install the library with the following command:

```bash
poetry install
```

Or through :

```bash
pip install git+https://github.com/IhabBendidi/st-visium-datasets
```



## Usage

This library provides a common interface for 10XGenomics 'Gene Expression' datasets based on huggingface.

This library provides a single dataset class: `visium` with different configurations

Please note that data would take time to be downloaded the first time. Data is saved under Huggingface's package dataset directory. You can change the location.

```python
from st_visium_datasets import load_visium_dataset

ds = load_visium_dataset() # default lodas 'all' config
# or ds = load_visium_dataset("human_skin")

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

torch_ds = ds.with_format("torch", device=device)
# cf: https://huggingface.co/docs/datasets/v2.15.0/en/use_with_pytorch
```

Please note that `load_visium_dataset` follows the same configuration as `load_dataset` from huggingface datasets. It can reuse the same arguments. Similarly, using downloaded datasets from specific folders, or downloading them unto specific folders can be done using the `data_dir` and `cache_dir` arguments.


```python
ds = load_visium_dataset('human_prostate', cache_dir="/projects/minos/ihab/visium_data")
```

## Availlable configs
| name                     | number_of_spots_under_tissue |
|--------------------------|------------------------------|
| all                      | 344961                       |
| human                    | 192976                       |
| human_heart              | 8482                         |
| human_lymph_node         | 8074                         |
| human_kidney             | 5936                         |
| human_colorectal         | 9080                         |
| human_skin               | 3458                         |
| human_prostate           | 14334                        |
| human_ovary              | 15153                        |
| human_brain              | 27696                        |
| human_large_intestine    | 6276                         |
| human_spinal_cord        | 5624                         |
| human_cerebellum         | 4992                         |
| human_brain_cerebellum   | 9984                         |
| human_lung               | 10053                        |
| human_breast             | 38063                        |
| human_colon              | 25771                        |
| mouse                    | 151985                       |
| mouse_olfactory_bulb     | 2370                         |
| mouse_kidney             | 6000                         |
| mouse_brain              | 123254                       |
| mouse_kidney_brain       | 2805                         |
| mouse_mouse_embryo       | 12877                        |
| mouse_lung_brain         | 4679                         |

