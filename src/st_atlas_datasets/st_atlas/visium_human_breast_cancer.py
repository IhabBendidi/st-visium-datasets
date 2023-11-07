import json
import os
import typing as tp

import datasets
from datasets.data_files import DataFilesDict

_DESCRIPTION = "TODO"
_LICENSE = "TODO"
_CITATION = "TODO"
_HOMEPAGE = "https://www.10xgenomics.com/resources/datasets/human-breast-cancer-ductal-carcinoma-in-situ-invasive-carcinoma-ffpe-1-standard-1-3-0"
_DATA_FILES = {
    "fastqs": [
        "https://cf.10xgenomics.com/samples/spatial-exp/1.3.0/Visium_FFPE_Human_Breast_Cancer/Visium_FFPE_Human_Breast_Cancer_fastqs.tar"
    ],
    "image_tiff": [
        "https://cf.10xgenomics.com/samples/spatial-exp/1.3.0/Visium_FFPE_Human_Breast_Cancer/Visium_FFPE_Human_Breast_Cancer_image.tif"
    ],
    "probe_set": [
        "https://cf.10xgenomics.com/samples/spatial-exp/1.3.0/Visium_FFPE_Human_Breast_Cancer/Visium_FFPE_Human_Breast_Cancer_probe_set.csv"
    ],
}


class VisiumHumanBreastCancer(datasets.GeneratorBasedBuilder):
    BUILDER_CONFIGS = [
        datasets.BuilderConfig(
            name="breast_cancer",
            version=datasets.Version("1.0.0"),
            data_files=DataFilesDict.from_local_or_remote(_DATA_FILES),
        )
    ]

    def _info(self):
        return datasets.DatasetInfo(
            description=_DESCRIPTION,
            supervised_keys=None,
            homepage=_HOMEPAGE,
            license=_LICENSE,
            citation=_CITATION,
        )

    def _split_generators(self, dl_manager: datasets.DownloadManager):
        local_data_files = dl_manager.download_and_extract(self.config.data_files)
        print(local_data_files)
        return [
            datasets.SplitGenerator(
                name="_all",
                gen_kwargs={
                    "local_data_files": local_data_files,
                },
            )
        ]

    def _generate_examples(self, *args, **kwargs):
        print(args)
        print(kwargs)


if __name__ == "__main__":
    import pyfastx
    from tifffile import TiffFile, imread

    img_file = (
        "data/visium_human_breast_cancer/raw/Visium_FFPE_Human_Breast_Cancer_image.tif"
    )
    fastq_file = (
        "data/visium_human_breast_cancer/raw/Visium_FFPE_Human_Breast_Cancer_fastqs.tar"
    )

    for read in pyfastx.Fastq(fastq_file, build_index=False):
        print(read)

    # im = imread(filepath)
    # print(im.shape)
    # print(im.dtype)
    # print(im.min(), im.max())

    # with TiffFile(filepath) as tif:
    #     for page in tif.pages:
    #         image = page.asarray()
    #         print("PAGE:", page, image.shape, image.dtype)
