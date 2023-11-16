import csv
import dataclasses as dc
import json
import math
import typing as tp
from pathlib import Path

import datasets
import numpy as np
from PIL import Image, ImageDraw
from pydantic.dataclasses import dataclass
from tifffile import TiffFile

from st_atlas_datasets.base import TenXGenomicsBaseBuilder, TenXGenomicsBuilderConfig

_BUILDER_CONFIGS = [
    TenXGenomicsBuilderConfig(
        full_name="Human Breast Cancer: Ductal Carcinoma In Situ, Invasive Carcinoma (FFPE)",
        short_name="Human Breast Cancer Invasive Ductal Carcinoma",
        homepage="https://www.10xgenomics.com/resources/datasets/human-breast-cancer-ductal-carcinoma-in-situ-invasive-carcinoma-ffpe-1-standard-1-3-0",
        description="10x Genomics obtained FFPE human breast tissue from BioIVT Asterand Human Tissue Specimens. The tissue was annotated with Ductal Carcinoma In Situ, Invasive Carcinoma. The tissue was sectioned as described in Visium Spatial Gene Expression for FFPE - Tissue Preparation Guide Demonstrated Protocol (CG000408). Tissue sections of 5 µm were placed on Visium Gene Expression slides, then stained following Deparaffinization, H&E Staining, Imaging & Decrosslinking Demonstrated Protocol (CG000409).",
        tiff_url="https://cf.10xgenomics.com/samples/spatial-exp/1.3.0/Visium_FFPE_Human_Breast_Cancer/Visium_FFPE_Human_Breast_Cancer_image.tif",
        spatial_url="https://cf.10xgenomics.com/samples/spatial-exp/1.3.0/Visium_FFPE_Human_Breast_Cancer/Visium_FFPE_Human_Breast_Cancer_spatial.tar.gz",
        filtered_feature_bc_matrix_url="https://cf.10xgenomics.com/samples/spatial-exp/1.3.0/Visium_FFPE_Human_Breast_Cancer/Visium_FFPE_Human_Breast_Cancer_filtered_feature_bc_matrix.h5",
        metadata={
            "species": "human",
            "ethnicity": "asian",
            "sex": "female",
            "age": "73",
            "anatomical_entity": "breast",
            "disease_state": "invasive ductal carcinoma",
            "preservation_method": "FFPE",
            "biomaterial_type": "Specimen from Organism",
            "donor_count": "1",
            "development_stage": "adult",
            "published_at": "2021-06-09",
            "software": "Space Ranger 1.3.0",
        },
    ),
    STAtlasBuilderConfig(
        full_name="Human Prostate Cancer, Adenocarcinoma with Invasive Carcinoma (FFPE)",
        short_name="Human Prostate Cancer Adenocarcinoma",
        homepage="https://www.10xgenomics.com/resources/datasets/human-prostate-cancer-adenocarcinoma-with-invasive-carcinoma-ffpe-1-standard-1-3-0",
        description="10x Genomics obtained FFPE human prostate tissue from Indivumed Human Tissue Specimens. The tissue was annotated with Adenocarcinoma, Invasive Carcinoma. The tissue was sectioned as described in Visium Spatial Gene Expression for FFPE – Tissue Preparation Guide Demonstrated Protocol (CG000408). Tissue sections of 5 µm were placed on Visium Gene Expression slides, then stained following Deparaffinization, H&E Staining, Imaging & Decrosslinking Demonstrated Protocol (CG000409).",
        urls=[
            "https://cf.10xgenomics.com/samples/spatial-exp/1.3.0/Visium_FFPE_Human_Prostate_Cancer/Visium_FFPE_Human_Prostate_Cancer_image.tif",
            "https://cf.10xgenomics.com/samples/spatial-exp/1.3.0/Visium_FFPE_Human_Prostate_Cancer/Visium_FFPE_Human_Prostate_Cancer_spatial.tar.gz",
            "https://cf.10xgenomics.com/samples/spatial-exp/1.3.0/Visium_FFPE_Human_Prostate_Cancer/Visium_FFPE_Human_Prostate_Cancer_filtered_feature_bc_matrix.h5",
        ],
        metadata={
            "species": "human",
            "sex": "male",
            "anatomical_entity": "prostate",
            "disease_state": "Adenocarcinoma",
            "preservation_method": "FFPE",
            "biomaterial_type": "Specimen from Organism",
            "donor_count": "1",
            "published_at": "2021-06-09",
            "software": "Space Ranger 1.3.0",
        },
    ),
]


class BreastCancer(STAtlasBaseBuilder):
    BUILDER_CONFIGS = _BUILDER_CONFIGS


def _get_spot_diameter(scale_factors_path: str | Path) -> int:
    with open(scale_factors_path) as f:
        data = json.load(f)
    return math.ceil(data["spot_diameter_fullres"])


class VisiumHumanBreastCancer(datasets.GeneratorBasedBuilder):
    BUILDER_CONFIGS = [datasets.BuilderConfig(name="default", version="1.0.0")]
    _TILE_SHAPE = (250, 250)

    def _info(self):
        return datasets.DatasetInfo(
            description=_DESCRIPTION,
            supervised_keys=None,
            homepage=_HOMEPAGE,
            license=_LICENSE,
            citation=_CITATION,
        )

    def _split_generators(self, dl_manager: datasets.DownloadManager):
        (
            tiff_path,
            probe_set_path,
            spatial_imaging_data_dir,
            filtered_feature_bc_matrix_path,
        ) = dl_manager.download_and_extract(_URLS)
        spatial_dir = Path(spatial_imaging_data_dir) / "spatial"
        spots = _Spot.from_csv(spatial_dir / "tissue_positions_list.csv")
        px_spot_diameter = _get_spot_diameter(spatial_dir / "scalefactors_json.json")
        tiff = TiffFile(tiff_path)
        tiff_img = tiff.asarray()
        rgb_img = Image.fromarray(tiff_img, mode="YCbCr").convert("RGB")
        target_res = (3840, 3530)
        scale = rgb_img.size[0] / target_res[0]
        rgb_img = rgb_img.resize(target_res, resample=Image.BICUBIC)
        rgb_img.save("base_test.png")
        draw = ImageDraw.Draw(rgb_img)
        keypoint_radius = 5
        color = "blue"
        for spot in spots:
            if not spot.in_tissue:
                continue
            xmin, ymin, xmax, ymax = spot.compute_spot_bbox(px_spot_diameter)
            keypoint_x = spot.pxl_col_in_fullres / scale
            keypoint_y = spot.pxl_row_in_fullres / scale
            draw.ellipse(
                (
                    keypoint_x - keypoint_radius,
                    keypoint_y - keypoint_radius,
                    keypoint_x + keypoint_radius,
                    keypoint_y + keypoint_radius,
                ),
                fill=color,
                outline=color,
            )
            draw.rectangle(
                (xmin / scale, ymin / scale, xmax / scale, ymax / scale),
                outline=color,
                width=2,
            )
        rgb_img.save("spot_test.png")

        exit(0)
        return [datasets.SplitGenerator(name="default")]

    # def _generate_examples(self):
    #     print(fastqs_filepaths)
    #     tiff_image = read_tif(tiff_filepath)
    #     tiff = TiffFile(tiff_filepath)
    #     print(tiff.imagej_metadata)
    #     print(tiff_image.shape)
    #     exit(0)


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
