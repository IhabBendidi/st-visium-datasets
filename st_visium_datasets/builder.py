from __future__ import annotations

import logging
import typing as tp
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import tifffile
from PIL import Image, ImageDraw
from tqdm import tqdm

from st_visium_datasets.base import VisiumConfig, VisiumConfigs
from st_visium_datasets.feature_barcode import load_feature_barcode_matrix_df
from st_visium_datasets.spatial import get_spot_diameter_px, get_tissue_positions_df

logger = logging.getLogger(__name__)


def build_spots_datasets(
    *,
    configs: VisiumConfigs,
    data_dir: Path,
    dataset_paths: dict[str, dict[str, str | Path]],
    spot_diameter_px: int | tp.Literal["auto"] = "auto",
    pil_resize_longest: int | None = 3840,
    overwrite: bool = False,
    num_proc: int | None = 2,
) -> list[Path]:
    with tqdm(
        total=len(dataset_paths), desc="Building datasets ...", position=0
    ) as pbar:
        with ThreadPoolExecutor(max_workers=num_proc) as executor:
            futures = []
            for i, (name, paths) in enumerate(dataset_paths.items()):
                f = executor.submit(
                    _build_spots_dataset,
                    config=configs[name],
                    data_dir=data_dir,
                    spot_diameter_px=spot_diameter_px,
                    pil_resize_longest=pil_resize_longest,
                    overwrite=overwrite,
                    index=i + 1,
                    **paths,
                )
                futures.append(f)

            dataset_dirs = []
            for f in as_completed(futures):
                dataset_dirs.append(f.result())
                pbar.update(1)

    return dataset_dirs


def _build_spots_dataset(
    *,
    config: VisiumConfig,
    data_dir: Path,
    tiff: Path | str,
    feature_bc_matrix: Path | str,
    spatial: Path | str,
    spot_diameter_px: int | tp.Literal["auto"] = "auto",
    pil_resize_longest: int | None = 3840,
    overwrite: bool = False,
    index: int = 0,
) -> Path:
    dataset_dir = data_dir / config.name
    spots_dir = dataset_dir / "spots"
    spots_dir.mkdir(exist_ok=True, parents=True)

    logger.debug(f"[{config.name}] Loading tissue positions dataframe ...")
    spots_df = get_tissue_positions_df(spatial)

    if not overwrite and _is_spots_dataset_already_built(spots_df, spots_dir):
        logger.debug(f"[{config.name}] Spots already extracted.")
        return dataset_dir

    logger.debug(f"[{config.name}] Loading feature barcode matrix dataframe ...")
    features_df = load_feature_barcode_matrix_df(feature_bc_matrix)

    if spot_diameter_px == "auto":
        spot_diameter_px = get_spot_diameter_px(spatial)

    logger.debug(f"[{config.name}] Reading tiff image ...")
    img: np.ndarray = _read_tiff(tiff)
    pil_img, resize_ratio = _get_pil_img(img, resize_longest=pil_resize_longest)
    draw, color = ImageDraw.Draw(pil_img), "blue"
    # line width of 2px and point radius of 3px if max size is 3840
    line_width, point_radius = (
        (max(pil_img.size) * 2) // 3840,
        (max(pil_img.size) * 3) // 3840,
    )

    logger.debug(f"[{config.name}] Extracting spots ...")
    with tqdm(
        total=len(spots_df),
        desc=f"Extracting spots '{config.name}'",
        position=index,
        leave=False,
    ) as pbar:
        for barcode, row in spots_df.iterrows():
            x, y = row["x"], row["y"]
            xmin, ymin, xmax, ymax = _get_spot_bbox(x, y, spot_diameter_px)
            spot_img: np.ndarray = img[ymin:ymax, xmin:xmax, ...]
            spot_img_path = spots_dir / f"{barcode}.npy"
            np.save(spot_img_path, spot_img)

            # get spot features
            features_df[[barcode]].rename(columns={barcode: "count"}).to_csv(
                spots_dir / f"{barcode}.csv"
            )

            _draw_spot_bbox(
                draw, (xmin, ymin, xmax, ymax), resize_ratio, color, line_width
            )
            _draw_spot_center(draw, (x, y), resize_ratio, color, point_radius)
            pbar.update(1)

    pil_img.save(dataset_dir / "spots.png", format="PNG")
    config.save(dataset_dir / "config.json")
    return dataset_dir


def _is_spots_dataset_already_built(spots_df: pd.DataFrame, spots_dir: Path):
    barcodes: list[str] = list(spots_df.index)
    for barcode in barcodes:
        if not (spots_dir / f"{barcode}.npy").is_file():
            return False
        if not (spots_dir / f"{barcode}.csv").is_file():
            return False
    return True


def _get_spot_bbox(x: int, y: int, spot_diameter_px: int) -> tuple[int, int, int, int]:
    spot_radius = spot_diameter_px / 2
    xmin, ymin = int(x - spot_radius), int(y - spot_radius)
    xmax, ymax = int(x + spot_radius), int(y + spot_radius)
    return xmin, ymin, xmax, ymax


def _read_tiff(tiff: Path | str) -> np.ndarray:
    return tifffile.imread(tiff, is_ome=False)


def _get_pil_img(
    img: np.ndarray, resize_longest: int | None = 3840
) -> tuple[Image.Image, float]:
    pil_img = Image.fromarray(img, mode="YCbCr").convert("RGB")
    if not resize_longest:
        return pil_img, 1.0
    w, h = pil_img.size
    resize_ratio = resize_longest / max(h, w)
    new_w, new_h = int(w * resize_ratio), int(h * resize_ratio)
    return pil_img.resize((new_w, new_h), resample=Image.BILINEAR), resize_ratio


def _draw_spot_bbox(
    draw: ImageDraw.ImageDraw,
    bbox: tuple[int, int, int, int] | np.ndarray,
    resize_ratio: float = 1.0,
    color: str = "blue",
    line_width=2,
) -> None:
    bbox = np.array(bbox) * resize_ratio
    draw.rectangle(tuple(bbox), outline=color, width=line_width)  # type: ignore


def _draw_spot_center(
    draw: ImageDraw.ImageDraw,
    center: tuple[int, int] | np.ndarray,
    resize_ratio: float = 1.0,
    color: str = "blue",
    point_radius=3,
) -> None:
    center = np.array(center) * resize_ratio
    draw.ellipse(
        (
            center[0] - point_radius,
            center[1] - point_radius,
            center[0] + point_radius,
            center[1] + point_radius,
        ),
        fill=color,
        outline=color,
    )
