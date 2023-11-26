import csv
import typing as tp
from pathlib import Path

import numpy as np
from PIL import Image, ImageDraw
from pydantic import BaseModel, Field, RootModel


class VisiumSpot(BaseModel):
    barcode: str = Field(
        ..., description="The sequence of the barcode associated to the spot."
    )
    in_tissue: bool = Field(..., description="Whether the spot is in the tissue.")
    array_row: int = Field(
        ..., description="The row of the spot in the array of spots."
    )
    array_col: int = Field(
        ..., description="The column of the spot in the array of spots."
    )
    pxl_row_in_fullres: int = Field(
        ..., description="The x coordinate of the center of the spot."
    )
    pxl_col_in_fullres: int = Field(
        ..., description="The y coordinate of the center of the spot."
    )

    @property
    def index(self) -> tuple[int, int]:
        """Return the index of the spot in the array of spots."""
        return self.array_row, self.array_col

    @property
    def center(self) -> tuple[int, int]:
        """Return the coordinates of the center of the spot in the full resolution
        image as (x, y).
        """
        return self.pxl_col_in_fullres, self.pxl_row_in_fullres

    def get_bbox(self, spot_diameter: float) -> tuple[int, int, int, int]:
        """Compute the bounding box of the spot in the full resolution image.
        xmin, ymin, xmax, ymax
        """
        spot_radius = spot_diameter / 2
        xmin = int(self.pxl_col_in_fullres - spot_radius)
        ymin = int(self.pxl_row_in_fullres - spot_radius)
        xmax = int(self.pxl_col_in_fullres + spot_radius)
        ymax = int(self.pxl_row_in_fullres + spot_radius)
        return xmin, ymin, xmax, ymax

    def get_crop(self, img: np.ndarray, spot_diameter: float) -> np.ndarray:
        """Extract the spot from the full resolution original image."""
        xmin, ymin, xmax, ymax = self.get_bbox(spot_diameter)
        return img[ymin:ymax, xmin:xmax, ...]

    def get_pil_crop(self, img: Image.Image, spot_diameter: float) -> Image.Image:
        """Extract the spot from the full resolution PIL image."""
        xmin, ymin, xmax, ymax = self.get_bbox(spot_diameter)
        return img.crop((xmin, ymin, xmax, ymax))


class VisiumSpots(RootModel):
    root: list[VisiumSpot]

    def __iter__(self):
        return iter(self.root)

    def __getitem__(self, item: int):
        return self.root[item]

    def __len__(self):
        return len(self.root)

    @classmethod
    def from_visium_csv(cls, csv_path: str | Path) -> tp.Self:
        csv_path = Path(csv_path)
        # we have 2 versions of the csv file, one with a header and one without
        if csv_path.name == "tissue_positions_list.csv":  # without header
            fieldnames = [
                "barcode",
                "in_tissue",
                "array_row",
                "array_col",
                "pxl_row_in_fullres",
                "pxl_col_in_fullres",
            ]
        else:
            fieldnames = None
        with open(csv_path) as f:
            reader = csv.DictReader(f, fieldnames=fieldnames)
            spots = list(reader)
        return cls.model_validate(spots)

    def plot(
        self,
        img: Image.Image,
        spot_diameter: float,
        resize_longest: int | None = 3840,
        only_in_tissue: bool = True,
        show: bool = True,
        save_path: str | Path | None = None,
    ) -> Image.Image:
        """Plot the spots on the full resolution image."""

        if resize_longest:
            w, h = img.size
            resize_ratio = resize_longest / max(h, w)
            new_w, new_h = int(w * resize_ratio), int(h * resize_ratio)
            new_img = img.resize((new_w, new_h), resample=Image.BILINEAR)
        else:
            resize_ratio = 1
            new_img = img.copy()
        draw = ImageDraw.Draw(new_img)
        color = "blue"
        # line width of 2px and point radius of 3px if max size is 3840
        line_width = (max(new_img.size) * 2) // 3840
        point_radius = (max(new_img.size) * 3) // 3840
        for spot in self.root:
            if only_in_tissue and not spot.in_tissue:
                continue
            bbox = np.array(spot.get_bbox(spot_diameter)) * resize_ratio
            center = np.array(spot.center) * resize_ratio
            draw.rectangle(tuple(bbox), outline=color, width=line_width)  # type: ignore
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
        if show:
            new_img.show()

        if save_path:
            new_img.save(save_path, format="PNG")

        return new_img
