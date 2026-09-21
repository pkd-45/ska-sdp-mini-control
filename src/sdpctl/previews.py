from __future__ import annotations

from pathlib import Path
import numpy as np
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt



def _load_image(path: Path) -> np.ndarray:
    try:
        from astropy.io import fits
        return np.squeeze(fits.getdata(path))
    except (ImportError, OSError, ValueError):
        # FakeRunner fixtures are NumPy arrays stored under a .fits-like filename.
        with path.open("rb") as fh:
            return np.squeeze(np.load(fh))



def _limits(data: np.ndarray) -> tuple[float, float]:
    finite = data[np.isfinite(data)]
    if finite.size == 0:
        raise ValueError("image contains no finite pixels")
    try:
        from astropy.visualization import ZScaleInterval
        lo, hi = ZScaleInterval().get_limits(finite)
        return float(lo), float(hi)
    except ImportError:
        return float(np.percentile(finite, 1)), float(np.percentile(finite, 99))



def make_preview(image_fits: str | Path, output_png: str | Path) -> Path:
    image_fits = Path(image_fits)
    output_png = Path(output_png)
    data = _load_image(image_fits)
    if data.ndim != 2:
        raise ValueError(f"expected 2D image after squeeze, got {data.shape}")
    lo, hi = _limits(data)
    output_png.parent.mkdir(parents=True, exist_ok=True)
    fig = plt.figure(figsize=(5, 5))
    ax = fig.add_subplot(111)
    ax.imshow(data, origin="lower", vmin=lo, vmax=hi, cmap="gray")
    ax.set_title(image_fits.name)
    ax.set_xlabel("x")
    ax.set_ylabel("y")
    fig.tight_layout()
    fig.savefig(output_png, dpi=120)
    plt.close(fig)
    return output_png
