# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Paper-ready Matplotlib style.

Properties chosen for print reproducibility:
- Serif fonts (Computer Modern / STIX) matching the LaTeX body font.
- Fixed figure dimensions so reported aspect ratios are consistent.
- A four-colour palette that survives grayscale printing.
- No decorative grids, no 3-D axes, no gradient fills.
"""

from __future__ import annotations

from typing import Any

import matplotlib as mpl


PAPER_PALETTE: tuple[str, ...] = ("#1f77b4", "#d62728", "#2ca02c", "#9467bd")
FIGURE_WIDTH_IN = 5.5
FIGURE_HEIGHT_IN = 3.2


def apply_paper_style() -> None:
    """Mutate the global Matplotlib rcParams in place."""
    params: dict[str, Any] = {
        "font.family": "serif",
        "font.serif": ["STIXGeneral", "DejaVu Serif", "Times New Roman"],
        "mathtext.fontset": "stix",
        "font.size": 10,
        "axes.labelsize": 10,
        "axes.titlesize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
        "axes.grid": False,
        "axes.spines.top": False,
        "axes.spines.right": False,
        "axes.prop_cycle": mpl.cycler(color=PAPER_PALETTE),
        "figure.figsize": (FIGURE_WIDTH_IN, FIGURE_HEIGHT_IN),
        "figure.dpi": 150,
        "savefig.dpi": 300,
        "savefig.bbox": "tight",
        "savefig.format": "pdf",
        "lines.linewidth": 1.4,
        "lines.markersize": 4,
    }
    mpl.rcParams.update(params)
