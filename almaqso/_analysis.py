from pathlib import Path
from astropy.io import fits
import numpy as np
import csv
import matplotlib.pyplot as plt

from ._utils import in_source_list

SPECTRUM_DIR = "spectrum"


def _get_spectrum(fits_file: Path) -> tuple[np.ndarray, np.ndarray]:
    """
    Extract frequency and spectrum data from a cube FITS file.

    Args:
        fits_file (Path): Path to the cube FITS file.

    Returns:
        tuple[np.ndarray, np.ndarray]: Frequencies (GHz) and corresponding spectrum (Jy).
    """
    with fits.open(fits_file) as hdul:
        data_header = hdul[0].header  # pyright: ignore[reportAttributeAccessIssue]
        data = hdul[0].data  # pyright: ignore[reportAttributeAccessIssue]
        data = np.squeeze(data)  # (stokes, chan, y, x) -> (chan, y, x)

        # Frequency axis
        CRVAL3 = data_header["CRVAL3"]
        CRPIX3 = data_header["CRPIX3"]
        CDELT3 = data_header["CDELT3"]

    frequencies = (
        CRVAL3 + (np.arange(data.shape[0]) - (CRPIX3 - 1)) * CDELT3
    ) / 1e9  # in GHz

    spectrums = np.nanmax(data, axis=(1, 2))  # Jy

    return frequencies, spectrums


def _plot_spectrum(
    frequencies: np.ndarray, spectrums: np.ndarray, fits_name: str, output_png: Path
) -> None:
    """
    Plot the spectrum of the target.

    Args:
        frequencies (np.ndarray): Frequencies (GHz).
        spectrums (np.ndarray): Corresponding spectrum (Jy).
        fits_name (str): Name of the FITS file.
        output_png (Path): Path to save the output PNG file.

    Returns:
        None
    """
    # index that values are 0 will be removed
    mask = spectrums > 0
    frequencies = frequencies[mask]
    spectrums = spectrums[mask]

    # Calculate y-axis limits based on the standard deviation
    y_mean = np.mean(spectrums)
    y_std = np.std(spectrums)
    y_min = y_mean - 5 * y_std
    y_max = y_mean + 5 * y_std

    # Get the minimum larger than y_min and maximum smaller than y_max
    y_min_data = np.min(spectrums[spectrums > y_min])
    y_max_data = np.max(spectrums[spectrums < y_max])

    y_min_lim = y_mean - (y_mean - y_min_data) * 1.2
    y_max_lim = y_mean + (y_max_data - y_mean) * 1.2

    # Plot the spectrum
    fig, ax = plt.subplots()
    ax.plot(frequencies, spectrums)
    ax.set_xlabel("Frequency (GHz)")
    ax.set_ylabel("Flux (Jy)")
    ax.set_ylim(y_min_lim, y_max_lim)
    ax.set_title(f"Spectrum from {fits_name}")
    ax.grid()
    fig.tight_layout()
    fig.savefig(output_png, dpi=300)


def _write_spectrum_csv(
    frequencies: np.ndarray, spectrums: np.ndarray, output_csv: Path
) -> None:
    """
    Write the spectrum data to CSV files.

    Args:
        frequencies (np.ndarray): Frequencies (GHz).
        spectrums (np.ndarray): Corresponding spectrum (Jy).
        output_csv (Path): Path to save the output CSV file.

    Returns:
        None
    """
    with open(output_csv, mode="w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Frequency (GHz)", "Flux (Jy)"])
        for freq, flux in zip(frequencies, spectrums):
            writer.writerow([freq, flux])


def calc_spectrum(working_dir: Path, sources: list[str]):
    """
    Calculate the spectrum of the target sources from the cube FITS image.

    Args:
        working_dir (Path): The working directory containing source subdirectories.
        sources (list[str]): List of source names to process.

    Returns:
        None
    """
    # Search for cube FITS files
    fits_dir = working_dir / "fits"
    source_dirs = [d for d in fits_dir.iterdir() if d.is_dir()]
    for source in source_dirs:
        if not in_source_list(source.name, sources):
            continue
        fits_files = list(source.glob("*_cube.fits"))
        # Create the output directory
        spectrum_dir = working_dir / SPECTRUM_DIR / source.name
        spectrum_dir.mkdir(exist_ok=True, parents=True)

        for fits_file in fits_files:
            frequencies, spectrums = _get_spectrum(fits_file)
            fits_name = fits_file.name
            # Plot the spectrum
            output_png = spectrum_dir / f"{fits_name.replace('.fits', '_spectrum.png')}"
            _plot_spectrum(frequencies, spectrums, fits_name, output_png)
            # Write the spectrum to CSV
            output_csv = spectrum_dir / f"{fits_name.replace('.fits', '_spectrum.csv')}"
            _write_spectrum_csv(frequencies, spectrums, output_csv)
