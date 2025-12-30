from glob import glob
from astropy.io import fits
import numpy as np
import os
import csv
import matplotlib.pyplot as plt


class Analysis:
    def __init__(self):
        self._dir_plot = "plots"
        self._frequencies = {}
        self._spectrums = {}
        self._optical_depths = {}

    def get_spectrum(self):
        """
        Get the spectrum of the target from the cube FITS image.
        """
        # Search "dirty_fits/*.fits" files
        fits_files = glob("dirty_fits/*_cube.fits")

        # Create the output directory
        os.makedirs(self._dir_plot, exist_ok=True)

        for fits_file in fits_files:
            with fits.open(fits_file) as hdul:
                # Get the beam size in pixels
                data_header = hdul[0].header
                data = hdul[0].data
                data = np.squeeze(data)  # (stokes, chan, y, x) -> (chan, y, x)

                # Frequency axis
                CRVAL3 = data_header["CRVAL3"]
                CRPIX3 = data_header["CRPIX3"]
                CDELT3 = data_header["CDELT3"]

            freqs = (
                CRVAL3 + (np.arange(data.shape[0]) - (CRPIX3 - 1)) * CDELT3
            ) / 1e9  # in GHz

            fits_name = os.path.basename(fits_file)
            self._frequencies[fits_name] = freqs
            self._spectrums[fits_name] = np.nanmax(data, axis=(1, 2))  # Jy
            # print(freqs)
            # print(data)

    def plot_spectrum(self):
        """
        Plot the spectrum of the target.
        """
        for fits_name, spectrum in self._spectrums.items():
            freqs = self._frequencies[fits_name]

            # index that values are 0 will be removed
            mask = spectrum > 0
            freqs = freqs[mask]
            spectrum = spectrum[mask]

            # Calculate y-axis limits based on the standard deviation
            y_mean = np.mean(spectrum)
            y_std = np.std(spectrum)
            y_min = y_mean - 5 * y_std
            y_max = y_mean + 5 * y_std

            # Get the minimun larger than y_min and maximum smaller than y_max
            y_min_data = np.min(spectrum[spectrum > y_min])
            y_max_data = np.max(spectrum[spectrum < y_max])

            y_min_lim = y_mean - (y_mean - y_min_data) * 1.2
            y_max_lim = y_mean + (y_max_data - y_mean) * 1.2

            # Plot the spectrum
            fig, ax = plt.subplots()
            ax.plot(freqs, spectrum)
            ax.set_xlabel("Frequency (GHz)")
            ax.set_ylabel("Flux (Jy)")
            ax.set_ylim(y_min_lim, y_max_lim)
            ax.set_title(f"Spectrum from {fits_name}")
            ax.grid()
            fig.tight_layout()
            fig.savefig(
                os.path.join(self._dir_plot, f"{fits_name}_spectrum.png"), dpi=300
            )

    def write_spectrum_csv(self):
        """
        Write the spectrum data to CSV files.
        """
        for fits_name, spectrum in self._spectrums.items():
            freqs = self._frequencies[fits_name]

            csv_name = os.path.join(
                self._dir_plot, f"{fits_name.replace('.fits', '')}_spectrum.csv"
            )
            with open(csv_name, mode="w", newline="") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(["Frequency (GHz)", "Flux (Jy)"])
                for freq, flux in zip(freqs, spectrum):
                    writer.writerow([freq, flux])

    def calc_optical_depth(self):
        """
        Calculate the optical depth from the spectrum data.
        TODO: Need to correct! The method is wrong.
        """
        for fits_name, spectrum in self._spectrums.items():
            # Calculate the base intensity
            intensity_base_prev = np.mean(spectrum)
            intensity_std_prev = np.std(spectrum)
            for i in range(100):
                # Eliminate the outliers (3 sigma)
                mask = np.abs(spectrum - intensity_base_prev) < 3 * intensity_std_prev
                filtered_intensity = spectrum[mask]

                # Calculate the mean and standard deviation
                intensity_base = np.mean(filtered_intensity)
                intensity_std = np.std(filtered_intensity)

                if abs(intensity_base_prev - intensity_base) < 1e-6 and i > 5:
                    break

                if i == 99:
                    raise RuntimeError(
                        f"Failed to calculate the base intensity for {fits_name}."
                    )

                intensity_base_prev = intensity_base
                intensity_std_prev = intensity_std

            # Calculate the optical depth
            optical_depth = -np.log(spectrum / intensity_base)

            self._optical_depths[fits_name] = optical_depth

    def plot_optical_depth(self):
        """
        Plot the optical depth of the target.
        """
        for fits_name, optical_depth in self._optical_depths.items():
            freqs = self._frequencies[fits_name]

            # Plot the optical depth
            fig, ax = plt.subplots()
            ax.plot(freqs, optical_depth)
            ax.set_xlabel("Frequency (GHz)")
            ax.set_ylabel("Optical Depth")
            ax.set_title(f"Optical Depth from {fits_name}")
            ax.grid()
            fig.tight_layout()
            fig.savefig(
                os.path.join(self._dir_plot, f"{fits_name}_optical_depth.png"), dpi=300
            )
            plt.close(fig)