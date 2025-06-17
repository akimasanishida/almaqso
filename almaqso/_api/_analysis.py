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
        fits_files = glob("dirty_fits/*.fits")

        # Create the output directory
        os.makedirs(self._dir_plot, exist_ok=True)

        for fits_file in fits_files:
            with fits.open(fits_file) as hdul:
                # Get the beam size in pixels
                data_header = hdul[0].header
                CDELT = abs(data_header["CDELT1"])
                cell_size = CDELT * 3600  # arcsec
                beam_size = hdul[1].data[0][0]  # arcsec
                beam_px = round(beam_size / cell_size)  # px
                # print(f"Cell size: {cell_size} arcsec")
                # print(f"Beam size: {beam_size} arcsec")
                # print(f"Beam size in pixels: {beam_px} px")
                BMAJ = hdul[1].data[0][0]  # arcsec
                BMIN = hdul[1].data[0][1]  # arcsec

                # Get the center pixel of the image
                x_center = data_header["CRPIX1"]
                y_center = data_header["CRPIX2"]

                # Extract the region around the peak of the image with the beam size
                data_extract = hdul[0].data[
                    0,
                    :,
                    int(y_center - beam_px / 2) : int(y_center + beam_px / 2),
                    int(x_center - beam_px / 2) : int(x_center + beam_px / 2),
                ]

                # Calculate the total flux density
                beam_area = (np.pi / (4 * np.log(2))) * BMAJ * BMIN
                total_flux_intensity = (
                    np.sum(data_extract, axis=(1, 2)) * cell_size**2 / beam_area
                )

                # Frequency axis
                CRVAL3 = data_header["CRVAL3"]
                CRPIX3 = data_header["CRPIX3"]
                CDELT3 = data_header["CDELT3"]

            freqs = (
                CRVAL3 + (np.arange(data_extract.shape[0]) - (CRPIX3 - 1)) * CDELT3
            ) / 1e9  # in GHz

            fits_name = os.path.basename(fits_file)
            self._frequencies[fits_name] = freqs
            self._spectrums[fits_name] = total_flux_intensity
    
    def plot_spectrum(self):
        """
        Plot the spectrum of the target.
        """
        for fits_name, spectrum in self._spectrums.items():
                freqs = self._frequencies[fits_name]

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
                ax.set_ylabel("Integrated flux (Jy)")
                ax.set_ylim(y_min_lim, y_max_lim)
                ax.set_title(f"Spectrum from {fits_name}")
                ax.grid()
                fig.tight_layout()
                fig.savefig(os.path.join(self._dir_plot, f"{fits_name}_spectrum.png"), dpi=300)

    def calc_optical_depth(self):
        """
        Calculate the optical depth from the spectrum data.
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
                    raise RuntimeError(f"Failed to calculate the base intensity for {fits_name}.")
                
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
            fig.savefig(os.path.join(self._dir_plot, f"{fits_name}_optical_depth.png"), dpi=300)
            plt.close(fig)