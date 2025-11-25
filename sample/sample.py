# Use the two lines below if you did not install almaqso via package manager like pip:
# import sys
# sys.path.append("path/to/almaqso")  # Where "README.md" is located. NOT "almaqso/almaqso" folder.

from almaqso import Almaqso

if __name__ == "__main__":
    # First create the instance of Almaqso
    almaqso = Almaqso(
        target=["J2000-1748"],  # Target name
        band=4,  # Band number
        cycle="",  # Cycle number. If not specified, the latest cycle will be used.
        work_dir="./J2000-1748_band4",  # which directory to work in
        casapath="/usr/local/casa/casa-6.6.6-17-pipeline-2025.1.0.35-py3.10.el8/bin/casa"  # Path to the CASA executable
    )
    # Then just run the analysis. This will download and analyze the data.
    almaqso.process(
        n_parallel=2,  # Number of parallel processes to run (In MDAS, you should smaller than 2 in mana00 or 16 in mana01-07)
        do_tclean=True,  # Whether to run tclean
        tclean_mode=["mfs", "mfs_spw", "cube"],  # tclean modes to run. Please choose from "mfs", "mfs_spw", and "cube".
        # do_selfcal=True,  # Whether to run selfcal. IMPORTANT: Selfcal does not work well now.
        # kw_selfcal={},  # Keyword arguments for selfcal. IMPORTANT: This argument will be modified in the future.
        do_export_fits=True,  # Whether to export the image to FITS format
        remove_asdm=True,  # Whether to remove the `*.asdm.sdm.tar` files after processing
        remove_intermediate=True,  # Whether to remove the intermediate files after processing. image files, fits files, measurement set, log files, and script files will be retained.
    )
    almaqso.analysis()  # Run the analysis on the processed data