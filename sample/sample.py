from almaqso import Almaqso

if __name__ == "__main__":
    # First create the instance of Almaqso
    almaqso = Almaqso(
        json_filename="./catalog/test_2.json",  # Path to the JSON file
        band=4,  # Band number
        work_dir="./test_dir",  # which directory to work in
        casapath="/usr/local/casa/casa-6.6.1-17-pipeline-2024.1.0.8/bin/casa",  # Path to the CASA executable
    )
    # Then just run the analysis. This will download and analyze the data.
    almaqso.process(
        n_parallel=2,  # Number of parallel processes to run (In MDAS, you should smaller than 2 in mana00 or 16 in mana01-07)
        do_tclean=True,  # Whether to run tclean
        kw_tclean={"specmode": "mfs"},  # Keyword arguments for tclean. specmode is required. weighting, robust are optional.
        do_selfcal=True,  # Whether to run selfcal
        kw_selfcal={},  # Keyword arguments for selfcal. specmode will be overwritten by the one in tclean.
        do_export_fits=True,  # Whether to export the data to FITS format
        remove_asdm=True,  # Whether to remove the `*.asdm.sdm.tar` files after processing
        remove_intermediate=True,  # Whether to remove the intermediate files after processing. image files, fits files, measurement set, log files, and script files will be retained.
    )
