import sys

sys.path.append(".")
from almaqso import Almaqso

if __name__ == "__main__":
    with Almaqso(
        # target=["J0211+1051"],
        # band=3,
        # cycle="",
        # work_dir="./test_dir_J0211+1051_band3/",
        target="J2000-1748",
        band=4,
        work_dir="./test_dir_J2000-1748_band4/",
        # target=["J1832-1035"],
        # band=6,
        # cycle="8",
        # work_dir="./test_dir_J1832-1035_band6_cycle8/",
        casapath="/usr/local/casa/casa-6.6.6-17-pipeline-2025.1.0.35-py3.10.el8/bin/casa"
    ) as almaqso:
        almaqso.process(
            n_parallel=2,
            skip_previous_successful=True,
            do_tclean=True,
            tclean_mode=["mfs", "mfs_spw", "cube"],
            # do_selfcal=True,
            # kw_selfcal={},
            do_export_fits=True,
            remove_casa_images=True,
            remove_asdm=True,
            remove_intermediate=True,
        )
        almaqso.sort_images(keep_original=False, remove_non_target=True)
        almaqso.analysis_calc_spectrum()
