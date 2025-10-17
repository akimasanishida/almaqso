import sys

sys.path.append(".")
from almaqso import Almaqso

if __name__ == "__main__":
    almaqso = Almaqso(
        target=["J2000-1748"],
        band=7,
        cycle="4,6~10",
        work_dir="./test_dir",
        casapath="/usr/local/casa/casa-6.6.6-17-pipeline-2025.1.0.35-py3.10.el8/bin/casa"
    )
    almaqso.process(
        n_parallel=2,
        do_tclean=True,
        tclean_mode=["mfs", "mfs_spw", "cube"],
        # do_selfcal=True,
        # kw_selfcal={},
        do_export_fits=True,
        remove_asdm=True,
        remove_intermediate=True,
    )
    # almaqso.analysis()
