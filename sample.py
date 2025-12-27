import sys

sys.path.append(".")
from almaqso import Almaqso

if __name__ == "__main__":
    almaqso = Almaqso(
        target=["J2000-1748"],
        band=4,
        cycle="",
        work_dir="./your_dirctory/",
        casapath="/usr/local/casa/casa-6.6.6-17-pipeline-2025.1.0.35-py3.10.el8/bin/casa",
    )
    almaqso.process(
        n_parallel=2,
        skip_previous_successful=True,
        do_tclean=True,
        tclean_mode=["mfs", "mfs_spw", "cube"],
        do_export_fits=True,
        remove_casa_images=True,
        remove_asdm=True,
        remove_intermediate=True,
    )
