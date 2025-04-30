import sys

sys.path.append(".")
from almaqso import Almaqso

if __name__ == "__main__":
    almaqso = Almaqso(
        json_filename="./catalog/test_2.json",
        band=4,
        work_dir="./test_dir",
        casapath="/usr/local/casa/casa-6.6.1-17-pipeline-2024.1.0.8/bin/casa",
    )
    almaqso.run(
        n_parallel=2,
        do_tclean=True,
        kw_tclean={"specmode": "mfs"},
        do_selfcal=False,
        kw_selfcal={},
        do_export_fits=True,
        remove_asdm=True,
        remove_intermediate=True,
    )
