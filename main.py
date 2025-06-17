import sys

sys.path.append(".")
from almaqso import Almaqso

if __name__ == "__main__":
    almaqso = Almaqso(
        # json_filename="./catalog/test_2.json",
        target=["J1832-1035"],
        band=4,
        work_dir="./test_dir",
        casapath="/usr/local/casa/casa-6.6.1-17-pipeline-2024.1.0.8/bin/casa",
    )
    # almaqso.process(
    #     n_parallel=8,
    #     do_tclean=True,
    #     kw_tclean={"specmode": "cube"},
    #     # do_selfcal=True,
    #     # kw_selfcal={},
    #     do_export_fits=True,
    #     remove_asdm=True,
    #     remove_intermediate=True,
    # )
    almaqso.analysis()