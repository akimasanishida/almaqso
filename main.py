import sys
sys.path.append('.')
from almaqso import Almaqso


if __name__ == "__main__":
    almaqso = Almaqso(json_filename="./catalog/test_2.json", band=4, work_dir="./test_dir")
    almaqso.run(n_parallel=5)
