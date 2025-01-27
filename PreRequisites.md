# Pre-requisites

This section describes how to prepare the required packages.

## analysisUtilites

[analysisUtilites](https://zenodo.org/records/7502160) is a CASA utility package.
If you don't have it, please intall the latest.
How to install is explained [here](https://casaguides.nrao.edu/index.php/Analysis_Utilities).

You have to modify the code to run almaqso correctly:

- `analysisUtils.py` of analysisUtilities:
    - `np.int32`, `np.int64` and `np.long` -> `int`
    - `np.float`, `np.float32`, `np.float64`, `float32` and `float64` -> `float`
- `almaqa2csg.py` of analysisUtilities:
    - `np.long` -> `int`
