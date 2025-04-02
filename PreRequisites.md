# Pre-requisites

This section describes how to prepare the required packages.

## analysisUtilites

[analysisUtilites](https://zenodo.org/records/13887809) is a CASA utility package.
If you don't have it, please intall the latest.
How to install is explained [here](https://casaguides.nrao.edu/index.php/Analysis_Utilities).

You have to modify the code to run almaqso correctly:

- `analysisUtils.py` of analysisUtilities:
    - `np.int32`, `np.int64` and `np.long` -> `int`
    - `np.float`, `np.float32`, `np.float64`, `float32` and `float64` -> `float`
- `almaqa2csg.py` of analysisUtilities:
    - `np.long` -> `int`

### `pickCellSize` function

Convert the type of `meanfreq` from `numpy.floatXX` to `float`:

**Before**

```python
    if (verbose):
        print("mean frequency = ", meanfreq)
    cellsize = printBaselineAngularScale(baselineStats,meanfreq*1e-9,verbose=verbose) / npix
```

**After**

```python
    if (verbose):
        print("mean frequency = ", meanfreq)
    meanfreq = float(meanfreq)  # Add this line!
    cellsize = printBaselineAngularScale(baselineStats,meanfreq*1e-9,verbose=verbose) / npix
```

### `centralObstructionFactor` function

Convert the type of `factor` from `numpy.floatXX` to `float`:

**Before**

```python
    epsilon = obscuration/float(diameter)
    myspline = scipy.interpolate.UnivariateSpline([0, 0.1, 0.2, 0.33, 0.4], [1.22, 1.205, 1.167, 1.098, 1.058], s=s)
    factor = myspline(epsilon)/1.22
```

**After**

```python
    epsilon = obscuration/float(diameter)
    myspline = scipy.interpolate.UnivariateSpline([0, 0.1, 0.2, 0.33, 0.4], [1.22, 1.205, 1.167, 1.098, 1.058], s=s)
    factor = float(myspline(epsilon)/1.22)  # Mod the right value as float(...)
```
