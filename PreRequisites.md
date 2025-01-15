# Pre-requisites

This section describes how to prepare the required packages.

## analysisUtilites

[analysisUtilites](https://zenodo.org/records/7502160) is a CASA utility package. If you don't have it, please intall the latest.

You may have to modify the code:

- `analysisUtils.py` of analysisUtilities:
    - `np.int32`, `np.int64` and `np.long` -> `int`
    - `np.float`, `np.float32`, `np.float64`, `float32` and `float64` -> `float`
- `almaqa2csg.py` of analysisUtilities:
    - `np.long` -> `int`

## UVMultiFit

[UVMultiFit](https://github.com/onsala-space-observatory/UVMultiFit) is also needed.
Although the installation is guided [here](https://github.com/onsala-space-observatory/UVMultiFit/blob/master/INSTALL.md), the information is a bit old (if your casa version is 5, then this guide is nice).
So I will guide you to install it.

First, clone the repository by following the official guide.
Then, **switch the branch to `develop`** (most versions of casa 6 is only compatible with this branch).

Open `Makefile` and modify one or two lines as follows:

```makefile
CASADIR=/usr/local/casa/casa-6.6.1-17-pipeline-2024.1.0.8  # modify this line
PYTHON=$(CASADIR)/bin/python3
LIBRARYPATH=$(CASADIR)/lib
PYTHONPATH=$(CASADIR)/lib/py/lib/python3.8/site-packages

install:
	$(PYTHON) setup.py install --prefix=/home/akimsans/.local  # modify this line if you want to install it in another directory

user:
	LD_LIBRARY_PATH=$(LIBRARYPATH) PYTHONPATH=$(PYTHONPATH) $(PYTHON) setup.py install --user

clean:
	rm -fvR build
```

**Note**: `CASADIR` is that have `bin`, `lib`, etc. but not `bin` or `casa` itself.

Now, you can install it by

```shell
make install
```

Finally, please tell your CASA where the UVMultiFit is by edditiong `~/.casa/startup.py`:

```python
import sys
sys.path.append('/usr/local/casa/casa-6.6.1-17-pipeline-2024.1.0.8/lib/py/lib/python3.8/site-packages')  # modify as you installed
```
