Basic usage
=================

Sample code to

- download J2000-1748 data in Band 4 of all cycles from ALMA archive,
- calibrate the data, and
- create MFS and Cube images using tclean of CASA

is shown below:

.. code-block:: python
    
    import sys
    from almaqso import Almaqso

    if __name__ == "__main__":
        with Almaqso(
            target="J2000-1748",
            band=4,
            cycle="",
            work_dir="your_work_dir/",
            casapath="/usr/local/casa/casa-6.6.6-17-pipeline-2025.1.0.35-py3.10.el8/bin/casa"
        ) as almaqso:
            almaqso.process(
                n_parallel=2,
                do_tclean=True,
                tclean_mode=["mfs", "cube"],
            )

:py:meth:`almaqso.Almaqso.process` has other useful options to control the processing and data storing.