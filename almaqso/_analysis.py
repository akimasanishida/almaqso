import os
from .QSOanalysis import QSOanalysis


def _analysis(tarfilename: str, skip=True, casacmd='casa'):
    obj = QSOanalysis(tarfilename, spacesave=True, casacmd=casacmd, casacmdforuvfit='casa')

    asdmname = 'uid___' + (tarfilename.split('_uid___')
                           [1]).replace('.asdm.sdm.tar', '')

    if os.path.exists(asdmname) and skip:
        print(asdmname+': analysis already done and skip')

    else:
        if os.path.exists(asdmname):
            print(asdmname+': analysis already done but reanalyzed')

        print('step:0')
        obj.intial_proc()
        print('step:1')
        obj.importasdm()
        print('step:2')
        obj.gen_calib_script()
        print('step:3')
        obj.remove_target()
        print('step:4')
        obj.doCalib()
