import almaqa2csg as csg
import analysisUtils as aU
import os
import sys
import numpy as np
import glob
import subprocess


def _run_casa_cmd(casa, cmd):
    try:
        result = subprocess.run(
            [casa, '--nologger', '--nogui', '-c', f'"{cmd}"'],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(f"STDOUT for {cmd}:", result.stdout)
        print(f"STDERR for {cmd}:", result.stderr)
    except subprocess.CalledProcessError as e:
        print(f"Error while executing {cmd}:")
        print(f"Return Code: {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        raise


class QSOanalysis():
    def __init__(self, tarfilename, casacmd='casa', casacmdforuvfit='casa',
                 spacesave=False, workingDir=None):
        self.tarfilename = tarfilename
        self.workingDir = workingDir
        self.spacesave = spacesave
        self.casacmd = casacmd
        self.casacmdforuvfit = casacmdforuvfit

        self.projID = tarfilename.split('_uid___')[0]
        self.asdmname = 'uid___' + \
            (tarfilename.split('_uid___')[1]).replace('.asdm.sdm.tar', '')

        try:
            print(f'analysisUtils of {aU.version()} will be used.')
        except Exception:
            raise Exception('analysisUtils is not found')

    def writelog(self, content=''):
        os.system('mkdir -p log')
        os.system('touch ./log/'+self.asdmname+'.analysis.v2.log')
        os.system('echo "'+content+'" >> '+'./log/' +
                  self.asdmname+'.analysis.v2.log')

    # step0: untar & make working dir
    def intial_proc(self, forcerun=False):
        if self.workingDir is not None:
            os.chdir(self.workingDir)

        if os.path.exists(self.tarfilename):
            os.makedirs(self.asdmname)
            os.system('cp '+self.tarfilename+' '+self.asdmname+'/')
            os.chdir(self.asdmname)
            os.system('tar -xf '+self.tarfilename)

        elif os.path.exists(self.tarfilename+'.gz'):
            os.makedirs(self.asdmname)
            os.system('cp '+self.tarfilename+'.gz '+self.asdmname+'/')
            os.chdir(self.asdmname)
            os.system('gzip -d '+self.tarfilename+'.gz')
            os.system('tar -xvf '+self.tarfilename)

        elif os.path.exists(self.asdmname):
            os.chdir(self.asdmname)

            if os.path.exists(self.tarfilename):
                os.system('tar -xvf '+self.tarfilename)

            elif os.path.exists(self.tarfilename+'.gz'):
                os.system('gzip -d '+self.tarfilename+'.gz')
                os.system('tar -xvf '+self.tarfilename)

        else:
            raise Exception('You may need to download data.')

        self.writelog('step0:OK')

    # step1: importasdm
    def importasdm(self):
        asdmfile = glob.glob(
            f'{os.getcwd()}/{os.path.basename(self.projID)}/*/*/*/raw/*')[0]
        visname = (os.path.basename(asdmfile)).replace('.asdm.sdm', '.ms')

        kw_importasdm = {
            'asdm': asdmfile,
            'vis': visname,
            'asis': 'Antenna Station Receiver Source CalAtmosphere CalWVR CorrelatorMode SBSummary',
            'bdfflags': True,
            'lazy': True,
            'flagbackup': False,
        }

        from casatasks import importasdm
        os.system('rm -rf '+kw_importasdm['vis'])
        importasdm(**kw_importasdm)

        try:
            self.spws = aU.getScienceSpws(vis=visname).split(",")
            os.system('mkdir -p tempfiles')
            np.save('tempfiles/spws.npy', np.array(self.spws))

        except Exception:
            self.spws = np.load('tempfiles/spws.npy')

        self.asdmfile = asdmfile
        self.visname = visname

        self.writelog('step1:OK')

    # step2: generate calib script
    def gen_calib_script(self):
        try:
            refant = aU.commonAntennas(self.visname)
            vis = self.visname
        except Exception:
            refant = aU.commonAntennas(self.visname + '.split')
            vis = self.visname + '.split'

        if os.path.exists(
                f'./log/{self.visname}.scriptForCalibration.py'
            ):
            print('Calibration script already exists.')
            os.system(f'cp ./log/{self.visname}.scriptForCalibration.py ./')
        else:
            print('Calibration script will be generated.')
            csg.generateReducScript(
                    msNames = vis,
                    refant = refant[0],
                    corrAntPos = False,
                    useCalibratorService = False,
                    useLocalAlmaHelper = False
            )
            os.makedirs('./log', exist_ok=True)
            os.system(f'cp {self.visname}.scriptForCalibration.py ./log/')

        self.refant = refant
        self.dish_diameter = aU.almaAntennaDiameter(refant[0])

        self.writelog('step2:OK')

    # step3: remove TARGET observations
    def remove_target(self):
        IntentListASDM = aU.getIntentsFromASDM(self.asdmfile)

        IntentList = []
        for intfield in list(IntentListASDM):
            IntentList = IntentList + IntentListASDM[intfield]

        listOfIntents_init = (np.unique(IntentList)[
                              np.unique(IntentList) != 'OBSERVE_TARGET'])

        os.system('rm -rf '+self.visname+'.org')
        os.system('mv '+self.visname+' '+self.visname+'.org')
        kw_mstransform = {
            'vis': self.visname+'.org',
            'outputvis': self.visname,
            'datacolumn': 'all',
            'intent': '*,'.join(listOfIntents_init)+'*',
            'keepflags': True
        }

        from casatasks import mstransform
        os.system('rm -rf '+kw_mstransform['outputvis'])
        os.system('rm -rf '+kw_mstransform['outputvis']+'.flagversions')
        mstransform(**kw_mstransform)

        self.writelog('step3:OK')

    # step4: do Calibration
    def doCalib(self):
        cmdfile = self.visname + '.scriptForCalibration.py'

        checksteps = open(cmdfile, 'r')
        syscalcheck = checksteps.readlines().copy()[21]
        checksteps.close()

        f = open(cmdfile.replace('.py', '.part.py'), 'w')
        if syscalcheck.split(':')[1].split("'")[1] == \
                'Application of the bandpass and gain cal tables':
            f.write(
                'mysteps = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]'+'\n')
        else:
            f.write(
                'mysteps = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17]'+'\n')
        f.write('applyonly = True'+'\n')
        f.write('execfile('+'"'+cmdfile+'"'+',globals())'+'\n')
        f.close()

        cmd = '"' + \
            'execfile('+"'"+cmdfile.replace('.py',
                                            '.part.py')+"'"+')' + '"'
        os.system(self.casacmd+' --nologger --nogui -c '+cmd)

        vis = self.visname + '.split'

        fields = np.unique(aU.getCalibrators(vis=vis))
        self.fields = []

        for field in fields:
            if field[0] == 'J':
                self.fields.append(field)

        self.beamsize = aU.estimateSynthesizedBeam(vis)
        from casatools import synthesisutils
        su = synthesisutils()

        if self.dish_diameter > 10.:
            # 12m
            self.imsize = su.getOptimumSize(int(120./self.beamsize*5))
        else:
            # 7m
            self.imsize = su.getOptimumSize(int(180./self.beamsize*5))

        self.cell = '{:.3f}'.format(self.beamsize/5) + 'arcsec'

        self.writelog('step4:OK')
