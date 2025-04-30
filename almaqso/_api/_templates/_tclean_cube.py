import analysisUtils as aU

cell, imsize, _ = aU.pickCellSize("{vis}", imsize=True, cellstring=True)
fields = aU.getFields("{vis}")

for field in fields:
    msmd.open("{vis}")
    spws = msmd.spwsforfield(field)
    msmd.close()
    for spw in spws:
        tclean(
            vis="{vis}",
            imagename=f"{dir}/{{field}}_spw{{spw}}_cube",
            deconvolver="hogbom",
            gridder="standard",
            specmode="cube",
            spw=str(spw),
            field=str(field),
            nchan=-1,
            outframe="lsrk",
            veltype="radio",
            weighting="{weighting}",
            robust={robust},
            cell=str(cell),
            imsize=imsize,
            niter=0,
            pbcor=True,
            interactive=False,
            savemodel="{savemodel}",
        )
