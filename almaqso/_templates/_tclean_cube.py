import analysisUtils as aU

vis = "{vis}"
dir = "{dir}"
weighting = "{weighting}"
robust = float({robust})
savemodel = "{savemodel}"

cell, imsize, _ = aU.pickCellSize(vis, imsize=True, cellstring=True)
fields = aU.getFields(vis)

for field in fields:
    msmd.open(vis)
    spws = msmd.spwsforfield(field)
    msmd.close()
    for spw in spws:
        ms.open(vis)
        ms.reset()
        ret_select = ms.msselect({{'field': str(field), 'spw': str(spw)}})
        num_row = ms.nrow()
        ms.close()
        if (not ret_select) or (num_row == 0):
            continue
        tclean(
            vis=vis,
            imagename=f"{{dir}}/{{field}}_spw{{spw}}_cube",
            deconvolver="hogbom",
            gridder="standard",
            specmode="cube",
            spw=str(spw),
            field=str(field),
            nchan=-1,
            outframe="lsrk",
            veltype="radio",
            weighting=weighting,
            robust=robust,
            cell=str(cell),
            imsize=imsize,
            niter=0,
            pbcor=True,
            interactive=False,
            restoringbeam="common",
            savemodel=savemodel,
        )
