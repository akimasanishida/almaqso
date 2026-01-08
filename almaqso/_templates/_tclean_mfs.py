import analysisUtils as aU

vis = "{vis}"
dir = "{dir}"
weighting = "{weighting}"
robust = float({robust})
savemodel = "{savemodel}"

cell, imsize, _ = aU.pickCellSize(vis, imsize=True, cellstring=True)
fields = aU.getFields(vis)

for field in fields:
    ms.open(vis)
    ret_select = ms.msselect({{'field': str(field)}})
    ms.close()
    if not ret_select:
        continue
    tclean(
        vis=vis,
        imagename=f"{{dir}}/{{field}}_mfs",
        deconvolver="hogbom",
        gridder="standard",
        specmode="mfs",
        field=str(field),
        weighting=weighting,
        robust=robust,
        cell=str(cell),
        imsize=imsize,
        niter=0,
        pbcor=True,
        interactive=False,
        savemodel=savemodel,
    )
