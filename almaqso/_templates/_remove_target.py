import analysisUtils as aU


fields = aU.getFields("{vis}")
fields_target = aU.getTargetsForIntent("{vis}")
fields_cal = list(set(fields) - set(fields_target))
print(fields_cal)

kw_split = {{
    "vis": "{vis}",
    "outputvis": "{vis}.split",
    "field": ", ".join(fields_cal),
    "datacolumn": "all",
}}

mstransform(**kw_split)

listobs(vis=kw_split["outputvis"], listfile=kw_split["outputvis"] + ".listobs")
