import analysisUtils as aU


fields = {retain_fields}

kw_split = {{
    "vis": "{vis}",
    "outputvis": "{vis}.split",
    "field": ", ".join(fields),
    "datacolumn": "all",
}}

mstransform(**kw_split)

listobs(vis=kw_split["outputvis"], listfile=kw_split["outputvis"] + ".listobs")
