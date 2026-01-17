import analysisUtils as aU


fields = {retain_fields}  # Target fields
fields_science = aU.getTargetsForIntent("{vis}")  # Science target fields
fields_selected = [f for f in fields if f not in fields_science]  # Remove science targets

kw_split = {{
    "vis": "{vis}",
    "outputvis": "{vis}.split",
    "field": ", ".join(fields_selected),
    "datacolumn": "all",
}}

mstransform(**kw_split)

listobs(vis=kw_split["outputvis"], listfile=kw_split["outputvis"] + ".listobs")
