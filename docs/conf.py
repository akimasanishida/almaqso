# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
sys.path.append(os.path.abspath('..'))

project = 'ALMAQSO'
copyright = '2024, Akimasa Nishida, Yuki Yoshimura'
author = 'Akimasa Nishida, Yuki Yoshimura'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.autosummary',
    "sphinxcontrib.plantuml",
    "sphinx.ext.imgconverter",
]

napoleon_google_docstring = True
autosummary_generate = True

templates_path = ['_templates']
html_static_path = ['diagrams']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

add_module_names = False

autodoc_member_order = 'bysource'

# Enable figure numbering
numfig = True

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'

latex_documents = [
    ('index', 'manual.tex', 'ALMAQSO Documentation and Reference Manual',
     'Akimasa Nishida, Yuki Yoshimura', 'howto'),
]
latex_elements = {
    "printindex": "",
}
latex_domain_indices = False
