"""Sphinx configuration for despina Python documentation."""

from __future__ import annotations

from datetime import datetime

project = "despina"
author = "Michael Byrne"
copyright = f"{datetime.now().year}, {author}"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "numpydoc",
    "sphinx_copybutton",
]

autosummary_generate = True
autodoc_default_options = {
    "members": True,
    "member-order": "bysource",
    "undoc-members": False,
}
autodoc_typehints = "description"
numpydoc_show_class_members = False

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable", None),
}

html_theme = "sphinx_rtd_theme"
html_title = "despina Python API"
html_theme_options = {
    "collapse_navigation": False,
    "navigation_depth": 4,
}
