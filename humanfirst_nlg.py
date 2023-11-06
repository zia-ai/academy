#!/usr/bin/env python # pylint: disable=missing-module-docstring
# -*- coding: utf-8 -*-
# ******************************************************************************************************************120
#
# Examples of NLG
#
# *********************************************************************************************************************

# standard imports
import re

def get_nlg_tag_regex(tag_name: str) -> re:
    """Returns NLG tags REGEX"""

    hf_tag_list = ["conversation","text"]

    assert tag_name in hf_tag_list

    if tag_name == "conversation":
        return re.compile(r"{{[ ]*conversation[ ]*}}")
    elif tag_name == "text":
        return re.compile(r"{{[ ]*text[ ]*}}")
    else:
        raise KeyError(tag_name)
