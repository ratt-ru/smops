import os
import click

from dataclasses import dataclass
from typing import Dict

from omegaconf import OmegaConf
from scabha.cargo import Parameter
from scabha import configuratt
from scabha.schema_utils import clickify_parameters

from smops import VERSION

cur_dir = os.path.dirname(__file__)
schema_name = os.path.join(cur_dir, "schema.yml")

def setup_for_clickify(schema_name):
    """
    Guess who's coming to dinner 

    Prepare schema in scabha terms for clickfy.
    Refer to: scabha2/scabha/schema_utils.py
    """
    @dataclass
    class Taboo:
        inputs: Dict[str, Parameter]
        outputs: Dict[str, Parameter]

    arg_struct = OmegaConf.structured(Taboo)
    nested_args = configuratt.load_nested([schema_name], structured=arg_struct)
    schema = OmegaConf.create(nested_args[0]).schema
    return schema

@click.command()
@click.version_option(VERSION, "-v", "--version", is_flag=True)
@clickify_parameters(
    setup_for_clickify(schema_name)
)
@click.pass_context
def get_arguments(*args, **kwargs):
    arg_name_map = {
        'ms': "ms_name",
        'polynomial_order': "poly_order",
        'num_threads': "nthreads",
        'output_prefix': "output_pref",
        }
    for old_key, new_key in arg_name_map.items():
        kwargs[new_key] = kwargs.pop(old_key)    
    opts = OmegaConf.create(kwargs)
    return opts
    