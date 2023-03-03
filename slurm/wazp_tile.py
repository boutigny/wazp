import numpy as np
import yaml, os, sys, json
from astropy.table import join

from lib.multithread import split_survey
from lib.utils import create_directory
from lib.utils import update_data_structure, get_footprint
from lib.wazp import compute_zpslices, bkg_global_survey
from lib.wazp import run_wazp_tile, wazp_concatenate
from lib.wazp import update_config, create_wazp_directories
from lib.wazp import tiles_with_clusters, official_wazp_cat
from lib.pmem import run_pmem_tile, pmem_concatenate_tiles
from lib.pmem import concatenate_calib_dz, eff_tiles_for_pmem

# read config files as online arguments 
config = sys.argv[1]
dconfig = sys.argv[2]
i_thread = sys.argv[3]

# open config files
with open(config) as fstream:
    param_cfg = yaml.safe_load(fstream)
with open(dconfig) as fstream:
    param_data = yaml.safe_load(fstream)


# log message 
print ('WaZP run on survey = ', param_cfg['survey'])
print ('....... ref filter = ', param_cfg['ref_filter'])
print ('Workdir = ', param_cfg['out_paths']['workdir'])

# create directory structure 
workdir = param_cfg['out_paths']['workdir']
create_wazp_directories(workdir)

# create required data structure if not exist and update params
param_data = update_data_structure(param_cfg, param_data)

# update configs (ref_filter, etc.)
param_cfg, param_data = update_config(param_cfg, param_data)

config = os.path.join(workdir, 'config', 'wazp.cfg')    
dconfig = os.path.join(workdir, 'config', 'data.cfg')    

# useful keys 
admin = param_cfg['admin']
wazp_cfg = param_cfg['wazp_cfg']
pmem_cfg = param_cfg['pmem_cfg']
tiles_filename = os.path.join(
    workdir, admin['tiling']['tiles_filename']
)
zpslices_filename = os.path.join(
    workdir, wazp_cfg['zpslices_filename']
)
gbkg_filename = os.path.join(workdir, 'gbkg', wazp_cfg['gbkg_filename'])
cosmo_params = param_cfg['cosmo_params']
survey = param_cfg['survey']
ref_filter = param_cfg['ref_filter']
clusters = param_cfg['clusters']


# read or create global footprint & split survey 
survey_footprint = get_footprint(
    param_data['input_data_structure'][survey], 
    param_data['footprint'][survey], workdir
)
all_tiles = split_survey(
    survey_footprint, param_data['footprint'][survey], 
    admin, tiles_filename
)

# compute zp slicing 
if not os.path.isfile(zpslices_filename):
    compute_zpslices(
        param_data['zp_metrics'][survey][ref_filter], 
        wazp_cfg, -1., zpslices_filename
    )

# compute global bkg ppties 
if not os.path.isfile(gbkg_filename):
    print ('Global bkg computation')
    bkg_global_survey(
        param_data['galcat'][survey], param_data['footprint'][survey], 
        tiles_filename, zpslices_filename, 
        admin['tiling'], cosmo_params, 
        param_data['magstar_file'][survey][ref_filter], 
        wazp_cfg, gbkg_filename)

# detect clusters on one single tile
print ('Run wazp for thread: ', i_thread)
run_wazp_tile(config, dconfig, i_thread)

print ('results in ', workdir)
print (f'Thread {i_thread} processed !')



