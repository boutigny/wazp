import numpy as np
import yaml, os, sys, json
from astropy.table import join

from lib.multithread import split_equal_area_in_threads
from lib.utils import hpx_split_survey, read_FitsCat
from lib.utils import create_mosaic_footprint
from lib.utils import create_directory, add_key_to_fits
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

# update param_data 
# update config (ref_filter, etc.)
param_cfg, param_data = update_config(param_cfg, param_data)

# store config file in workdir - Not needed and even harmful in the slurm version
#with open(
#        os.path.join(workdir, 'config', 'wazp.cfg'), 'w'
#) as outfile:
#    json.dump(param_cfg, outfile)
config = os.path.join(workdir, 'config', 'wazp.cfg')    
#with open(
#        os.path.join(workdir, 'config', 'data.cfg'), 'w'
#) as outfile:
#    json.dump(param_data, outfile)
dconfig = os.path.join(workdir, 'config', 'data.cfg')      

# useful keys 
admin = param_cfg['admin']
wazp_cfg = param_cfg['wazp_cfg']
pmem_cfg = param_cfg['pmem_cfg']
tiles_filename = os.path.join(
    workdir, admin['tiling']['tiles_filename']
)
tiles_hpix_filename = os.path.join(
    workdir, admin['tiling']['tiles_hpix_filename']
)
zpslices_filename = os.path.join(
    workdir, wazp_cfg['zpslices_filename']
)
gbkg_filename = os.path.join(workdir, 'gbkg', wazp_cfg['gbkg_filename'])
cosmo_params = param_cfg['cosmo_params']
survey = param_cfg['survey']
ref_filter = param_cfg['ref_filter']
clusters = param_cfg['clusters']

# split_area:
if not os.path.isfile(tiles_filename):
    ntiles = hpx_split_survey(
        param_data['footprint'][survey], admin['tiling'],
        tiles_hpix_filename, tiles_filename
    )
    n_threads, thread_ids = split_equal_area_in_threads(
        admin['nthreads_max'], 
        tiles_filename
    )
    add_key_to_fits(tiles_filename, thread_ids, 'thread_id', 'int')
    all_tiles = read_FitsCat(tiles_filename)
else:
    all_tiles = read_FitsCat(tiles_filename)
    ntiles, n_threads = len(all_tiles), np.amax(all_tiles['thread_id']) 
    thread_ids = all_tiles['thread_id']
print ('Ntiles / Nthreads = ', ntiles, ' / ', n_threads)

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
print ('Run pmem for thread: ', i_thread)
run_pmem_tile(config, dconfig, i_thread)

print ('results in ', workdir)
print (f'Thread {i_thread} pmem processed !')