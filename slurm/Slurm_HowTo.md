# How to use the slurm version of WaZP 
*The author of WaZp is Christophe Benoist from OCA* 

In the slurm version of WaZP the main python file wazp_main.py has been split in 5 scripts corresponding to the different steps of the execution:

- `wazp_main.py` is the main script working exactly in the same way as the original one. The path name of the configuration files `wazp.cfg` and data.cfg 
should be passed as arguments. 

	Typically: 

```
		python wazp_main.py wazp.cfg data.cfg
```

- `wazp_tile.py` is processing the tiles
- `wazp_tile_trailer` is running once all the tiles have been processed
- `wazp_pmem.py` is associating galaxy members to the clusters
- `wazp_pmem_trailer.py` is running once all pmems have peen processed

## Workflow

`wazp_main.py` will submit N slurm jobs in parrallel for the "tile" and "pmem" steps. The number of jobs is controlled by the nthreads_max parameter in wazp.cfg
`wazp_main.py` is run interactively or in batch, it will handle all the initialization phase which is required to run the other steps. The result of the initialization step is saved in the output directory (controlled by the workdir parameter in wazp.cfg) and is automatically reused if wazp_main.py is rerun.

Once the initialization phase is complete, the slurm jobs are submitted (N jobs for wazp_tile.py, 1 job for wazp_tile_trailer.py, N jobs for wazp_pmem.py and 1 job for `wazp_pmem_trailer.py`). The various steps are synchronized using the `--dependency=afterany` slurm option

A `slurm_output` directory is created in the workdir to receive the slurm excution log files.

## Initialization

Before launching wazp_main for the first time, one needs to create a few links in the slurm directory:

```
	cd slurm
	ln -s ../data.cfg
	ln -s ../wazp.cfg
	ln -s ../aux
	ln -s ../lib
```
One also have to create the input_data directory which contain the galaxies and footprint catalogs (slit according to the choosen Healpix scheme)

Finally the script also expects a `setup.sh` script in the `slurm` directory in order to initialize the conda environment within the batch jobs.
The following is an example:

```
	source /pbs/throng/lsst/software/desc/common/miniconda/setup_current_python.sh
	conda activate wazp
```
