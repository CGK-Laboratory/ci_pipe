# CIPipeline

## Description
CIPipeline (imported as `ci_pipe`) is a Python library for building and running calcium-imaging processing pipelines. It provides core pipeline primitives, optional adapters for Inscopix (`isx`) and CaImAn (`caiman`), utilities, plotters and example Jupyter notebooks.

This project was developed as a final project by students from Facultad de Ingeniería, Universidad de Buenos Aires, under the supervision of Dr. Fernando Chaure, in collaboration with the CGK Laboratory.

## Authors
- González Agustín
- Loyarte Iván
- Rueda Nazarena
- Singer Joaquín

## Installation

1. Install the library from PyPI

```bash
pip install cipipeline
```

2. Optional: Install Inscopix `isx` (required for the `isx` module)

   Software and installation instructions can be downloaded from the vendor site: https://www.inscopix.com
   
   Note: Do not confuse this with the public `isx` library available on PyPI or GitHub. This project requires the proprietary Inscopix software package.

3. Optional: Install CaImAn (required for the `caiman` module)

- Project: https://github.com/flatironinstitute/CaImAn
- Docs: https://caiman.readthedocs.io

CaImAn strongly recommends installing via conda for full functionality; follow the CaImAn docs.

4. Jupyter (recommended for opening example notebooks)

```bash
pip install jupyterlab
# or
pip install notebook
```

## Quick Start

Here's a simple example of creating and running a calcium imaging pipeline with ISX:

```python
import isx
from ci_pipe.pipeline import CIPipe

# Create a pipeline from videos in a directory
pipeline = CIPipe.with_videos_from_directory(
    'input_dir', 
    outputs_directory='output_dir', 
    isx=isx
)

# Run a complete processing pipeline
(
    pipeline
    .set_defaults(
        isx_bp_subtract_global_minimum=False, 
        isx_mc_max_translation=25, 
        isx_acr_filters=[('SNR', '>', 3), ('Event Rate', '>', 0), ('# Comps', '=', 1)]
    )
    .isx.preprocess_videos()
    .isx.bandpass_filter_videos()
    .isx.motion_correction_videos(isx_mc_series_name="series1")
    .isx.normalize_dff_videos()
    .isx.extract_neurons_pca_ica()
    .isx.detect_events_in_cells()
    .isx.auto_accept_reject_cells()
    .isx.longitudinal_registration(isx_lr_reference_selection_strategy='by_num_cells_desc')
)
```

For more examples, including CaImAn integration and advanced workflows, see the notebooks in `docs/examples`.

## Documentation and Resources
- PyPI package: https://pypi.org/project/cipipeline
- CGK Lab: https://cgk-laboratory.github.io
- Inscopix: https://www.inscopix.com
- CaImAn: https://github.com/flatironinstitute/CaImAn and https://caiman.readthedocs.io
- Jupyter starter guide: https://jupyter.org/install

## Examples
Example Jupyter notebooks are available in `docs/examples`. To run them locally:

```bash
git clone https://github.com/CGK-Laboratory/ci_pipe
cd ci_pipe
pip install -e .
# install optional dependencies if needed (isx, caiman)
jupyter lab
# open notebooks in docs/examples
```

---

Read the Spanish version in `README_es.md`
