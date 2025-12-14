# CIPipeline / CI_Pipe

## English

### Short description
CIPipeline (imported as `ci_pipe`) is a Python library for building and running calcium-imaging processing pipelines used by the CGK Laboratory. It provides core pipeline primitives, optional adapters for Inscopix (`isx`) and CaImAn (`caiman`), utilities, plotters and example Jupyter notebooks.

### Development team
The project was developed as a final project by students from Facultad de Ingeniería, Universidad de Buenos Aires, under the supervision of a tutor, in collaboration with the CGK Laboratory.

Contributors:
- Gonzalez Agustín
- Loyarte Iván
- Rueda Nazarena
- Singer Joaquín
- Fernando Chaure (Tutor)

### Installation tutorial

1) Install the library from PyPI

```bash
pip install cipipeline
```

2) Optional: Install Inscopix `isx` (required for the `isx` module)

- Repository: https://github.com/Inscopix/isx
- Inscopix: https://www.inscopix.com

Follow the `isx` repository documentation for installation details (some Inscopix packages may require credentials or vendor-specific installers).

3) Optional: Install CaImAn (required for the `caiman` module)

- Project: https://github.com/flatironinstitute/CaImAn
- Docs: https://caiman.readthedocs.io

CaImAn strongly recommends installing via conda for full functionality; follow the CaImAn docs.

4) Jupyter (recommended for opening example notebooks)

```bash
pip install jupyterlab
# or
pip install notebook
```

### Useful links
- PyPI package: https://pypi.org/project/cipipeline
- Inscopix / isx: https://github.com/Inscopix/isx and https://www.inscopix.com
- CaImAn: https://github.com/flatironinstitute/CaImAn and https://caiman.readthedocs.io
- Jupyter starter guide: https://jupyter.org/install

### CGK Laboratory page
- CGK Lab: https://cgk-lab.example  # replace with the real lab page URL

### Example guide
Example Jupyter notebooks live in `docs/examples`. To run them locally:

```bash
git clone <repo>
cd <repo>
pip install -e .
# install optional dependencies if needed (isx, caiman)
jupyter lab
# open notebooks in docs/examples
```

---

Read the Spanish translation in `README.es.md` (or view both sections in the repository).
