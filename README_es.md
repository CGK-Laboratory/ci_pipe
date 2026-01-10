# CIPipeline

## Descripción
CIPipeline (referido en imports como `ci_pipe`) es una biblioteca Python para construir y ejecutar pipelines de procesamiento de imágenes de calcio. Provee primitivas de pipeline, adaptadores opcionales para Inscopix (`isx`) y CaImAn (`caiman`), utilidades y notebooks de ejemplo.

Este proyecto fue desarrollado como trabajo final por estudiantes de la Facultad de Ingeniería, Universidad de Buenos Aires, bajo la supervisión del Dr. Fernando Chaure, en colaboración con el Laboratorio CGK.

## Autores
- González Agustín
- Loyarte Iván
- Rueda Nazarena
- Singer Joaquín

## Instalación

1. Instalar la librería desde PyPI

```bash
pip install cipipeline
```

2. Instalar bibliotecas/paquetes requeridos para módulos específicos

   Actualmente, CIPipeline soporta los siguientes módulos opcionales:
   
   - **Inscopix `isx`** (requerido para el módulo de `isx`): El software y las instrucciones de instalación pueden descargarse del sitio del fabricante: https://www.inscopix.com
     
     Nota: No confundir con la biblioteca pública `isx` disponible en PyPI o GitHub. Este proyecto requiere el paquete de software propietario de Inscopix.
   
   - **CaImAn** (requerido para el módulo de `caiman`): 
     - Proyecto: https://github.com/flatironinstitute/CaImAn
     - Docs: https://caiman.readthedocs.io
     
     CaImAn recomienda usar conda para una instalación completa; sigue la documentación de CaImAn.

3. Jupyter (recomendado para abrir los notebooks de ejemplo)

```bash
pip install jupyterlab
# o
pip install notebook
```

## Inicio Rápido

Aquí hay un ejemplo simple de cómo crear y ejecutar un pipeline de imágenes de calcio con ISX:

```python
import isx
from ci_pipe.pipeline import CIPipe

# Crear un pipeline desde videos en un directorio
pipeline = CIPipe.with_videos_from_directory(
    'input_dir', 
    outputs_directory='output_dir', 
    isx=isx
)

# Ejecutar un pipeline completo de procesamiento
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

Para más ejemplos, incluyendo integración con CaImAn y flujos de trabajo avanzados, consulta los notebooks en `docs/examples`.

## Documentación y Recursos
- Paquete PyPI: https://pypi.org/project/cipipeline
- CGK Lab: https://cgk-laboratory.github.io
- Inscopix: https://www.inscopix.com
- CaImAn: https://github.com/flatironinstitute/CaImAn y https://caiman.readthedocs.io
- Guía de inicio de Jupyter: https://jupyter.org/install

## Ejemplos
Los notebooks de ejemplo están disponibles en `docs/examples`. Para ejecutarlos localmente:

```bash
git clone https://github.com/CGK-Laboratory/ci_pipe
cd ci_pipe
pip install -e .
# instalar dependencias opcionales si es necesario (isx, caiman)
jupyter lab
# abrir los notebooks en docs/examples
```

