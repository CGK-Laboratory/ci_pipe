# CIPipeline / CI_Pipe

### Descripción corta
CIPipeline (referido en imports como `ci_pipe`) es una biblioteca Python para construir y ejecutar pipelines de procesamiento de imágenes de calcio usada por el Laboratorio CGK. Provee primitivas de pipeline, adaptadores opcionales para Inscopix (`isx`) y CaImAn (`caiman`), utilidades y notebooks de ejemplo.

### Equipo de desarrollo

El equipo está formado por estudiantes de la Facultad de Ingeniería de la UBA en contexto del trabajo final de la carrera de Ingeniería en Informática en conjunto con su tutor. 

- Gonzalez Agustín
- Loyarte Iván
- Rueda Nazarena
- Singer Joaquín
- Fernando Chaure - Tutor

El trabajo es un trabajo en conjunto con y para el Laboratorio CGK — equipo de neurociencia computacional y microscopía

### Tutorial de instalación

1) Instalar la librería desde PyPI

```bash
pip install cipipeline
```

2) Opcional: Instalar Inscopix `isx` (requerido para el modulo de `isx`)

- Repositorio: https://github.com/Inscopix/isx
- Inscopix: https://www.inscopix.com

Sigue la documentación del repositorio `isx` para detalles de instalación (algunos paquetes pueden requerir credenciales o instaladores específicos del proveedor).

3) Opcional: Instalar CaImAn (requerido para el modulo de `caiman`)

- Proyecto: https://github.com/flatironinstitute/CaImAn
- Docs: https://caiman.readthedocs.io

CaImAn recomienda usar conda para una instalación completa.

4) Jupyter (recomendado para abrir los notebooks de ejemplo)

```bash
pip install jupyterlab
# o
pip install notebook
```

### Enlaces útiles
- Paquete PyPI: https://pypi.org/project/cipipeline
- Inscopix / isx: https://github.com/Inscopix/isx y https://www.inscopix.com
- CaImAn: https://github.com/flatironinstitute/CaImAn y https://caiman.readthedocs.io
- Guía de inicio de Jupyter: https://jupyter.org/install

### Página del laboratorio CGK
- CGK Lab: https://cgk-lab.example  # reemplazar por la URL real del laboratorio

### Guía de ejemplos
Los notebooks de ejemplo están en `docs/examples`. Para ejecutarlos localmente:

```bash
git clone <repo>
cd <repo>
pip install -e .
# instalar dependencias opcionales si es necesario (isx, caiman)
jupyter lab
# abrir los notebooks en docs/examples
```
