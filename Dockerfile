FROM jupyter/datascience-notebook

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      libgeos-dev \
      libproj-dev \
      llvm-7-dev \
      proj-bin

USER $NB_UID

RUN conda install --quiet --yes \
    'colorcet' \
    'datashader' \
    'mercantile' \
    'holoviews' \
    && \
    conda clean --all -f -y && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

RUN pip install 'geoviews' 'elasticsearch' 'elasticsearch_dsl'
#    'geoviews' \
#    mercantile \
