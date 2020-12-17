FROM haizizh/compbio:v0.1.2
LABEL maintainer="Haizi Zheng <haizi.zh@gmail.com>"
ARG DEBIAN_FRONTEND=noninteractiv

ADD . /tmp/repo
WORKDIR /tmp/repo
ENV PATH /opt/conda/bin:${PATH}
ENV LANG C.UTF-8
ENV SHELL /bin/bash
RUN /bin/bash -c "conda install -y -c conda-forge mamba && \
    mamba create -q -y -c conda-forge -c bioconda -n snakemake snakemake snakemake-minimal --only-deps && \
    conda clean --all -y && \
    source activate snakemake && \
    which python && \
    pip install .[reports,messaging,google-cloud]"
RUN echo "source activate snakemake" > ~/.bashrc
ENV PATH /opt/conda/envs/snakemake/bin:${PATH}