FROM gcr.io/dataflow-templates-base/python3-template-launcher-base


ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}

COPY . /dataflow/template/

WORKDIR ${WORKDIR}

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipeline_preprocess_preprocess_table.py"

RUN pip install --no-cache-dir -U apache-beam==2.32.0

RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y libffi-dev git \
    && rm -rf /var/lib/apt/lists/* \
    # Upgrade pip and install requirements
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    # Download the requirements to speed up launching Dataflow Job
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r requirements.txt

ENV PIP_NO_DEPS=True