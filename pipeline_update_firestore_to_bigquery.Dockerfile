FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY requirements.txt .
COPY settings.ini .
COPY config.py .
COPY pipeline_update_firestore_to_bigquery.py .

COPY utils/* utils/
COPY gcp/* gcp/

COPY update/* update/


ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/pipeline_update_firestore_to_bigquery.py"

RUN pip install -U -r ./requirements.txt