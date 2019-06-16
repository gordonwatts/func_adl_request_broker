# Build image that has all the software to convert AST's to C++ code

# Based on a python3 - latest.
FROM python:3

WORKDIR /usr/src/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY functional_adl-0.1.0a1-py3-none-any.whl /root/web/functional_adl-0.1.0a1-py3-none-any.whl
RUN pip3 install /root/web/functional_adl-0.1.0a1-py3-none-any.whl

# Get over the source code
COPY func_adl_request_broker/ ./func_adl_request_broker
COPY tools/ .

CMD ["/bin/bash"]
