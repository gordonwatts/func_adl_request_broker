# BBUild an image that will run the results xrootd server

FROM cloudpg/cachingondemand

WORKDIR /usr/src/app
COPY xrootd/xrootd-results.cfg /etc/xrootd/xrootd-results.cfg
RUN chown -R xrootd:xrootd /etc/xrootd

ENTRYPOINT [ "/bin/bash", "-c", "sudo -u xrootd xrootd -c /etc/xrootd/xrootd-results.cfg" ]
