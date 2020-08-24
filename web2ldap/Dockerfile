FROM python:3.7

RUN \
  apt update && apt install -y ldap-utils libldap2-dev libsasl2-dev sed  && \
  python -m venv /opt/web2ldap && \
  /opt/web2ldap/bin/pip3 install --find-links https://www.web2ldap.de/download.html web2ldap

ADD ./conf/__init__.py /opt/web2ldap/etc/web2ldap/web2ldapcnf/__init__.py

EXPOSE 1760

CMD ["/opt/web2ldap/bin/web2ldap", "0.0.0.0","1760"]
