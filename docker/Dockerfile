FROM adtiming/omjava
MAINTAINER openmediation
ENV SYSTEMNAME "omsystem"
ENV SERVERNAME "om-adc"
RUN mkdir ${SERVERNAME}
ADD docker-entrypoint.sh /
ADD application-loc.yml /${SERVERNAME}
ADD ../target/om-adc.jar /${SERVERNAME}
WORKDIR /
HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl -f http://localhost:19014/ || exit 1
EXPOSE 19014
ENTRYPOINT ["/docker-entrypoint.sh"] 
CMD ["om-adc"]
