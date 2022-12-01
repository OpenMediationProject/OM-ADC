FROM adtiming/omjava
MAINTAINER openmediation
ENV SYSTEMNAME "omsystem"
ENV SERVERNAME "om-adc"
ENV OMYML_DBADDRESS ""
ENV OMYML_DBNAME ""
ENV OMYML_USERNAME ""
ENV OMYML_PASSWORD ""
ENV OMYML_DSSERVERDOMAIN ""
RUN mkdir ${SERVERNAME}
ARG JAR_FILE
ADD docker/docker-entrypoint.sh /
ADD docker/application-loc.yml /${SERVERNAME}
ADD ${JAR_FILE} /${SERVERNAME}
WORKDIR /
HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl -f http://localhost:19014/ || exit 1
RUN chmod +x /docker-entrypoint.sh
EXPOSE 19014
ENTRYPOINT ["/docker-entrypoint.sh"] 
CMD ["om-adc"]
