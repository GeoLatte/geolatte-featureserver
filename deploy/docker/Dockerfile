FROM wegenenverkeer/java6

RUN mkdir -p /etc/service/geolate-nosqlfs
ADD startapplication.sh /etc/service/geolate-nosqlfs/run
RUN chmod a+x /etc/service/geolate-nosqlfs/run

EXPOSE 8080
