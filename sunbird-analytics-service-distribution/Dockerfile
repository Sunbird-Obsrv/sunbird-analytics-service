FROM alpine:3.11
RUN apk --no-cache add openjdk11 --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community
RUN apk update \
    && apk add  unzip \
    && apk add curl \
    && adduser -u 1001 -h /home/analytics/ -D analytics \
    && mkdir -p /home/analytics
RUN chown -R analytics:analytics /home/analytics
USER analytics
COPY analytics-api-2.0-dist.zip /home/analytics/
RUN unzip /home/analytics/analytics-api-2.0-dist.zip -d /home/analytics/
RUN rm /home/analytics/analytics-api-2.0/lib/org.slf4j.slf4j-log4j12-1.7.16.jar
RUN rm /home/analytics/analytics-api-2.0-dist.zip
WORKDIR /home/analytics/
CMD java -XX:+PrintFlagsFinal $JAVA_OPTIONS $MIN_HEAP $MAX_HEAP -XX:+UseG1GC -XX:+UseStringDeduplication -cp '/home/analytics/analytics-api-2.0/lib/*' -Dconfig.file=/home/analytics/application.conf play.core.server.ProdServerStart /home/analytics/analytics-api-2.0