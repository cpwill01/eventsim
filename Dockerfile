FROM sbtscala/scala-sbt:eclipse-temurin-11.0.17_8_1.8.2_2.13.10 AS buildjar
WORKDIR /eventsim_build

COPY . .
RUN sbt plugins
RUN sbt assembly

FROM eclipse-temurin:11.0.17_8-jre AS output
WORKDIR /eventsim_app

COPY ./eventsim.sh ./eventsim.sh
COPY --from=buildjar /eventsim_build/target/scala-2.12/eventsim-assembly-2.0.jar ./eventsim-assembly-2.0.jar
ENTRYPOINT ["./eventsim.sh"]
