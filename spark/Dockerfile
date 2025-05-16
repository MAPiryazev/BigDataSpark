FROM bitnami/spark:3.5
USER root
RUN apt-get update && apt-get install -y curl
# Не скачиваем JARы — будем монтировать
CMD ["/opt/bitnami/spark/sbin/start-master.sh"]