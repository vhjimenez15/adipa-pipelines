FROM nginx:alpine

RUN apk add --no-cache apache2-utils

COPY nginx.conf /etc/nginx/nginx.conf

# El .htpasswd se genera en entrypoint con las vars de entorno
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
