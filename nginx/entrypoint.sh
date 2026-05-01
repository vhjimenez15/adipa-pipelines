#!/bin/sh
set -e

# Genera el .htpasswd en runtime desde las variables de entorno
# Así la contraseña nunca vive en la imagen
htpasswd -bc /etc/nginx/.htpasswd "$NGINX_USER" "$NGINX_PASSWORD"

exec nginx -g "daemon off;"
