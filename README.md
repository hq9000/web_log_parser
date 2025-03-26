assumes the following log format:

```
log_format custom '$remote_addr - $host [$time_local] "$request" $status $body_bytes_sent $request_time "$http_referer" "$http_user_agent"';
access_log /var/log/nginx/access.log custom;
```