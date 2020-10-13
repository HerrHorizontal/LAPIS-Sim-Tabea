# sed -i '1s/^/# DML \n# CONTEXT-DATABASE: lapis \n/' $1

ex -s -c '1i|# CONTEXT-DATABASE: lapis ' -c x $1
ex -s -c '1i|# DML ' -c x $1
influx -import -path=$1
