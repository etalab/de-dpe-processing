# mysql -u root -p dpe < ~/NOM-TABLE.sql
# mysql -u root -p dpe -e "select * from NOM-TABLE" -B | sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" > NOM-TABLE.csv