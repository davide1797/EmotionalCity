cd C:\MongoDB\Server\3.2\bin
mongodump -d emotionalcity -c tweets -q "{ createdDate: {$lte:1707475302} }" -o C:\Users\ddipi\Desktop\backups\2024\02\09
