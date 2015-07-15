export passwd="insert the password here"
curl -X PUT "http://Administrator:${passwd}@130.56.252.54:8092/canberra/_design/dev_tweercruncher" \
  --data @./src/main/couchdb/tweetcruncher.json \
  --header "Content-Type:application/json"
