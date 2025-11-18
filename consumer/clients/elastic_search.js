const { Client } = require('@elastic/elasticsearch')

const es_client = new Client({
  node: 'http://localhost:9200'
})


module.exports = es_client
