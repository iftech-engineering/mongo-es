# Mongo-ES
A MongoDB to Elasticsearch connector

## Installation
```bash
npm i -g mongo-es
```

## Usage
```bash
# normal mode
mongo-es ./config.json

# debug mode, with debug info printed
NODE_ENV=dev mongo-es ./config.json
```

## Concepts
### Scan phase
scan entire database (or select with query) for existed documents

### Tail phase
tail the oplog for documents' create, update or delete

## Configuration
Structure:
```json
{
  "controls": {},
  "mongodb": {},  
  "elasticsearch": {},
  "tasks": [
    {
      "extract": {},
      "transform": {},
      "load": {}
    }
  ]
}
```
[Detail example](https://github.com/jike-engineering/mongo-es/blob/master/examples/config.json)

### controls
- `mongodbReadCapacity` - Max docs read per second (default: `10000`).
- `elasticsearchBulkSize` - Max bluk size per request (default: `5000`).
- `tailFromTime` - If set, program start to tail oplog with query: `{ ts: { $gte: new Timestamp(new Date(tailFromTime).getTime(), 0) } }`, without scan entire database. (optional)

### mongodb
- `url` - The connection URI string, eg: `mongodb://user:password@localhost:27017/db?replicaSet=rs0`.
**notice**: `'db'` in url will be ignored.
**notice**: must use a `admin` user to access oplog.
- `options` - Connection settings, see: [MongoClient](http://mongodb.github.io/node-mongodb-native/2.1/api/MongoClient.html#.connect). (optional)

### elasticsearch
- `options` - Elasticsearch Config Options, see: [Configuration](https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/configuration.html).
- `index` - If set, auto create index when program start, see: [Indeces Create](https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/api-reference-5-0.html#api-indices-create-5-0). (optional)

### task.extract
- `db` - Database name.
- `collection` - Collection name in database.
- `query` - Query selector, see [Query](https://docs.mongodb.com/manual/reference/operator/query/#query-selectors).
**notice**: not work in [Tail phase](https://github.com/jike-engineering/mongo-es#tail-phase).
- `projection` - Projection selector, see [Projection](https://docs.mongodb.com/manual/reference/operator/projection/).
**notice**: works in [Tail phase](https://github.com/jike-engineering/mongo-es#tail-phase).
- `sort` Sort order of the result set. **recommend**: `{ "$natural": -1 }`, new documents first, see [Sort](https://docs.mongodb.com/manual/reference/method/cursor.sort/).

### task.transform
- `mapping` - The field mapping from mongodb's collection to elasticsearch's index.
- `parent` - The field in mongodb's collection to use as the `_parent` in elasticsearch's index. (optional)

### task.load
- `index` - The name of the index.
- `type` - The name of the document type.
- `body` - The request body, see [Put Mapping](https://www.elastic.co/guide/en/elasticsearch/reference/5.x/indices-put-mapping.html).

## License
[Mozilla Public License Version 2.0](https://www.mozilla.org/en-US/MPL/2.0/)
