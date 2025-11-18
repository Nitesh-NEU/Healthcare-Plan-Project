const es_client = require("./../clients/elastic_search")

const INDEX_NAME="planindex";
let MapOfDocuments = {};
let listOfKeys = [];

const convertMapToDocumentIndex = async (jsonObject, parentId, objectName, parentObjId) => {
    const valueMap = {};
    const map = {};

    for (const [key, value] of Object.entries(jsonObject)) {
        const redisKey = `${jsonObject.objectType}:${parentId}`;
        if (Array.isArray(value)) {
            await convertToList(value, jsonObject.objectId, key, parentObjId);
        } else if (typeof value === 'object') {
            await convertMapToDocumentIndex(value, jsonObject.objectId, key, parentObjId);
        } else {
            valueMap[key] = value;
            map[redisKey] = valueMap
        }
    }

    if (objectName === "plan") {
        valueMap["plan_join"] = {
            "parent": "",
            "name": objectName
        }
    } else if (objectName.match(/^-?\d+$/)) {
        parentId = parentObjId;
        valueMap["plan_join"] = {
            "parent": parentObjId,
            "name": "linkedPlanServices"
        };
    } else {
        valueMap["plan_join"] = {
            "name": objectName,
            "parent": parentId
        }
    }

    const id = `${parentId}:${jsonObject.objectId}`;
    if (!!jsonObject?.objectId) MapOfDocuments[id] = valueMap;
    return map;
}

const convertToList = async (jsonArray, parentId, objectName, parentObjId) => {
    const list = [];
    for (let i = 0; i < jsonArray.length; i++) {
        let value = jsonArray[i];
        if (Array.isArray(value)) {
            value = await convertToList(value, parentId, objectName, parentObjId);
        } else if (typeof value === 'object') {
            value = await convertMapToDocumentIndex(value, parentId, objectName);
        }
        list.push(value);
    }
    return list;
}

const convertToKeysList = async (jsonArray) => {
    let list = [];
    for (let value of jsonArray) {
        if (Array.isArray(value)) {
            value = await convertToKeysList(value);
        } else if (typeof value === 'object') {
            value = await convertToKeys(value);
        }
        list.push(value);
    }
    return list;
}

const convertToKeys = async (jsonObject) => {
    const map = {};
    const valueMap = {};

    for (const [key, value] of Object.entries(jsonObject)) {
        const redisKey = jsonObject["objectId"];
        if (Array.isArray(value)) {
            await convertToKeysList(value);
        } else if (typeof value === 'object') {
            await convertToKeys(value);
        } else {
            valueMap[key] = value;
            map[redisKey] = valueMap;
        }
    }

    listOfKeys.push(jsonObject["objectId"]);
    return map;

}

const postDocument = async (plan) => {

    const indexExists = await doesIndexExist(INDEX_NAME);
    if(!indexExists){
        await createIndex(INDEX_NAME);
    }
    try {
        MapOfDocuments = {};
        await convertMapToDocumentIndex(plan, "", "plan", plan.objectId);
        console.log(MapOfDocuments);
        for (const [key, value] of Object.entries(MapOfDocuments)) {
            const [parentId, objectId] = key.split(":");
            await es_client.index({
                index: INDEX_NAME,
                id: objectId,
                routing: parentId,
                body: value,
            });
        }
    } catch (e) {
        console.log("Error", e);
    }
}

const deleteDocument = async (jsonObject) => {
    listOfKeys = [];
    await convertToKeys(jsonObject);
    for (const key of listOfKeys) {
        es_client.delete({
            index: INDEX_NAME,
            id: key,
        }, (err, res) => {
            if (err) {
                console.error(err.message);
            } else {
                console.log('Indexes have been deleted!', res);
            }
        });
    }
}

const doesIndexExist = async (indexName) =>{
    const res = await es_client.indices.exists({
        index: indexName
    });
    console.log(`Index ${indexName} exists: ${res}`);
    return res;
}

const createIndex = async (indexName) => {

    const mapping = {
        properties: {
          plan: {
            properties: {
              _org: { type: 'text' },
              objectId: { type: 'keyword' },
              objectType: { type: 'text' },
              planType: { type: 'text' },
              creationDate: { type: 'date', format: 'MM-dd-yyyy' },
            },
          },
          planCostShares: {
            properties: {
              copay: { type: 'long' },
              deductible: { type: 'long' },
              _org: { type: 'text' },
              objectId: { type: 'keyword' },
              objectType: { type: 'text' },
            },
          },
          linkedPlanServices: {
            properties: {
              _org: { type: 'text' },
              objectId: { type: 'keyword' },
              objectType: { type: 'text' },
            },
          },
          linkedService: {
            properties: {
              _org: { type: 'text' },
              name: { type: 'text' },
              objectId: { type: 'keyword' },
              objectType: { type: 'text' },
            },
          },
          planserviceCostShares: {
            properties: {
              copay: { type: 'long' },
              deductible: { type: 'long' },
              _org: { type: 'text' },
              objectId: { type: 'keyword' },
              objectType: { type: 'text' },
            },
          },
          plan_join: {
            type: 'join',
            eager_global_ordinals: true,
            relations: {
              plan: ['planCostShares', 'linkedPlanServices'],
              linkedPlanServices: ['planserviceCostShares', 'linkedService']
            },
          },
        },
      };

    const res = await es_client.indices.create({
        index: indexName,
        mappings: mapping
    });
    console.log(res);
    if(res)
        console.log(`Index ${indexName} is created`);
    else 
        console.log("Could not create index");
}

module.exports = {
    postDocument, deleteDocument
}