const AWS = require("aws-sdk");
const https = require("https");
const ACCESSKEY = process.env.AWS_ACCESSKEY;
const SECRETKEY = process.env.AWS_SECRETKEY;
const REGION = "us-east-2";
const DATABASE = "POCSampleDatabase";
const UPDATEDKMSKEYID = "d9cdb873-699f-4375-9e82-4d9393877d4a";

//Tipos de dado para medidadas
const types = {
  bg: "BIGINT",
  vc: "VARCHAR",
  db: "DOUBLE",
  bl: "BOOLEAN"
}

const config = {
  region: REGION,
  accessKeyId: ACCESSKEY,
  secretAccessKey: SECRETKEY,
};

const globalParams = {
  DatabaseName: DATABASE,
  MaxResults: 15,
};

AWS.config.update(config);

const agent = new https.Agent({
  maxSockets: 5000,
});

// Criando instância do TimestreamWrite
writeClient = new AWS.TimestreamWrite({
  maxRetries: 10,
  httpOptions: {
    timeout: 20000,
    agent: agent,
  },
});

// Criando instância do TimestreamQuery
queryClient = new AWS.TimestreamQuery();

const listTables = async () => {
  console.log("Listando tablelas...");
  writeClient
    .listTables(globalParams)
    .promise()
    .then(
      (data) => {
        console.log(`data ${JSON.stringify(data.Tables)}`);
        return data.Tables;
      },
      (err) => {
        console.log("Error while listing databases", err);
      }
    );
};

const select = async (table) => {
  console.log(
    `Fazendo select all no banco: ${DATABASE}, tabela: ${table} com limit 5`
  );
  const params = {
    QueryString: `SELECT * FROM ${DATABASE}.${table} LIMIT 5`,
  };

  queryClient
    .query(params)
    .promise()
    .then(
      (data) => {
        console.log(`data: ${JSON.stringify(data)}`);
        return data.Tables;
      },
      (err) => {
        console.log(`Error query ${params.QueryString}:`, err);
      }
    );
};

const insert = async (data) => {
  console.log(
    `Iniciando a gravação de dados no banco: ${DATABASE}, tabela: ${data.table} com limit 5`
  );
  const currentTime = Date.now().toString();

  const dimensions = data.dimensions;
  const measure = data.measure;

  const checkDimensions = await checkDimensionsStructure(dimensions);

  if (!checkDimensions.correct) {
    console.log(checkDimensions.message);
    return;
  }

  const value1 = {
    Dimensions: dimensions,
    MeasureName: measure.name,
    MeasureValue: measure.value,
    MeasureValueType: measure.type,
    Time: currentTime.toString(),
  };

  const records = [value1];

  const params = {
    DatabaseName: DATABASE,
    TableName: data.table,
    Records: records,
  };

  const request = writeClient.writeRecords(params);

  request.promise().then(
    (data) => {
      console.log(`data: ${JSON.stringify(data)}`);
      return data;
    },
    (err) => {
      console.log(`Error query ${params.QueryString}:`, err);
      printRejectedRecordsException(request);
    }
  );
};

// Cancela uma query, apenas quando ela ainda não foi concluída
const cancelQuery = async (queryID) => {
  const params = {
    QueryId: queryID,
  };

  queryClient
    .cancelQuery(params)
    .promise()
    .then(
      (data) => {
        console.log(`data: ${JSON.stringify(data)}`);
      },
      (err) => {
        console.log(`Error query ${params.QueryString}: ${err}`);
      }
    );
};

// Printa os logs de rejeição gravação de dados
const printRejectedRecordsException = async (request) => {
  const responsePayload = JSON.parse(
    request.response.httpResponse.body.toString()
  );
  console.log(`RejectedRecords: , ${responsePayload.RejectedRecords}`);
};

// Checa a estrutura do atributo Dimensions
const checkDimensionsStructure = async (data) => {
  if (!Array.isArray(data) || data.length === 0) {
    return { correct: false, message: "Is not an array" };
  }
  for (let i = 0; i < data.length; i++) {
    const item = data[i];
    if (!item.Name || item.Value) {
      return { correct: false, message: "Item struct is incorrect" };
    }
    if (item.Name.length === 0 || item.Value.length === 0) {
      return { correct: false, message: "Name or Value of item is empty" };
    }
    continue;
  }
  return { correct: true, message: "" };
};

const data = {
  table: "Test",
  measure: {
    name: "test_delete",
    value: "001",
    type: types.vc
  },
  dimensions: [
    { Name: "name", Value: "tester" },
    { Name: "email", Value: "tester@test.com" },
  ],
};

// listTables();
// select('Test');
insert(data);
