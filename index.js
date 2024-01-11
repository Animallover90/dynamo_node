const {DynamoDBClient} = require("@aws-sdk/client-dynamodb");
const {
    DynamoDBDocumentClient,
    PutCommand,
    BatchWriteCommand,
    ScanCommand,
    GetCommand,
    BatchGetCommand,
    QueryCommand,
    UpdateCommand,
    DeleteCommand,
    ExecuteStatementCommand,
    BatchExecuteStatementCommand
} = require("@aws-sdk/lib-dynamodb");
const client = new DynamoDBClient({region: 'ap-northeast-2'});

const docClient = DynamoDBDocumentClient.from(client);

const put_item = async () => {
    const command = new PutCommand({
        TableName: "test",
        Item: {
            id: "abc",
            title: "aaa",
            create_date: "2023-05-02"
        },
    });

    const response = await docClient.send(command);
    console.log(response);
    return response;
};

const bach_put_item = async () => {

    const data_list = [
        [
            {
                id: "bbb",
                title: "bbb",
                create_date: "2023-05-02"
            },
            {
                id: "nnn",
                title: "nnn",
                create_date: "2023-05-02"
            }
        ]
    ]

    // For every chunk of 25 items, make one BatchWrite request.
    for (const chunk of data_list) {
        const putRequests = chunk.map((item) => ({
            PutRequest: {
                Item: item,
            },
        }));

        const command = new BatchWriteCommand({
            RequestItems: {
                ["test"]: putRequests,
            },
        });

        await docClient.send(command);
    }
};

const partiql_put_item = async () => {
    const command = new ExecuteStatementCommand({
        Statement: `INSERT INTO test value {'id':?, 'title':?, 'create_date':?}`,
        Parameters: ["def", "aaa", "2023-04-30"],
    });

    const response = await docClient.send(command);
    console.log(response);
    return response;
};

const partiql_batch_put_item = async () => {
    const params = ["ggg", "hhh", "jjj"];
    const command = new BatchExecuteStatementCommand({
        Statements: params.map((data) => ({
            Statement: `INSERT INTO test value {'id':?}`,
            Parameters: [data],
        })),
    });

    const response = await docClient.send(command);
    console.log(response);
    return response;
};

const scan_item = async () => {
    const command = new ScanCommand({
        ProjectionExpression: "#id, title, create_date",
        ExpressionAttributeNames: {"#id": "id"},
        TableName: "test",
    });

    const response = await docClient.send(command);
    for (const item of response.Items) {
        console.log(`${item.id} - (${item.title}, ${item.create_date})`);
    }
    return response;
};

const get_batch_item = async () => {
    const command = new BatchGetCommand({
        RequestItems: {
            test: {
                // Each entry in Keys is an object that specifies a primary key.
                Keys: [
                    {
                        id: "abc",
                    },
                    {
                        id: "def",
                    },
                ],
                // Only return the "id" and "title" attributes.
                ProjectionExpression: "id, title",
            },
        },
    });

    const response = await docClient.send(command);
    console.log(response.Responses["test"]);
    return response;
};

const get_item = async () => {
    const command = new GetCommand({
        TableName: "test",
        Key: {
            id: "abc",
        },
    });

    const response = await docClient.send(command);
    console.log(response);
    return response;
};

const query_item = async () => {
    const command = new QueryCommand({
        TableName: "test",
        IndexName: "title-index",
        KeyConditionExpression:
            "title = :title", //정렬키를 적용하면 title 외에 정렬 값을 통해 > < 를 AND조건으로 추가 가능 ex) "title = :title AND create_date > :create_date"
        ExpressionAttributeValues: {
            ":title": "aaa"
        },
    });

    const response = await docClient.send(command);
    console.log(response);
    return response;
};

const update_item = async () => {
    const command = new UpdateCommand({
        TableName: "test",
        Key: {
            id: "abc",
        },
        UpdateExpression: "set title = :title",
        ExpressionAttributeValues: {
            ":title": "qqq",
        },
        ReturnValues: "ALL_NEW",
    });

    const response = await docClient.send(command);
    console.log(response);
    return response;
};

const delete_item = async () => {
    const command = new DeleteCommand({
        TableName: "test",
        Key: {
            id: "abc",
        },
    });

    const response = await docClient.send(command);
    console.log(response);
    return response;
};



//////////////////////////////// ttl 테스트 함수 ////////////////////////////////////////////
// const TTL_DELTA = 60 * 60 * 24 * 7; // Keep records for 7 days
const TTL_DELTA = 60; // Keep records for 1 min
const put_ttl_item = async () => {
    const command = new PutCommand({
        TableName: "test",
        Item: {
            id: "abc",
            title: "aaa",
            create_date: "2023-05-02",
            ttl: (Math.floor(+new Date() / 1000) + TTL_DELTA)
        },
    });

    const response = await docClient.send(command);
    console.log(response);
    return response;
};